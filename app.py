from fastapi import FastAPI, Request, Form, Cookie, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from passlib.context import CryptContext
import jwt  # PyJWT
from datetime import datetime, timedelta
from typing import Optional

import pymysql
import os
import json

# ================== 基础配置 ==================

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
SECRET_KEY = "this_is_a_super_secret_key_change_me"
ALGORITHM = "HS256"

# ================== WebSocket 管理（按 post_id 分组） ==================

class CommentManager:
    def __init__(self):
        # key: post_id (int) -> List[WebSocket]
        self.connections: dict[int, list[WebSocket]] = {}
        # 正在输入的用户，key: post_id -> set(username)
        self.typing_users: dict[int, set[str]] = {}

    async def connect(self, websocket: WebSocket, post_id: int, username: str | None):
        await websocket.accept()
        self.connections.setdefault(post_id, []).append(websocket)
        await self.broadcast_online_count(post_id)

    def disconnect(self, websocket: WebSocket, post_id: int, username: str | None):
        if post_id in self.connections and websocket in self.connections[post_id]:
            self.connections[post_id].remove(websocket)
        if username and post_id in self.typing_users:
            self.typing_users[post_id].discard(username)

    async def broadcast(self, post_id: int, message: dict):
        conns = self.connections.get(post_id, [])
        for ws in list(conns):
            try:
                await ws.send_text(json.dumps(message, ensure_ascii=False))
            except Exception:
                pass

    async def broadcast_online_count(self, post_id: int):
        count = len(self.connections.get(post_id, []))
        msg = {"type": "online_count", "count": count}
        await self.broadcast(post_id, msg)

    async def set_typing(self, post_id: int, username: str, is_typing: bool):
        if post_id not in self.typing_users:
            self.typing_users[post_id] = set()
        if is_typing:
            self.typing_users[post_id].add(username)
        else:
            self.typing_users[post_id].discard(username)
        msg = {"type": "typing", "users": list(self.typing_users[post_id])}
        await self.broadcast(post_id, msg)

comment_manager = CommentManager()

# ================== MySQL 直连配置（pymysql） ==================

DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "Hhaazzeell602")  # 本地默认密码
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "myminireddit")

conn = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    charset="utf8mb4",
    autocommit=True,
)
cur = conn.cursor()

# 建表：用户、帖子、评论
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE,
    password VARCHAR(255),
    is_admin TINYINT(1) NOT NULL DEFAULT 0,
    is_banned TINYINT(1) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    title VARCHAR(200),
    content TEXT,
    created_at DATETIME,
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS comments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    post_id INT,
    user_id INT,
    content TEXT,
    created_at DATETIME,
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# ================== 工具函数：JWT & 用户 ==================

def create_token(username: str) -> str:
    expire = datetime.utcnow() + timedelta(days=7)
    data = {"sub": username, "exp": expire}
    token = jwt.encode(data, SECRET_KEY, algorithm=ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token

def get_username_from_token(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return data.get("sub")
    except Exception:
        return None

def get_user_id(username: str) -> Optional[int]:
    cur.execute("SELECT id FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    return row[0] if row else None

def is_admin(username: str) -> bool:
    cur.execute("SELECT is_admin FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    return bool(row and row[0] == 1)

def is_banned(username: str) -> bool:
    cur.execute("SELECT is_banned FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    return bool(row and row[0] == 1)

# ================== 页面路由：首页 / 登录 / 注册 / 退出 ==================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
        FROM posts p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN topics t ON p.topic_id = t.id
        WHERE p.is_deleted = 0
        ORDER BY p.id DESC
    """)
    posts = cur.fetchall()

    admin_flag = is_admin(username) if username else False

    return templates.TemplateResponse(
        "index.html",
        {"request": request, "username": username, "posts": posts, "is_admin": admin_flag},
    )

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.get("/logout")
async def logout():
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("token")
    return resp

# ================== WebSocket：评论实时 + 在线人数 + 正在输入 ==================

@app.websocket("/ws/comments/{post_id}")
async def comments_websocket(websocket: WebSocket, post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        await websocket.close()
        return

    await comment_manager.connect(websocket, post_id, username)
    await comment_manager.broadcast_online_count(post_id)

    try:
        while True:
            data_text = await websocket.receive_text()
            try:
                data = json.loads(data_text)
            except Exception:
                continue

            if data.get("type") == "typing":
                await comment_manager.set_typing(post_id, username, bool(data.get("is_typing")))
    except WebSocketDisconnect:
        comment_manager.disconnect(websocket, post_id, username)
        await comment_manager.broadcast_online_count(post_id)
        await comment_manager.set_typing(post_id, username, False)

# ================== 管理员路由：删帖 / 删评 / 用户管理 ==================

@app.post("/admin/post/{post_id}/delete")
async def admin_delete_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("UPDATE posts SET is_deleted = 1 WHERE id = %s", (post_id,))
    conn.commit()
    return RedirectResponse("/", status_code=302)

@app.post("/admin/comment/{comment_id}/delete")
async def admin_delete_comment(comment_id: int, post_id: int = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("UPDATE comments SET is_deleted = 1 WHERE id = %s", (comment_id,))
    conn.commit()
    return RedirectResponse(f"/post/{post_id}", status_code=302)

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("SELECT id, username, is_admin, is_banned FROM users ORDER BY id ASC")
    users = cur.fetchall()

    return templates.TemplateResponse(
        "admin_users.html",
        {"request": request, "username": username, "users": users},
    )

@app.post("/admin/user/{user_id}/ban")
async def admin_ban_user(user_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("SELECT username, is_admin, is_banned FROM users WHERE id = %s", (user_id,))
    row = cur.fetchone()
    if not row:
        return RedirectResponse("/admin/users", status_code=302)

    target_username, target_is_admin, target_is_banned = row
    if target_username == username or target_is_admin == 1:
        return RedirectResponse("/admin/users", status_code=302)

    new_flag = 0 if target_is_banned == 1 else 1
    cur.execute("UPDATE users SET is_banned = %s WHERE id = %s", (new_flag, user_id))
    conn.commit()
    return RedirectResponse("/admin/users", status_code=302)

@app.get("/admin/topics", response_class=HTMLResponse)
async def admin_topics(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    # 查出所有 topic，包括未审核的
    cur.execute("""
        SELECT t.id, t.name, t.description, t.is_approved, t.created_at, u.username
        FROM topics t
        LEFT JOIN users u ON t.created_by = u.id
        ORDER BY t.created_at DESC
    """)
    topics = cur.fetchall()

    return templates.TemplateResponse(
        "admin_topics.html",
        {
            "request": request,
            "username": username,
            "topics": topics,
        },
    )

@app.post("/admin/topic/{topic_id}/approve")
async def admin_approve_topic(topic_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("UPDATE topics SET is_approved = 1 WHERE id = %s", (topic_id,))
    conn.commit()
    return RedirectResponse("/admin/topics", status_code=302)

@app.post("/admin/topic/{topic_id}/delete")
async def admin_delete_topic(topic_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    # 简单处理：直接删除这个 topic（如果以后想更安全，可以只标记 is_deleted 字段）
    cur.execute("DELETE FROM topics WHERE id = %s", (topic_id,))
    conn.commit()
    return RedirectResponse("/admin/topics", status_code=302)

@app.get("/admin/inbox", response_class=HTMLResponse)
async def admin_inbox(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    # 查未审核 topic 数量
    cur.execute("SELECT COUNT(*) FROM topics WHERE is_approved = 0")
    pending_topics = cur.fetchone()[0] or 0

    # 预留：举报数量（等我们设计 reports 表再加）
    pending_reports = 0

    return templates.TemplateResponse(
        "admin_inbox.html",
        {
            "request": request,
            "username": username,
            "pending_topics": pending_topics,
            "pending_reports": pending_reports,
        },
    )

# ================== 登录 / 注册 ==================

@app.post("/register")
async def register(username: str = Form(), password: str = Form()):
    if len(password) > 50:
        return HTMLResponse("密码太长啦，请不要超过 50 个字符。<a href='/register'>返回</a>", status_code=400)

    hashed = pwd_context.hash(password)
    try:
        cur.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hashed),
        )
        return RedirectResponse("/login", status_code=302)
    except pymysql.err.IntegrityError:
        return HTMLResponse("用户名已存在，请换一个。<a href='/register'>返回</a>", status_code=400)

@app.post("/login")
async def login(username: str = Form(), password: str = Form()):
    cur.execute("SELECT password, is_banned FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if not row or not pwd_context.verify(password, row[0]):
        return HTMLResponse("用户不存在或密码错误。<a href='/login'>返回</a>", status_code=400)

    if row[1] == 1:
        return HTMLResponse("此账号已被封禁，请联系管理员。", status_code=403)

    token = create_token(username)
    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie("token", token, httponly=True, max_age=7*24*60*60)
    return resp

# ================== 发帖 ==================

@app.get("/post/new", response_class=HTMLResponse)
async def new_post_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    # 加载已经批准的 topics（is_approved=1）
    cur.execute("""
        SELECT id, name
        FROM topics
        WHERE is_approved = 1
        ORDER BY name ASC
    """)
    topics = cur.fetchall()

    return templates.TemplateResponse(
        "new_post.html",
        {"request": request, "username": username, "topics": topics},
    )

@app.post("/post/new")
async def new_post(
    title: str = Form(),
    content: str = Form(),
    topic_id: Optional[int] = Form(None),
    new_topic_name: str = Form(""),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()

    # 如果用户填写了新 topic 名称，插入到 topics 作为待审核的申请
    new_topic_name = new_topic_name.strip()
    if new_topic_name:
        cur.execute(
            "INSERT INTO topics (name, description, created_by, is_approved, created_at) VALUES (%s, %s, %s, 0, %s)",
            (new_topic_name, "", user_id, now),
        )
        conn.commit()
        # 暂时不把这个新 topic_id 用在当前帖子，因为还没审核通过

    # 插入帖子（topic_id 可能为 None）
    if topic_id:
        cur.execute(
            "INSERT INTO posts (user_id, title, content, created_at, topic_id) VALUES (%s, %s, %s, %s, %s)",
            (user_id, title, content, now, topic_id),
        )
    else:
        cur.execute(
            "INSERT INTO posts (user_id, title, content, created_at) VALUES (%s, %s, %s, %s)",
            (user_id, title, content, now),
        )
    cur.execute("SELECT LAST_INSERT_ID()")
    post_id = cur.fetchone()[0]

    return RedirectResponse(f"/post/{post_id}", status_code=302)

# ================== 帖子详情 + 评论 ==================

@app.get("/post/{post_id}", response_class=HTMLResponse)
async def post_detail(post_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username, p.user_id, t.name
        FROM posts p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN topics t ON p.topic_id = t.id
        WHERE p.id = %s AND p.is_deleted = 0
    """, (post_id,))
    row = cur.fetchone()

    if not row:
        return HTMLResponse("帖子不存在。<a href='/'>返回首页</a>", status_code=404)

    post = {
        "id": row[0],
        "title": row[1],
        "content": row[2],
        "created_at": row[3],
        "username": row[4],
        "user_id": row[5],
        "topic_name": row[6],
    }

    cur.execute("""
        SELECT c.id, c.content, c.created_at, u.username
        FROM comments c
        JOIN users u ON c.user_id = u.id
        WHERE c.post_id = %s AND c.is_deleted = 0
        ORDER BY c.id ASC
    """, (post_id,))
    comments = cur.fetchall()

    admin_flag = is_admin(username) if username else False

    return templates.TemplateResponse(
        "post_detail.html",
        {
            "request": request,
            "username": username,
            "post": post,
            "comments": comments,
            "is_admin": admin_flag,
        },
    )

# ================== 评论提交 ==================

@app.post("/comment")
async def add_comment(post_id: int = Form(), content: str = Form(), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()
    cur.execute(
        "INSERT INTO comments (post_id, user_id, content, created_at) VALUES (%s, %s, %s, %s)",
        (post_id, user_id, content, now),
    )

    msg = {
        "type": "new_comment",
        "content": content,
        "username": username,
        "created_at": now.strftime("%Y-%m-%d %H:%M"),
    }
    await comment_manager.broadcast(post_id, msg)

    return RedirectResponse(f"/post/{post_id}", status_code=302)

# ================== 帖子编辑（新增） ==================  # <<< 新增的这一节

@app.get("/post/{post_id}/edit", response_class=HTMLResponse)
async def edit_post_page(post_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    cur.execute("""
        SELECT id, title, content, user_id, is_deleted
        FROM posts
        WHERE id = %s
    """, (post_id,))
    row = cur.fetchone()
    if not row or row[4] == 1:
        return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

    post = {
        "id": row[0],
        "title": row[1],
        "content": row[2],
        "user_id": row[3],
    }

    user_id = get_user_id(username)
    if user_id != post["user_id"] and not is_admin(username):
        return HTMLResponse("你没有权限编辑这个帖子。<a href='/'>返回首页</a>", status_code=403)

    return templates.TemplateResponse(
        "edit_post.html",
        {"request": request, "username": username, "post": post},
    )

@app.post("/post/{post_id}/edit")
async def edit_post(post_id: int, title: str = Form(), content: str = Form(), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    cur.execute("""
        SELECT user_id, is_deleted FROM posts WHERE id = %s
    """, (post_id,))
    row = cur.fetchone()
    if not row or row[1] == 1:
        return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

    post_user_id = row[0]
    user_id = get_user_id(username)
    if user_id != post_user_id and not is_admin(username):
        return HTMLResponse("你没有权限编辑这个帖子。<a href='/'>返回首页</a>", status_code=403)

    cur.execute(
        "UPDATE posts SET title = %s, content = %s WHERE id = %s",
        (title, content, post_id)
    )
    conn.commit()

    return RedirectResponse(f"/post/{post_id}", status_code=302)