from fastapi import FastAPI, Request, Form, Cookie, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from fastapi import Query
from fastapi import HTTPException

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

# ================== 建表：用户、帖子、评论、板块、举报 ==================

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
    topic_id INT NULL,
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

cur.execute("""
CREATE TABLE IF NOT EXISTS topics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) UNIQUE,
    description TEXT,
    created_by INT,
    is_approved TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME,
    FOREIGN KEY (created_by) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS reports (
    id INT PRIMARY KEY AUTO_INCREMENT,
    type ENUM('post', 'comment') NOT NULL,
    target_id INT NOT NULL,
    reporter_id INT NOT NULL,
    reason TEXT,
    status ENUM('pending', 'handled') NOT NULL DEFAULT 'pending',
    created_at DATETIME,
    FOREIGN KEY (reporter_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS notifications (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,              -- 通知接收者
    type VARCHAR(50) NOT NULL,         -- 通知类型，如 new_comment_on_post / like_on_post
    related_post_id INT NULL,
    related_comment_id INT NULL,
    from_user_id INT NULL,             -- 触发者
    message TEXT NOT NULL,
    is_read TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (from_user_id) REFERENCES users(id),
    FOREIGN KEY (related_post_id) REFERENCES posts(id),
    FOREIGN KEY (related_comment_id) REFERENCES comments(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

conn.commit()

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


def get_post_likes_count(post_id: int) -> int:
    cur.execute("SELECT COUNT(*) FROM post_likes WHERE post_id = %s", (post_id,))
    row = cur.fetchone()
    return row[0] if row else 0


def user_liked_post(user_id: int, post_id: int) -> bool:
    cur.execute("SELECT 1 FROM post_likes WHERE post_id = %s AND user_id = %s", (post_id, user_id))
    row = cur.fetchone()
    return bool(row)


def is_admin(username: str) -> bool:
    cur.execute("SELECT is_admin FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    return bool(row and row[0] == 1)


def is_banned(username: str) -> bool:
    cur.execute("SELECT is_banned FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    return bool(row and row[0] == 1)


def create_notification(
    user_id: int,
    type_: str,
    message: str,
    related_post_id: Optional[int] = None,
    related_comment_id: Optional[int] = None,
    from_user_id: Optional[int] = None,
):
    now = datetime.utcnow()
    cur.execute(
        """
        INSERT INTO notifications
        (user_id, type, message, related_post_id, related_comment_id, from_user_id, is_read, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s)
        """,
        (user_id, type_, message, related_post_id, related_comment_id, from_user_id, now),
    )
    conn.commit()


def get_unread_notifications_count(user_id: int) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM notifications WHERE user_id = %s AND is_read = 0",
        (user_id,),
    )
    row = cur.fetchone()
    return row[0] if row else 0


# ================== 页面路由：首页 / 登录 / 注册 / 退出 ==================


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    # topics
    cur.execute("""
        SELECT id, name
        FROM topics
        WHERE is_approved = 1
        ORDER BY name ASC
    """)
    topics = cur.fetchall()

    # posts
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
        FROM posts p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN topics t ON p.topic_id = t.id
        WHERE p.is_deleted = 0
        ORDER BY p.id DESC
    """)
    raw_posts = cur.fetchall()

    # 组装带点赞数的 posts：[(id, title, content, created_at, username, topic_name, likes), ...]
    posts = []
    for p in raw_posts:
        post_id = p[0]
        likes = get_post_likes_count(post_id)
        posts.append((p[0], p[1], p[2], p[3], p[4], p[5], likes))

    admin_flag = is_admin(username) if username else False

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "username": username,
            "posts": posts,
            "topics": topics,
            "is_admin": admin_flag,
        },
    )


@app.get("/api/posts")
async def api_posts():
    # 查询当前未删除的帖子及 topic 名称和点赞数
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
        FROM posts p
        JOIN users u ON p.user_id = u.id
        LEFT JOIN topics t ON p.topic_id = t.id
        WHERE p.is_deleted = 0
        ORDER BY p.id DESC
    """)
    rows = cur.fetchall()

    posts = []
    for row in rows:
        post_id = row[0]
        likes = get_post_likes_count(post_id)
        posts.append({
            "id": row[0],
            "title": row[1],
            "content": row[2],
            "created_at": row[3].strftime("%Y-%m-%d %H:%M") if row[3] else "",
            "username": row[4],
            "topic_name": row[5],
            "likes": likes,
        })

    return JSONResponse(posts)


@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query("", alias="q"),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    keyword = q.strip()

    results = []
    if keyword:
        like = f"%{keyword}%"
        cur.execute("""
            SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
            FROM posts p
            JOIN users u ON p.user_id = u.id
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE p.is_deleted = 0
              AND (
                    p.title LIKE %s
                 OR p.content LIKE %s
                 OR u.username LIKE %s
                 OR (t.name IS NOT NULL AND t.name LIKE %s)
              )
            ORDER BY p.id DESC
        """, (like, like, like, like))
        rows = cur.fetchall()
        for row in rows:
            post_id = row[0]
            likes = get_post_likes_count(post_id)
            results.append((
                row[0],  # id
                row[1],  # title
                row[2],  # content
                row[3],  # created_at
                row[4],  # username
                row[5],  # topic_name
                likes,   # likes
            ))

    admin_flag = is_admin(username) if username else False

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "username": username,
            "is_admin": admin_flag,
            "q": keyword,
            "results": results,
        },
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


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    # 查一下当前用户信息（这里先只要 is_admin / is_banned）
    cur.execute("SELECT id, is_admin, is_banned FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if not row:
        return RedirectResponse("/login", status_code=302)

    user_id, user_is_admin, user_is_banned = row

    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "username": username,
            "user_id": user_id,
            "is_admin": bool(user_is_admin),
            "is_banned": bool(user_is_banned),
        },
    )


@app.post("/settings/password")
async def change_password(
    old_password: str = Form(),
    new_password: str = Form(),
    new_password2: str = Form(),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    # 取出当前用户的 hash 密码
    cur.execute("SELECT password FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if not row:
        return RedirectResponse("/login", status_code=302)

    hashed = row[0]

    # 验证旧密码
    if not pwd_context.verify(old_password, hashed):
        return HTMLResponse("旧密码错误。<a href='/settings'>返回设置</a>", status_code=400)

    # 检查新密码一致
    if new_password != new_password2:
        return HTMLResponse("两次输入的新密码不一致。<a href='/settings'>返回设置</a>", status_code=400)

    if len(new_password) < 6:
        return HTMLResponse("新密码太短，至少 6 位。<a href='/settings'>返回设置</a>", status_code=400)

    # 更新密码
    new_hashed = pwd_context.hash(new_password)
    cur.execute("UPDATE users SET password = %s WHERE username = %s", (new_hashed, username))
    conn.commit()

    return HTMLResponse("密码修改成功，下次请使用新密码登录。<a href='/'>返回首页</a>")


@app.get("/user/{username}", response_class=HTMLResponse)
async def user_profile(username: str, request: Request, token: str = Cookie(None)):
    current_username = get_username_from_token(token)

    # 查这个用户是否存在
    cur.execute("SELECT id, is_admin, is_banned FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if not row:
        return HTMLResponse("用户不存在。<a href='/'>返回首页</a>", status_code=404)

    user_id, user_is_admin, user_is_banned = row

    # 查 TA 发的帖子
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, t.name
        FROM posts p
        LEFT JOIN topics t ON p.topic_id = t.id
        WHERE p.user_id = %s AND p.is_deleted = 0
        ORDER BY p.id DESC
    """, (user_id,))
    posts = cur.fetchall()

    # 计算每个帖子的点赞数
    posts_with_likes = []
    for p in posts:
        post_id = p[0]
        likes = get_post_likes_count(post_id)
        posts_with_likes.append((
            p[0],  # id
            p[1],  # title
            p[2],  # content
            p[3],  # created_at
            p[4],  # topic_name
            likes, # likes
        ))

    current_is_admin = is_admin(current_username) if current_username else False

    return templates.TemplateResponse(
        "user_profile.html",
        {
            "request": request,
            "username": current_username,   # 当前登录用户
            "profile_username": username,   # 正在查看的用户
            "profile_user_id": user_id,
            "profile_is_admin": bool(user_is_admin),
            "profile_is_banned": bool(user_is_banned),
            "posts": posts_with_likes,
            "current_is_admin": current_is_admin,
        },
    )


@app.get("/notifications", response_class=HTMLResponse)
async def notifications_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    # 查询最近 100 条通知
    cur.execute("""
        SELECT
            n.id,              -- 0
            n.type,            -- 1
            n.message,         -- 2
            n.is_read,         -- 3
            n.related_post_id, -- 4
            n.related_comment_id, -- 5
            n.created_at,      -- 6
            fu.username        -- 7 from_username
        FROM notifications n
        LEFT JOIN users fu ON n.from_user_id = fu.id
        WHERE n.user_id = %s
        ORDER BY n.created_at DESC
        LIMIT 100
    """, (user_id,))
    rows = cur.fetchall()

    # 统计未读数量
    unread_count = get_unread_notifications_count(user_id)

    return templates.TemplateResponse(
        "notifications.html",
        {
            "request": request,
            "username": username,
            "notifications": rows,
            "unread_count": unread_count,
        },
    )


@app.post("/notifications/read_all")
async def notifications_read_all(token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    cur.execute("UPDATE notifications SET is_read = 1 WHERE user_id = %s AND is_read = 0", (user_id,))
    conn.commit()
    return RedirectResponse("/notifications", status_code=302)


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


# ================== 管理员路由：删帖 / 删评 / 用户管理 / 板块管理 / 举报管理 ==================


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

    cur.execute("""
        SELECT t.id, t.name, t.description, t.is_approved, t.created_at, u.username
        FROM topics t
        LEFT JOIN users u ON t.created_by = u.id
        ORDER BY t.created_at DESC
    """)
    topics = cur.fetchall()

    return templates.TemplateResponse(
        "admin_topics.html",
        {"request": request, "username": username, "topics": topics},
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

    cur.execute("DELETE FROM topics WHERE id = %s", (topic_id,))
    conn.commit()
    return RedirectResponse("/admin/topics", status_code=302)


@app.get("/admin/reports", response_class=HTMLResponse)
async def admin_reports(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("""
        SELECT r.id, r.type, r.target_id, u.username, r.reason, r.status, r.created_at
        FROM reports r
        JOIN users u ON r.reporter_id = u.id
        WHERE r.status = 'pending'
        ORDER BY r.created_at DESC
    """)
    reports = cur.fetchall()

    return templates.TemplateResponse(
        "admin_reports.html",
        {"request": request, "username": username, "reports": reports},
    )


@app.post("/admin/report/{report_id}/handle")
async def admin_handle_report(report_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("UPDATE reports SET status = 'handled' WHERE id = %s", (report_id,))
    conn.commit()
    return RedirectResponse("/admin/reports", status_code=302)


@app.get("/admin/inbox", response_class=HTMLResponse)
async def admin_inbox(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or not is_admin(username):
        return RedirectResponse("/", status_code=302)

    cur.execute("SELECT COUNT(*) FROM topics WHERE is_approved = 0")
    pending_topics = cur.fetchone()[0] or 0

    cur.execute("SELECT COUNT(*) FROM reports WHERE status = 'pending'")
    pending_reports = cur.fetchone()[0] or 0

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


# ================== 发帖 / 板块创建 ==================


@app.get("/post/new", response_class=HTMLResponse)
async def new_post_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

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
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()

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


@app.get("/topic/new", response_class=HTMLResponse)
async def new_topic_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)
    return templates.TemplateResponse(
        "new_topic.html",
        {"request": request, "username": username}
    )


@app.post("/topic/new")
async def new_topic(name: str = Form(), description: str = Form(""), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    name = name.strip()
    if not name:
        return HTMLResponse("板块名称不能为空。<a href='/topic/new'>返回</a>", status_code=400)

    now = datetime.utcnow()
    try:
        cur.execute(
            "INSERT INTO topics (name, description, created_by, is_approved, created_at) VALUES (%s, %s, %s, 0, %s)",
            (name, description, user_id, now),
        )
        conn.commit()
        return HTMLResponse("板块创建申请已提交，等待管理员审核。<a href='/'>返回首页</a>")
    except pymysql.err.IntegrityError:
        return HTMLResponse("该板块名称已存在，请换一个。<a href='/topic/new'>返回</a>", status_code=400)


@app.get("/topic/{topic_id}", response_class=HTMLResponse)
async def topic_page(topic_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    # 查这个 topic
    cur.execute("SELECT id, name FROM topics WHERE id = %s AND is_approved = 1", (topic_id,))
    topic = cur.fetchone()
    if not topic:
        return HTMLResponse("板块不存在或未通过审核。<a href='/'>返回首页</a>", status_code=404)

    # 查属于这个 topic 的帖子
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username
        FROM posts p
        JOIN users u ON p.user_id = u.id
        WHERE p.is_deleted = 0 AND p.topic_id = %s
        ORDER BY p.id DESC
    """, (topic_id,))
    posts = cur.fetchall()

    admin_flag = is_admin(username) if username else False

    return templates.TemplateResponse(
        "topic_posts.html",
        {
            "request": request,
            "username": username,
            "is_admin": admin_flag,
            "topic": topic,   # (id, name)
            "posts": posts,
        },
    )


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

    likes_count = get_post_likes_count(post_id)
    user_like_flag = False
    if username:
        uid = get_user_id(username)
        if uid:
            user_like_flag = user_liked_post(uid, post_id)

    return templates.TemplateResponse(
        "post_detail.html",
        {
            "request": request,
            "username": username,
            "post": post,
            "comments": comments,
            "is_admin": admin_flag,
            "likes_count": likes_count,
            "user_liked": user_like_flag,
        },
    )


@app.post("/post/{post_id}/like")
async def like_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    # 检查帖子存在且未删除
    cur.execute("SELECT id, user_id, title FROM posts WHERE id = %s AND is_deleted = 0", (post_id,))
    row = cur.fetchone()
    if not row:
        return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

    post_id_db, post_owner_id, post_title = row

    now = datetime.utcnow()

    # 如果已经点过赞，则取消赞；否则插入新赞
    if user_liked_post(user_id, post_id):
        cur.execute("DELETE FROM post_likes WHERE post_id = %s AND user_id = %s", (post_id, user_id))
    else:
        cur.execute(
            "INSERT INTO post_likes (post_id, user_id, created_at) VALUES (%s, %s, %s)",
            (post_id, user_id, now),
        )
        # 只有点赞时发通知，且不给自己点赞发通知
        if post_owner_id != user_id:
            msg = f"{username} 给你的帖子《{post_title}》点了赞"
            create_notification(
                user_id=post_owner_id,
                type_="like_on_post",
                message=msg,
                related_post_id=post_id,
                from_user_id=user_id,
            )

    conn.commit()

    # 操作完回到帖子详情页
    return RedirectResponse(f"/post/{post_id}", status_code=302)


@app.post("/api/post/{post_id}/like")
async def api_like_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return JSONResponse({"error": "not_logged_in"}, status_code=401)

    user_id = get_user_id(username)
    if not user_id:
        return JSONResponse({"error": "no_user"}, status_code=401)

    # 检查帖子是否存在
    cur.execute("SELECT id, user_id, title FROM posts WHERE id = %s AND is_deleted = 0", (post_id,))
    row = cur.fetchone()
    if not row:
        return JSONResponse({"error": "post_not_found"}, status_code=404)

    post_id_db, post_owner_id, post_title = row

    now = datetime.utcnow()

    # 切换点赞状态
    if user_liked_post(user_id, post_id):
        # 已点赞 → 取消赞
        cur.execute("DELETE FROM post_likes WHERE post_id = %s AND user_id = %s", (post_id, user_id))
        liked = False
    else:
        # 未点赞 → 点赞
        cur.execute(
            "INSERT INTO post_likes (post_id, user_id, created_at) VALUES (%s, %s, %s)",
            (post_id, user_id, now),
        )
        liked = True

        # 点赞时发通知
        if post_owner_id != user_id:
            msg = f"{username} 给你的帖子《{post_title}》点了赞"
            create_notification(
                user_id=post_owner_id,
                type_="like_on_post",
                message=msg,
                related_post_id=post_id,
                from_user_id=user_id,
            )

    conn.commit()
    # 返回最新点赞数与当前状态
    likes = get_post_likes_count(post_id)
    return JSONResponse({"liked": liked, "likes": likes})


# 举报帖子
@app.post("/report/post/{post_id}")
async def report_post(
    post_id: int,
    reason: str = Form(""),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    reporter_id = get_user_id(username)
    if not reporter_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()

    cur.execute("SELECT id FROM posts WHERE id = %s AND is_deleted = 0", (post_id,))
    row = cur.fetchone()
    if not row:
        return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

    cur.execute(
        "INSERT INTO reports (type, target_id, reporter_id, reason, status, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
        ("post", post_id, reporter_id, reason, "pending", now),
    )
    conn.commit()

    return HTMLResponse("举报已提交，感谢你的反馈。<a href='/post/%d'>返回帖子</a>" % post_id)


# 举报评论
@app.post("/report/comment/{comment_id}")
async def report_comment(
    comment_id: int,
    post_id: int = Form(...),
    reason: str = Form(""),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    reporter_id = get_user_id(username)
    if not reporter_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()

    cur.execute("SELECT id FROM comments WHERE id = %s AND is_deleted = 0", (comment_id,))
    row = cur.fetchone()
    if not row:
        return HTMLResponse("评论不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

    cur.execute(
        "INSERT INTO reports (type, target_id, reporter_id, reason, status, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
        ("comment", comment_id, reporter_id, reason, "pending", now),
    )
    conn.commit()

    return HTMLResponse("举报已提交，感谢你的反馈。<a href='/post/%d'>返回帖子</a>" % post_id)


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
    # 获取新评论ID
    cur.execute("SELECT LAST_INSERT_ID()")
    comment_id = cur.fetchone()[0]

    # 给帖子作者发送通知（如果评论人不是作者本人）
    cur.execute("SELECT user_id, title FROM posts WHERE id = %s AND is_deleted = 0", (post_id,))
    row = cur.fetchone()
    if row:
        post_owner_id, post_title = row
        if post_owner_id != user_id:
            # 构造简单消息
            msg = f"{username} 评论了你的帖子《{post_title}》"
            create_notification(
                user_id=post_owner_id,
                type_="new_comment_on_post",
                message=msg,
                related_post_id=post_id,
                related_comment_id=comment_id,
                from_user_id=user_id,
            )

    msg = {
        "type": "new_comment",
        "content": content,
        "username": username,
        "created_at": now.strftime("%Y-%m-%d %H:%M"),
    }
    await comment_manager.broadcast(post_id, msg)

    return RedirectResponse(f"/post/{post_id}", status_code=302)


# ================== 帖子编辑 ==================


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