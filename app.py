from fastapi import FastAPI, Request, Form, Cookie, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from passlib.context import CryptContext
import jwt  # PyJWT
from datetime import datetime, timedelta
from typing import Optional

import pymysql
import os

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

        # 更新在线人数
        await self.broadcast_online_count(post_id)

    def disconnect(self, websocket: WebSocket, post_id: int, username: str | None):
        if post_id in self.connections and websocket in self.connections[post_id]:
            self.connections[post_id].remove(websocket)

        # 从正在输入里移除
        if username:
            if post_id in self.typing_users and username in self.typing_users[post_id]:
                self.typing_users[post_id].remove(username)

    async def broadcast(self, post_id: int, message: dict):
        conns = self.connections.get(post_id, [])
        for ws in list(conns):
            try:
                await ws.send_json(message)
            except Exception:
                pass

    async def broadcast_online_count(self, post_id: int):
        count = len(self.connections.get(post_id, []))
        msg = {
            "type": "online_count",
            "count": count,
        }
        await self.broadcast(post_id, msg)

    async def set_typing(self, post_id: int, username: str, is_typing: bool):
        if post_id not in self.typing_users:
            self.typing_users[post_id] = set()

        if is_typing:
            self.typing_users[post_id].add(username)
        else:
            self.typing_users[post_id].discard(username)

        # 把当前正在输入的用户名列表广播出去
        msg = {
            "type": "typing",
            "users": list(self.typing_users[post_id]),
        }
        await self.broadcast(post_id, msg)

comment_manager = CommentManager()

# ================== MySQL 直连配置（pymysql） ==================

DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "Hhaazzeell602")  # 本地开发用
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
    password VARCHAR(255)
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

# ================== JWT 工具函数 ==================

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

# ================== 页面路由 ==================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    # 查询所有帖子，按时间倒序
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username
        FROM posts p
        JOIN users u ON p.user_id = u.id
        WHERE p.is_deleted = 0
        ORDER BY p.created_at DESC
    """)
    posts = cur.fetchall()  # 列表，每个元素是 (id, title, content, created_at, username)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "username": username,
            "posts": posts,
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

@app.websocket("/ws/comments/{post_id}")
async def comments_websocket(websocket: WebSocket, post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    # 如果你希望未登录也能看评论，把下面这段去掉
    if not username:
        await websocket.close()
        return

    await comment_manager.connect(websocket, post_id, username)

    # 刚连上时就推送当前在线人数
    await comment_manager.broadcast_online_count(post_id)

    try:
        while True:
            # 接收客户端发来的 JSON，主要用于 "typing" 状态
            data_text = await websocket.receive_text()
            try:
                import json
                data = json.loads(data_text)
            except Exception:
                continue

            if data.get("type") == "typing" and username:
                is_typing = bool(data.get("is_typing"))
                await comment_manager.set_typing(post_id, username, is_typing)

    except WebSocketDisconnect:
        comment_manager.disconnect(websocket, post_id, username)
        # 断开后更新在线人数和 typing 状态
        await comment_manager.broadcast_online_count(post_id)
        await comment_manager.set_typing(post_id, username, False)

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
    cur.execute("SELECT password FROM users WHERE username = %s", (username,))
    row = cur.fetchone()
    if not row or not pwd_context.verify(password, row[0]):
        return HTMLResponse("用户不存在或密码错误。<a href='/login'>返回</a>", status_code=400)

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
    return templates.TemplateResponse("new_post.html", {"request": request, "username": username})

@app.post("/post/new")
async def new_post(title: str = Form(), content: str = Form(), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username:
        return RedirectResponse("/login", status_code=302)

    user_id = get_user_id(username)
    if not user_id:
        return RedirectResponse("/login", status_code=302)

    now = datetime.utcnow()
    cur.execute(
        "INSERT INTO posts (user_id, title, content, created_at) VALUES (%s, %s, %s, %s)",
        (user_id, title, content, now),
    )
    # 获取刚插入的帖子 id
    cur.execute("SELECT LAST_INSERT_ID()")
    post_id = cur.fetchone()[0]

    return RedirectResponse(f"/post/{post_id}", status_code=302)

# ================== 帖子详情 + 评论 ==================

@app.get("/post/{post_id}", response_class=HTMLResponse)
async def post_detail(post_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    # 查帖子
    cur.execute("""
        SELECT p.id, p.title, p.content, p.created_at, u.username
        FROM posts p
        JOIN users u ON p.user_id = u.id
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
    }

    # 查评论
    cur.execute("""
        SELECT c.content, c.created_at, u.username
        FROM comments c
        JOIN users u ON c.user_id = u.id
        WHERE c.post_id = %s AND c.is_deleted = 0
        ORDER BY c.created_at ASC
    """, (post_id,))
    comments = cur.fetchall()  # 每个元素: (content, created_at, username)

    return templates.TemplateResponse(
        "post_detail.html",
        {
            "request": request,
            "username": username,
            "post": post,
            "comments": comments,
        },
    )

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

    # 让 WebSocket 客户端“实时”看到这条新评论
    msg = {
        "type": "new_comment",
        "content": content,
        "username": username,
        "created_at": now.strftime("%Y-%m-%d %H:%M"),
    }
    # 注意：这里是异步函数，要 await
    await comment_manager.broadcast(post_id, msg)

    return RedirectResponse(f"/post/{post_id}", status_code=302)