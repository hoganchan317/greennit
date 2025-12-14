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
import uuid
import secrets
import string

# ================== 基础配置 ==================

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.middleware("http")
async def check_session_middleware(request: Request, call_next):
    """Middleware to ensure session token is still active. Public paths are allowed.
    If session is revoked (is_active = 0) we delete the cookie and redirect to /login.
    """
    path = request.url.path
    # exact public paths
    public_exact = {"/login", "/register", "/"}
    # prefix-based public paths
    public_prefixes = ["/static", "/favicon.ico", "/api/posts"]

    if path in public_exact or any(path.startswith(p) for p in public_prefixes):
        return await call_next(request)

    token = request.cookies.get("token")
    if token:
        try:
            session_token = get_session_from_token(token)
        except Exception:
            session_token = None
        if session_token and not is_session_valid(session_token):
            response = RedirectResponse("/login", status_code=302)
            response.delete_cookie("token")
            return response

    return await call_next(request)

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
        # websocket -> post_id
        self.ws_to_post: dict[WebSocket, int] = {}
        # websocket -> username
        self.ws_to_username: dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, post_id: int, username: str | None):
        await websocket.accept()
        self.connections.setdefault(post_id, []).append(websocket)
        # track websocket mappings
        self.ws_to_post[websocket] = post_id
        if username:
            self.ws_to_username[websocket] = username
        await self.broadcast_online_count(post_id)

    def disconnect(self, websocket: WebSocket, post_id: int, username: str | None):
        if post_id in self.connections and websocket in self.connections[post_id]:
            self.connections[post_id].remove(websocket)
        # cleanup mappings
        if websocket in self.ws_to_post:
            try:
                del self.ws_to_post[websocket]
            except KeyError:
                pass
        if websocket in self.ws_to_username:
            try:
                del self.ws_to_username[websocket]
            except KeyError:
                pass
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

    async def close_by_username(self, username: str):
        # Close all websockets for a given username.
        # Make a copy of items to avoid mutation during iteration.
        items = list(self.ws_to_username.items())
        for ws, u in items:
            if u == username:
                post_id = self.ws_to_post.get(ws)
                try:
                    await ws.close()
                except Exception:
                    pass
                # cleanup
                try:
                    if post_id is not None:
                        self.disconnect(ws, post_id, u)
                except Exception:
                    pass


comment_manager = CommentManager()

# ================== 私信（实时聊天）管理 ==================


class ChatManager:
    def __init__(self):
        # key: username -> list[WebSocket]
        self.connections: dict[str, list[WebSocket]] = {}
        # websocket -> username
        self.ws_to_username: dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, username: str):
        await websocket.accept()
        self.connections.setdefault(username, []).append(websocket)
        self.ws_to_username[websocket] = username

    def disconnect(self, websocket: WebSocket):
        username = self.ws_to_username.get(websocket)
        if username and websocket in self.connections.get(username, []):
            self.connections[username].remove(websocket)
        if websocket in self.ws_to_username:
            try:
                del self.ws_to_username[websocket]
            except KeyError:
                pass

    async def broadcast_to_user(self, username: str, message: dict):
        conns = self.connections.get(username, [])
        for ws in list(conns):
            try:
                await ws.send_text(json.dumps(message, ensure_ascii=False))
            except Exception:
                pass

    async def broadcast_to_users(self, usernames: list, message: dict):
        for u in usernames:
            await self.broadcast_to_user(u, message)

    async def close_by_username(self, username: str):
        conns = list(self.connections.get(username, []))
        for ws in conns:
            try:
                await ws.close()
            except Exception:
                pass
            try:
                self.disconnect(ws)
            except Exception:
                pass


chat_manager = ChatManager()


class GroupChatManager:
    def __init__(self):
        # key: group_id -> list[WebSocket]
        self.connections: dict[int, list[WebSocket]] = {}
        # websocket -> group_id
        self.ws_to_group: dict[WebSocket, int] = {}
        # websocket -> username
        self.ws_to_username: dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, group_id: int, username: str | None):
        await websocket.accept()
        self.connections.setdefault(group_id, []).append(websocket)
        self.ws_to_group[websocket] = group_id
        if username:
            self.ws_to_username[websocket] = username
        await self.broadcast_online_count(group_id)

    def disconnect(self, websocket: WebSocket):
        group_id = self.ws_to_group.get(websocket)
        username = self.ws_to_username.get(websocket)
        if group_id in self.connections and websocket in self.connections[group_id]:
            try:
                self.connections[group_id].remove(websocket)
            except ValueError:
                pass
        if websocket in self.ws_to_group:
            try:
                del self.ws_to_group[websocket]
            except KeyError:
                pass
        if websocket in self.ws_to_username:
            try:
                del self.ws_to_username[websocket]
            except KeyError:
                pass

    async def broadcast_to_group(self, group_id: int, message: dict):
        conns = self.connections.get(group_id, [])
        for ws in list(conns):
            try:
                await ws.send_text(json.dumps(message, ensure_ascii=False))
            except Exception:
                pass

    async def broadcast_online_count(self, group_id: int):
        count = len(self.connections.get(group_id, []))
        msg = {"type": "online_count", "count": count}
        await self.broadcast_to_group(group_id, msg)

    async def close_by_username(self, username: str):
        items = list(self.ws_to_username.items())
        for ws, u in items:
            if u == username:
                try:
                    await ws.close()
                except Exception:
                    pass
                try:
                    self.disconnect(ws)
                except Exception:
                    pass


group_chat_manager = GroupChatManager()

# ================== MySQL 直连配置（pymysql） ==================

DB_USER = os.environ.get("DB_USER", "root")
DB_PASS = os.environ.get("DB_PASS", "Hhaazzeell602")  # 本地默认密码
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "greennit")

def get_db_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
    )

# ============ DATABASE SCHEMA INITIALIZATION ============
_schema_conn = get_db_conn()
_schema_cur = _schema_conn.cursor()

# 1. users (基础表，无依赖)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    is_admin TINYINT(1) NOT NULL DEFAULT 0,
    is_banned TINYINT(1) NOT NULL DEFAULT 0,
    avatar_url VARCHAR(500) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 2. topics (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS topics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    created_by INT NOT NULL,
    is_approved TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (created_by) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 3. forums (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS forums (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) UNIQUE NOT NULL,
    description TEXT,
    creator_id INT NOT NULL,
    is_approved TINYINT(1) NOT NULL DEFAULT 0,
    is_private TINYINT(1) NOT NULL DEFAULT 0,
    invite_code VARCHAR(20) UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (creator_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 4. posts (依赖 users, topics, forums)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    title VARCHAR(200) NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    topic_id INT NULL,
    forum_id INT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (topic_id) REFERENCES topics(id),
    FOREIGN KEY (forum_id) REFERENCES forums(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 5. comments (依赖 posts, users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS comments (
    id INT PRIMARY KEY AUTO_INCREMENT,
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 6. post_likes (依赖 posts, users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS post_likes (
    post_id INT NOT NULL,
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (post_id, user_id),
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 7. reports (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS reports (
    id INT PRIMARY KEY AUTO_INCREMENT,
    type ENUM('post', 'comment', 'user') NOT NULL,
    target_id INT NOT NULL,
    reporter_id INT NOT NULL,
    reason TEXT NOT NULL,
    status ENUM('pending', 'handled') NOT NULL DEFAULT 'pending',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (reporter_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 8. notifications (依赖 users, posts, comments)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS notifications (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    type VARCHAR(50) NOT NULL,
    related_post_id INT NULL,
    related_comment_id INT NULL,
    from_user_id INT NULL,
    message TEXT NOT NULL,
    is_read TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (from_user_id) REFERENCES users(id),
    FOREIGN KEY (related_post_id) REFERENCES posts(id),
    FOREIGN KEY (related_comment_id) REFERENCES comments(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 9. private_messages (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS private_messages (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sender_id INT NOT NULL,
    receiver_id INT NOT NULL,
    content TEXT NOT NULL,
    is_read TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sender_id) REFERENCES users(id),
    FOREIGN KEY (receiver_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 10. user_sessions (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS user_sessions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    device_info VARCHAR(500),
    ip_address VARCHAR(45),
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_active DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 11. chat_groups (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS chat_groups (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    avatar_url VARCHAR(500),
    creator_id INT NOT NULL,
    invite_code VARCHAR(20) UNIQUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (creator_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 12. group_members (依赖 chat_groups, users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS group_members (
    id INT PRIMARY KEY AUTO_INCREMENT,
    group_id INT NOT NULL,
    user_id INT NOT NULL,
    role ENUM('owner', 'admin', 'member') NOT NULL DEFAULT 'member',
    is_muted TINYINT(1) NOT NULL DEFAULT 0,
    muted_until DATETIME NULL,
    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES chat_groups(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 13. group_messages (依赖 chat_groups, users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS group_messages (
    id INT PRIMARY KEY AUTO_INCREMENT,
    group_id INT NOT NULL,
    sender_id INT NOT NULL,
    content TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES chat_groups(id),
    FOREIGN KEY (sender_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 14. forum_members (依赖 forums, users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS forum_members (
    id INT PRIMARY KEY AUTO_INCREMENT,
    forum_id INT NOT NULL,
    user_id INT NOT NULL,
    role ENUM('owner', 'admin', 'member') NOT NULL DEFAULT 'member',
    joined_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (forum_id) REFERENCES forums(id),
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# 15. forum_requests (依赖 users)
_schema_cur.execute("""
CREATE TABLE IF NOT EXISTS forum_requests (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    reason TEXT,
    status ENUM('pending', 'approved', 'rejected') NOT NULL DEFAULT 'pending',
    admin_reply TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

_schema_conn.commit()
_schema_cur.close()
_schema_conn.close()
# ============ END DATABASE SCHEMA ============

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


def create_token_with_session(username: str, session_token: str) -> str:
    expire = datetime.utcnow() + timedelta(days=7)
    payload = {"sub": username, "session": session_token, "exp": expire}
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def get_session_from_token(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("session")
    except Exception:
        return None


def is_session_valid(session_token: Optional[str]) -> bool:
    if not session_token:
        return False
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_active FROM user_sessions WHERE session_token = %s", (session_token,))
        row = cur.fetchone()
    return bool(row and row[0] == 1)


def validate_auth(token: Optional[str]) -> tuple[Optional[str], bool]:
    """返回 (username, is_valid)"""
    if not token:
        return None, False
    username = get_username_from_token(token)
    if not username or is_banned(username):
        return None, False
    session_token = get_session_from_token(token)
    if not is_session_valid(session_token):
        return None, False
    return username, True


def get_user_id(username: str) -> Optional[int]:
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return row[0] if row else None


def get_post_likes_count(post_id: int) -> int:
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM post_likes WHERE post_id = %s", (post_id,))
        row = cur.fetchone()
    return row[0] if row else 0


def user_liked_post(user_id: int, post_id: int) -> bool:
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM post_likes WHERE post_id = %s AND user_id = %s", (post_id, user_id))
        row = cur.fetchone()
    return bool(row)


def is_admin(username: str) -> bool:
    if not username:
        return False
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_admin FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return bool(row and row[0] == 1)


def is_banned(username: str) -> bool:
    if not username:
        return False
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_banned FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    return bool(row and row[0] == 1)


def is_banned_by_username(username: str) -> bool:
    """Alias helper that checks ban status for a username (DB-backed)."""
    return is_banned(username)


def create_notification(
    user_id: int,
    type_: str,
    message: str,
    related_post_id: Optional[int] = None,
    related_comment_id: Optional[int] = None,
    from_user_id: Optional[int] = None,
):
    now = datetime.utcnow()
    with get_db_conn() as conn:
        cur = conn.cursor()
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
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM notifications WHERE user_id = %s AND is_read = 0",
            (user_id,),
        )
        row = cur.fetchone()
    return row[0] if row else 0


def get_unread_messages_count(user_id: int) -> int:
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM private_messages WHERE receiver_id = %s AND is_read = 0", (user_id,))
        row = cur.fetchone()
    return row[0] if row else 0


def get_conversations(user_id: int) -> list:
    """Return list of conversations for user_id.
    Each conversation is a dict: {other_id, other_username, last_message, last_at, unread_count}
    """
    conv_map: dict[int, dict] = {}
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, sender_id, receiver_id, content, is_read, created_at FROM private_messages "
            "WHERE sender_id = %s OR receiver_id = %s "
            "ORDER BY created_at ASC",
            (user_id, user_id),
        )
        rows = cur.fetchall()

    for r in rows:
        pm_id, sender_id, receiver_id, content, is_read, created_at = r
        other_id = receiver_id if sender_id == user_id else sender_id
        # initialize
        item = conv_map.get(other_id)
        if not item:
            conv_map[other_id] = {
                "other_id": other_id,
                "last_message": content,
                "last_at": created_at,
                "unread_count": 0,
            }
        else:
            # since ordered asc, later rows replace
            conv_map[other_id]["last_message"] = content
            conv_map[other_id]["last_at"] = created_at

    # compute unread counts and usernames
    results = []
    if conv_map:
        other_ids = list(conv_map.keys())
        # fetch usernames
        id_to_username = {}
        with get_db_conn() as conn:
            cur = conn.cursor()
            # build placeholder list
            placeholders = ",".join(["%s"] * len(other_ids))
            cur.execute(f"SELECT id, username FROM users WHERE id IN ({placeholders})", tuple(other_ids))
            for row in cur.fetchall():
                id_to_username[row[0]] = row[1]

            # fetch unread counts per other_id
            for other_id, item in conv_map.items():
                cur.execute(
                    "SELECT COUNT(*) FROM private_messages WHERE receiver_id = %s AND sender_id = %s AND is_read = 0",
                    (user_id, other_id),
                )
                unread_count = cur.fetchone()[0]
                results.append({
                    "other_id": other_id,
                    "other_username": id_to_username.get(other_id, "<deleted>"),
                    "last_message": item["last_message"],
                    "last_at": item["last_at"],
                    "unread_count": unread_count,
                })

    # sort by last_at desc
    results.sort(key=lambda x: x["last_at"] or datetime.min, reverse=True)
    return results


def get_messages(user1_id: int, user2_id: int) -> list:
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT p.id, p.sender_id, p.receiver_id, p.content, p.is_read, p.created_at, u.username "
            "FROM private_messages p JOIN users u ON p.sender_id = u.id "
            "WHERE (p.sender_id = %s AND p.receiver_id = %s) OR (p.sender_id = %s AND p.receiver_id = %s) "
            "ORDER BY p.created_at ASC",
            (user1_id, user2_id, user2_id, user1_id),
        )
        rows = cur.fetchall()
    msgs = []
    for r in rows:
        pm_id, sender_id, receiver_id, content, is_read, created_at, sender_username = r
        msgs.append({
            "id": pm_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_username": sender_username,
            "content": content,
            "is_read": is_read,
            "created_at": created_at,
        })
    return msgs


# ================== 群组（Group Chat）辅助函数 ==================

def get_user_groups(user_id: int) -> list:
    """获取用户加入的所有群组"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT g.id, g.name, g.description, g.creator_id, gm.role,
                   (SELECT COUNT(*) FROM group_members WHERE group_id = g.id) as member_count
            FROM chat_groups g
            JOIN group_members gm ON g.id = gm.group_id
            WHERE gm.user_id = %s
            ORDER BY g.created_at DESC
        """, (user_id,))
        return cur.fetchall()


def get_group_by_id(group_id: int) -> dict:
    """获取群组信息"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, description, avatar_url, creator_id, created_at, invite_code
            FROM chat_groups WHERE id = %s
        """, (group_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0], "name": row[1], "description": row[2],
                "avatar_url": row[3], "creator_id": row[4], "created_at": row[5],
                "invite_code": row[6]
            }
        return None


def is_group_member(group_id: int, user_id: int) -> bool:
    """检查用户是否是群成员"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user_id))
        return cur.fetchone() is not None


def get_group_role(group_id: int, user_id: int) -> str:
    """获取用户在群里的角色"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user_id))
        row = cur.fetchone()
        return row[0] if row else None


def get_group_members(group_id: int) -> list:
    """获取群成员列表，包含禁言状态"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT u.id, u.username, u.is_admin, gm.role, gm.joined_at, gm.is_muted, gm.muted_until
            FROM group_members gm
            JOIN users u ON gm.user_id = u.id
            WHERE gm.group_id = %s
            ORDER BY 
                CASE gm.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 ELSE 3 END,
                gm.joined_at ASC
        """, (group_id,))
        return cur.fetchall()


def get_group_messages(group_id: int, limit: int = 100) -> list:
    """获取群消息"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT gm.id, gm.sender_id, u.username, gm.content, gm.created_at
            FROM group_messages gm
            JOIN users u ON gm.sender_id = u.id
            WHERE gm.group_id = %s
            ORDER BY gm.created_at ASC
            LIMIT %s
        """, (group_id, limit))
        return cur.fetchall()


def generate_invite_code() -> str:
    """生成8位邀请码"""
    chars = string.ascii_uppercase + string.digits
    return ''.join(secrets.choice(chars) for _ in range(8))


def get_group_by_invite_code(code: str) -> dict:
    """通过邀请码获取群组"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, description, creator_id FROM chat_groups WHERE invite_code = %s", (code,))
        row = cur.fetchone()
        if row:
            return {"id": row[0], "name": row[1], "description": row[2], "creator_id": row[3]}
        return None


# ================== 论坛（Forums）辅助函数 ==================

def get_user_forums(user_id: int) -> list:
    """获取用户加入的所有论坛"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT f.id, f.name, f.description, f.is_private, fm.role,
                   (SELECT COUNT(*) FROM forum_members WHERE forum_id = f.id) as member_count
            FROM forums f
            JOIN forum_members fm ON f.id = fm.forum_id
            WHERE fm.user_id = %s AND f.is_approved = 1
            ORDER BY f.created_at DESC
        """, (user_id,))
        return cur.fetchall()


def get_forum_by_id(forum_id: int) -> dict:
    """获取论坛信息"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, description, creator_id, is_approved, is_private, invite_code, created_at
            FROM forums WHERE id = %s
        """, (forum_id,))
        row = cur.fetchone()
        if row:
            return {
                "id": row[0], "name": row[1], "description": row[2],
                "creator_id": row[3], "is_approved": row[4], "is_private": row[5],
                "invite_code": row[6], "created_at": row[7]
            }
        return None


def is_forum_member(forum_id: int, user_id: int) -> bool:
    """检查用户是否是论坛成员"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM forum_members WHERE forum_id = %s AND user_id = %s", (forum_id, user_id))
        return cur.fetchone() is not None


def get_forum_role(forum_id: int, user_id: int) -> str:
    """获取用户在论坛的角色"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role FROM forum_members WHERE forum_id = %s AND user_id = %s", (forum_id, user_id))
        row = cur.fetchone()
    return row[0] if row else None


def can_view_forum(forum_id: int, user_id: int, is_site_admin: bool) -> bool:
    """检查用户是否可以查看论坛"""
    forum = get_forum_by_id(forum_id)
    if not forum or not forum.get("is_approved"):
        return False
    if is_site_admin:
        return True
    if not forum.get("is_private"):
        return True
    return is_forum_member(forum_id, user_id)


def can_post_in_forum(forum_id: int, user_id: int, is_site_admin: bool) -> bool:
    """检查用户是否可以在论坛发帖"""
    if is_site_admin:
        return True
    return is_forum_member(forum_id, user_id)


def get_forum_members(forum_id: int) -> list:
    """获取论坛成员列表"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT u.id, u.username, u.is_admin, fm.role, fm.joined_at
            FROM forum_members fm
            JOIN users u ON fm.user_id = u.id
            WHERE fm.forum_id = %s
            ORDER BY 
                CASE fm.role WHEN 'admin' THEN 1 WHEN 'moderator' THEN 2 ELSE 3 END,
                fm.joined_at ASC
        """, (forum_id,))
        return cur.fetchall()


def get_forum_posts(forum_id: int, page: int = 1, per_page: int = 20) -> tuple:
    """获取论坛帖子（分页）"""
    offset = (page - 1) * per_page
    with get_db_conn() as conn:
        cur = conn.cursor()
        # 总数
        cur.execute("SELECT COUNT(*) FROM posts WHERE forum_id = %s AND is_deleted = 0", (forum_id,))
        total = cur.fetchone()[0]
        # 帖子列表
        cur.execute("""
            SELECT p.id, p.title, p.content, p.created_at, u.username,
                   (SELECT COUNT(*) FROM comments WHERE post_id = p.id AND is_deleted = 0) as comment_count,
                   (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) as like_count
            FROM posts p
            JOIN users u ON p.user_id = u.id
            WHERE p.forum_id = %s AND p.is_deleted = 0
            ORDER BY p.created_at DESC
            LIMIT %s OFFSET %s
        """, (forum_id, per_page, offset))
        posts = cur.fetchall()
    return posts, total


def get_forum_by_invite_code(code: str) -> dict:
    """通过邀请码获取论坛"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, description, is_private FROM forums WHERE invite_code = %s AND is_approved = 1", (code,))
        row = cur.fetchone()
        if row:
            return {"id": row[0], "name": row[1], "description": row[2], "is_private": row[3]}
        return None


def is_member_muted(group_id: int, user_id: int) -> bool:
    """检查成员是否被禁言并自动解除过期禁言"""
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT is_muted, muted_until FROM group_members WHERE group_id = %s AND user_id = %s",
            (group_id, user_id)
        )
        row = cur.fetchone()
        if not row:
            return False
        is_muted, muted_until = row
        if not is_muted:
            return False
        if muted_until and muted_until < datetime.utcnow():
            # 禁言已过期，自动解除
            cur.execute(
                "UPDATE group_members SET is_muted = 0, muted_until = NULL WHERE group_id = %s AND user_id = %s",
                (group_id, user_id)
            )
            conn.commit()
            return False
        return True



def render_error(request: Request, message: str, back_url: Optional[str] = None, status_code: int = 400):
    # Build a minimal nav-aware context so the navbar can render consistently
    token = request.cookies.get("token") if hasattr(request, "cookies") else None
    username = get_username_from_token(token)
    user_id = get_user_id(username) if username and not is_banned(username) else None
    unread_cnt = get_unread_notifications_count(user_id) if user_id else 0
    msg_unread = get_unread_messages_count(user_id) if user_id else 0
    is_admin_flag = is_admin(username) if username else False
    ctx = {
        "request": request,
        "message": message,
        "back_url": back_url,
        "username": username,
        "unread_count": unread_cnt,
        "message_unread_count": msg_unread,
        "is_admin": is_admin_flag,
    }
    return templates.TemplateResponse("error.html", ctx, status_code=status_code)


def _nav_context_from_request(request: Request) -> dict:
    token = request.cookies.get("token") if hasattr(request, "cookies") else None
    username = get_username_from_token(token)
    if not username or is_banned(username):
        return {"username": username, "unread_count": 0, "message_unread_count": 0, "is_admin": False}
    user_id = get_user_id(username)
    unread_cnt = get_unread_notifications_count(user_id) if user_id else 0
    msg_unread = get_unread_messages_count(user_id) if user_id else 0
    return {"username": username, "unread_count": unread_cnt, "message_unread_count": msg_unread, "is_admin": is_admin(username)}


def _inject_nav(request: Request, ctx: dict) -> dict:
    base = _nav_context_from_request(request)
    # allow explicit ctx to override base
    base.update(ctx or {})
    return base


# ================== 页面路由：首页 / 登录 / 注册 / 退出 ==================


@app.get("/", response_class=HTMLResponse)
async def home(
    request: Request,
    token: str = Cookie(None),
    page: int = Query(1, ge=1),
):
    username = get_username_from_token(token)
    PAGE_SIZE = 10
    offset = (page - 1) * PAGE_SIZE

    with get_db_conn() as conn:
        cur = conn.cursor()

        # topics
        cur.execute("""
            SELECT id, name
            FROM topics
            WHERE is_approved = 1
            ORDER BY name ASC
        """)
        topics = cur.fetchall()

        # 统计总帖子数（未删除）
        cur.execute("SELECT COUNT(*) FROM posts WHERE is_deleted = 0")
        total_posts = cur.fetchone()[0] or 0

        # 分页查询帖子
        cur.execute("""
            SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
            FROM posts p
            JOIN users u ON p.user_id = u.id
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE p.is_deleted = 0
            ORDER BY p.id DESC
            LIMIT %s OFFSET %s
        """, (PAGE_SIZE, offset))
        raw_posts = cur.fetchall()

        # TOP 热门帖子（按点赞数降序，取前 5 条）
        cur.execute("""
            SELECT p.id, p.title, p.content, p.created_at, u.username, t.name,
                   IFNULL(l.like_count, 0) AS like_count
            FROM posts p
            JOIN users u ON p.user_id = u.id
            LEFT JOIN topics t ON p.topic_id = t.id
            LEFT JOIN (
                SELECT post_id, COUNT(*) AS like_count
                FROM post_likes
                GROUP BY post_id
            ) l ON p.id = l.post_id
            WHERE p.is_deleted = 0
            ORDER BY like_count DESC, p.id DESC
            LIMIT 5
        """)
        top_posts = cur.fetchall()

    # 组装带点赞数的 posts：[(id, title, content, created_at, username, topic_name, likes), ...]
    posts = []
    for p in raw_posts:
        post_id = p[0]
        likes = get_post_likes_count(post_id)
        posts.append((p[0], p[1], p[2], p[3], p[4], p[5], likes))

    admin_flag = is_admin(username) if username else False

    # 计算分页信息
    total_pages = (total_posts + PAGE_SIZE - 1) // PAGE_SIZE
    has_prev = page > 1
    has_next = page < total_pages if total_pages > 0 else False

    return templates.TemplateResponse(
        "index.html",
        _inject_nav(request, {
            "request": request,
            "posts": posts,
            "topics": topics,
            "page": page,
            "total_pages": total_pages,
            "has_prev": has_prev,
            "has_next": has_next,
            "top_posts": top_posts,
        }),
    )


@app.get("/api/posts")
async def api_posts():
    with get_db_conn() as conn:
        cur = conn.cursor()
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


# ================== 群组（Group Chat）路由 ==================


@app.get("/groups", response_class=HTMLResponse)
async def groups_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    groups = get_user_groups(user_id)

    group_list = []
    for g in groups:
        # 获取最后一条消息
        with get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT u.username, gm.content, gm.created_at
                FROM group_messages gm
                JOIN users u ON gm.sender_id = u.id
                WHERE gm.group_id = %s
                ORDER BY gm.created_at DESC LIMIT 1
            """, (g[0],))
            last_msg = cur.fetchone()

        group_list.append({
            "id": g[0],
            "name": g[1],
            "description": g[2],
            "role": g[4],
            "member_count": g[5],
            "last_message": last_msg
        })

    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("groups.html", {
        "request": request,
        "username": username,
        "groups": group_list,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.get("/groups/new", response_class=HTMLResponse)
async def new_group_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("new_group.html", {
        "request": request,
        "username": username,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/groups/new")
async def create_group(request: Request, name: str = Form(...), description: str = Form(""), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    # 创建群组
    with get_db_conn() as conn:
        cur = conn.cursor()
        code = generate_invite_code()
        cur.execute(
            "INSERT INTO chat_groups (name, description, creator_id, invite_code, created_at) VALUES (%s, %s, %s, %s, NOW())",
            (name, description, user_id, code)
        )
        group_id = cur.lastrowid

        # 创建者自动成为 owner
        cur.execute(
            "INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (%s, %s, 'owner', NOW())",
            (group_id, user_id)
        )
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.get("/groups/{group_id}/invite", response_class=HTMLResponse)
async def invite_member_page(group_id: int, request: Request, q: str = Query(None), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)

    # 只有 owner 和 admin 可以邀请
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有邀请成员的权限。", f"/groups/{group_id}")

    # 获取群信息包含 invite_code
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, description, avatar_url, creator_id, created_at, invite_code
            FROM chat_groups WHERE id = %s
        """, (group_id,))
        row = cur.fetchone()
        if not row:
            return render_error(request, "群组不存在。", "/groups")
        group = {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "avatar_url": row[3],
            "creator_id": row[4],
            "created_at": row[5],
            "invite_code": row[6]
        }

    users = []
    if q:
        with get_db_conn() as conn:
            cur = conn.cursor()
            # 搜索用户，排除已经是成员的
            cur.execute("""
                SELECT u.id, u.username, u.is_admin
                FROM users u
                WHERE u.username LIKE %s
                AND u.is_banned = 0
                AND u.id NOT IN (SELECT user_id FROM group_members WHERE group_id = %s)
                LIMIT 20
            """, (f"%{q}%", group_id))
            rows = cur.fetchall()
            for r in rows:
                users.append({"id": r[0], "username": r[1], "is_admin": r[2]})

    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("group_invite.html", {
        "request": request,
        "username": username,
        "group": group,
        "q": q,
        "users": users,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/groups/{group_id}/invite/{target_username}")
async def invite_member(group_id: int, target_username: str, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)

    if role not in ['owner', 'admin']:
        return render_error(request, "你没有邀请成员的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}/invite")

    if is_group_member(group_id, target_id):
        return render_error(request, "该用户已经是群成员。", f"/groups/{group_id}/invite")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (%s, %s, 'member', NOW())",
            (group_id, target_id)
        )
        conn.commit()

    # 发送通知给被邀请的用户
    group = get_group_by_id(group_id)
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO notifications (user_id, type, message, from_user_id, is_read, created_at) VALUES (%s, 'group_invite', %s, %s, 0, NOW())",
            (target_id, f"你被邀请加入群组「{group['name']}」", user_id)
        )
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}/invite", status_code=302)



@app.get("/groups/join", response_class=HTMLResponse)
async def join_by_code_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("group_join.html", {
        "request": request,
        "username": username,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/groups/join")
async def join_by_code(request: Request, code: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    group = get_group_by_invite_code(code.strip())
    if not group:
        return render_error(request, "邀请码无效或已过期。", "/groups/join")

    group_id = group["id"]
    if is_group_member(group_id, user_id):
        return RedirectResponse(f"/groups/{group_id}", status_code=302)

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO group_members (group_id, user_id, role, joined_at) VALUES (%s, %s, 'member', NOW())",
            (group_id, user_id)
        )
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.post("/groups/{group_id}/regenerate_invite")
async def regenerate_invite(group_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有管理邀请码的权限。", f"/groups/{group_id}")

    new_code = generate_invite_code()
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE chat_groups SET invite_code = %s WHERE id = %s", (new_code, group_id))
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}/invite", status_code=302)


@app.post("/groups/{group_id}/mute")
async def mute_member(group_id: int, target_username: str = Form(...), minutes: int = Form(60), request: Request = None, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有禁言成员的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}")

    target_role = get_group_role(group_id, target_id)
    if target_role == 'owner':
        return render_error(request, "不能禁言群主。", f"/groups/{group_id}")

    # admin 不能禁言其他 admin
    if role == 'admin' and target_role == 'admin':
        return render_error(request, "管理员不能禁言其他管理员。", f"/groups/{group_id}")

    muted_until = datetime.utcnow() + timedelta(minutes=int(minutes)) if minutes and int(minutes) > 0 else None
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE group_members SET is_muted = 1, muted_until = %s WHERE group_id = %s AND user_id = %s", (muted_until, group_id, target_id))
        conn.commit()

    # notify
    create_notification(target_id, 'group_mute', f"你在群组被禁言 {minutes} 分钟。", None, None, user_id)

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.post("/groups/{group_id}/mute/{target_username}")
async def mute_member_path(group_id: int, target_username: str, duration: int = Form(60), request: Request = None, token: str = Cookie(None)):
    # wrapper to support path-based mute forms
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有禁言成员的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}")

    target_role = get_group_role(group_id, target_id)
    if target_role == 'owner':
        return render_error(request, "不能禁言群主。", f"/groups/{group_id}")

    if role == 'admin' and target_role == 'admin':
        return render_error(request, "管理员不能禁言其他管理员。", f"/groups/{group_id}")

    minutes = int(duration) if duration else 60
    muted_until = datetime.utcnow() + timedelta(minutes=minutes)
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE group_members SET is_muted = 1, muted_until = %s WHERE group_id = %s AND user_id = %s", (muted_until, group_id, target_id))
        conn.commit()

    create_notification(target_id, 'group_mute', f"你在群组被禁言 {minutes} 分钟。", None, None, user_id)
    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.post("/groups/{group_id}/unmute/{target_username}")
async def unmute_member_path(group_id: int, target_username: str, request: Request = None, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有解除禁言的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE group_members SET is_muted = 0, muted_until = NULL WHERE group_id = %s AND user_id = %s", (group_id, target_id))
        conn.commit()

    create_notification(target_id, 'group_unmute', f"你在群组的禁言已被解除。", None, None, user_id)
    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.post("/groups/{group_id}/unmute")
async def unmute_member(group_id: int, target_username: str = Form(...), request: Request = None, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有解除禁言的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE group_members SET is_muted = 0, muted_until = NULL WHERE group_id = %s AND user_id = %s", (group_id, target_id))
        conn.commit()

    create_notification(target_id, 'group_unmute', f"你在群组的禁言已被解除。", None, None, user_id)

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.websocket("/ws/group/{group_id}")
async def group_ws(websocket: WebSocket, group_id: int):
    # accept only authenticated group members
    token = websocket.cookies.get("token") if hasattr(websocket, "cookies") else None
    token = token or websocket.query_params.get("token")
    username = get_username_from_token(token)
    if not username or is_banned(username):
        try:
            await websocket.close()
        except Exception:
            pass
        return

    user_id = get_user_id(username)
    if not is_group_member(group_id, user_id):
        try:
            await websocket.close()
        except Exception:
            pass
        return

    await group_chat_manager.connect(websocket, group_id, username)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                obj = json.loads(data)
                if isinstance(obj, dict) and obj.get("type") == "message":
                    content = (obj.get("content") or "").strip()
                else:
                    content = str(data).strip()
            except Exception:
                content = str(data).strip()

            if not content:
                continue

            if is_member_muted(group_id, user_id):
                await websocket.send_text(json.dumps({"type": "error", "message": "你已被禁言，无法发送消息。"}, ensure_ascii=False))
                continue

            with get_db_conn() as conn:
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO group_messages (group_id, sender_id, content, created_at) VALUES (%s, %s, %s, NOW())",
                    (group_id, user_id, content)
                )
                msg_id = cur.lastrowid
                conn.commit()

            payload = {
                "type": "new_message",
                "id": msg_id,
                "group_id": group_id,
                "sender_id": user_id,
                "sender_username": username,
                "content": content,
                "created_at": datetime.utcnow().isoformat()
            }
            await group_chat_manager.broadcast_to_group(group_id, payload)

    except WebSocketDisconnect:
        group_chat_manager.disconnect(websocket)
    except Exception:
        try:
            await websocket.close()
        except Exception:
            pass
        group_chat_manager.disconnect(websocket)


@app.post("/groups/{group_id}/send")
async def send_group_message(group_id: int, request: Request, content: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    if not is_group_member(group_id, user_id):
        return render_error(request, "你不是该群成员。", "/groups")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO group_messages (group_id, sender_id, content, created_at) VALUES (%s, %s, %s, NOW())",
            (group_id, user_id, content)
        )
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.post("/groups/{group_id}/leave")
async def leave_group(group_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)

    if not role:
        return render_error(request, "你不是该群成员。", "/groups")

    # Owner 不能直接退出，需要先转让
    if role == 'owner':
        return render_error(request, "群主不能直接退出，请先转让群主。", f"/groups/{group_id}")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, user_id))
        conn.commit()

    return RedirectResponse("/groups", status_code=302)


@app.post("/groups/{group_id}/kick/{target_username}")
async def kick_member(group_id: int, target_username: str, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    role = get_group_role(group_id, user_id)

    # 只有 owner 和 admin 可以踢人
    if role not in ['owner', 'admin']:
        return render_error(request, "你没有踢人的权限。", f"/groups/{group_id}")

    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/groups/{group_id}")

    target_role = get_group_role(group_id, target_id)

    # 不能踢 owner
    if target_role == 'owner':
        return render_error(request, "不能踢出群主。", f"/groups/{group_id}")

    # admin 不能踢 admin
    if role == 'admin' and target_role == 'admin':
        return render_error(request, "管理员不能踢出其他管理员。", f"/groups/{group_id}")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, target_id))
        conn.commit()

    return RedirectResponse(f"/groups/{group_id}", status_code=302)


@app.get("/groups/{group_id}", response_class=HTMLResponse)
async def group_chat_page(group_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    # 检查是否是群成员
    if not is_group_member(group_id, user_id):
        return render_error(request, "你不是该群成员。", "/groups")

    group = get_group_by_id(group_id)
    if not group:
        return render_error(request, "群组不存在。", "/groups")

    messages = get_group_messages(group_id)
    # 获取成员列表包含禁言状态
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT u.id, u.username, u.is_admin, gm.role, gm.joined_at, gm.is_muted, gm.muted_until
            FROM group_members gm
            JOIN users u ON gm.user_id = u.id
            WHERE gm.group_id = %s
            ORDER BY CASE gm.role WHEN 'owner' THEN 1 WHEN 'admin' THEN 2 ELSE 3 END, gm.joined_at ASC
        """, (group_id,))
        members = cur.fetchall()
    role = get_group_role(group_id, user_id)

    message_list = []
    for m in messages:
        message_list.append({
            "id": m[0],
            "sender_id": m[1],
            "sender_username": m[2],
            "content": m[3],
            "created_at": m[4]
        })

    member_list = []
    for m in members:
        # m: id, username, is_admin, role, joined_at, is_muted, muted_until
        is_muted = False
        try:
            is_muted_flag = m[5]
            muted_until = m[6]
        except Exception:
            is_muted_flag = 0
            muted_until = None
        # 被标记为禁言且未设置结束时间或结束时间在将来则视为仍被禁言
        is_muted = (is_muted_flag == 1) and (muted_until is None or muted_until > datetime.utcnow())
        member_list.append({
            "id": m[0],
            "username": m[1],
            "is_admin": m[2],
            "role": m[3],
            "joined_at": m[4],
            "is_muted": is_muted
        })

    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("group_chat.html", {
        "request": request,
        "username": username,
        "current_user_id": user_id,
        "group": group,
        "messages": message_list,
        "members": member_list,
        "role": role,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


# ================== 论坛（Forums）路由 ==================


@app.get("/forums", response_class=HTMLResponse)
async def forums_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    
    # 我加入的论坛
    my_forums = get_user_forums(user_id)
    
    # 公开论坛（我没加入的）
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT f.id, f.name, f.description,
                   (SELECT COUNT(*) FROM forum_members WHERE forum_id = f.id) as member_count
            FROM forums f
            WHERE f.is_approved = 1 AND f.is_private = 0
            AND f.id NOT IN (SELECT forum_id FROM forum_members WHERE user_id = %s)
            ORDER BY f.created_at DESC
        """, (user_id,))
        public_forums = cur.fetchall()
    
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    
    return templates.TemplateResponse("forums.html", {
        "request": request,
        "username": username,
        "my_forums": my_forums,
        "public_forums": public_forums,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.get("/forums/request", response_class=HTMLResponse)
async def request_forum_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)
    
    # 获取用户的申请记录
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, description, reason, status, admin_reply, created_at
            FROM forum_requests WHERE user_id = %s ORDER BY created_at DESC
        """, (user_id,))
        my_requests = cur.fetchall()
    
    return templates.TemplateResponse("forum_request.html", {
        "request": request,
        "username": username,
        "my_requests": my_requests,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/forums/request")
async def submit_forum_request(request: Request, name: str = Form(...), description: str = Form(""), reason: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO forum_requests (user_id, name, description, reason, status, created_at) VALUES (%s, %s, %s, %s, 'pending', NOW())",
            (user_id, name, description, reason)
        )
        conn.commit()
    
    return RedirectResponse("/forums/request", status_code=302)


@app.get("/forums/join", response_class=HTMLResponse)
async def join_forum_page(request: Request, code: str = Query(None), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    forum = None
    error = None
    already_member = False
    
    if code:
        forum = get_forum_by_invite_code(code)
        if not forum:
            error = "邀请码无效或论坛不存在"
        elif is_forum_member(forum["id"], user_id):
            already_member = True
    
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)
    
    return templates.TemplateResponse("forum_join.html", {
        "request": request,
        "username": username,
        "code": code,
        "forum": forum,
        "error": error,
        "already_member": already_member,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/forums/join")
async def join_forum_submit(request: Request, code: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    forum = get_forum_by_invite_code(code)
    
    if not forum:
        return render_error(request, "邀请码无效或论坛不存在。", "/forums/join")
    
    if is_forum_member(forum["id"], user_id):
        return RedirectResponse(f"/forums/{forum['id']}", status_code=302)
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO forum_members (forum_id, user_id, role, joined_at) VALUES (%s, %s, 'member', NOW())",
            (forum["id"], user_id)
        )
        conn.commit()
    
    return RedirectResponse(f"/forums/{forum['id']}", status_code=302)


@app.get("/forums/{forum_id}", response_class=HTMLResponse)
async def forum_detail_page(forum_id: int, request: Request, page: int = Query(1), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    
    # 检查权限
    if not can_view_forum(forum_id, user_id, is_admin_flag):
        return render_error(request, "你没有权限查看此论坛。", "/forums")
    
    forum = get_forum_by_id(forum_id)
    role = get_forum_role(forum_id, user_id)
    posts, total = get_forum_posts(forum_id, page)
    members = get_forum_members(forum_id)
    
    per_page = 20
    total_pages = (total + per_page - 1) // per_page
    
    post_list = []
    for p in posts:
        post_list.append({
            "id": p[0], "title": p[1], "content": p[2][:100] + "..." if p[2] and len(p[2]) > 100 else (p[2] or ""),
            "created_at": p[3], "author": p[4], "comment_count": p[5], "like_count": p[6]
        })
    
    member_list = []
    for m in members:
        member_list.append({
            "id": m[0], "username": m[1], "is_site_admin": m[2], "role": m[3], "joined_at": m[4]
        })
    
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    
    return templates.TemplateResponse("forum_detail.html", {
        "request": request,
        "username": username,
        "forum": forum,
        "posts": post_list,
        "members": member_list,
        "role": role,
        "page": page,
        "total_pages": total_pages,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.get("/forums/{forum_id}/new_post", response_class=HTMLResponse)
async def new_forum_post_page(forum_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    
    if not can_post_in_forum(forum_id, user_id, is_admin_flag):
        return render_error(request, "你没有权限在此论坛发帖。", f"/forums/{forum_id}")
    
    forum = get_forum_by_id(forum_id)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    
    return templates.TemplateResponse("forum_new_post.html", {
        "request": request,
        "username": username,
        "forum": forum,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/forums/{forum_id}/new_post")
async def create_forum_post(forum_id: int, request: Request, title: str = Form(...), content: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    
    if not can_post_in_forum(forum_id, user_id, is_admin_flag):
        return render_error(request, "你没有权限在此论坛发帖。", f"/forums/{forum_id}")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO posts (user_id, title, content, forum_id, created_at, is_deleted) VALUES (%s, %s, %s, %s, NOW(), 0)",
            (user_id, title, content, forum_id)
        )
        post_id = cur.lastrowid
        conn.commit()
    
    return RedirectResponse(f"/post/{post_id}", status_code=302)



@app.get("/forums/{forum_id}/members", response_class=HTMLResponse)
async def forum_members_page(forum_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    role = get_forum_role(forum_id, user_id)
    
    # 只有论坛管理员或超级管理员可以管理成员
    if role != 'admin' and not is_admin_flag:
        return render_error(request, "你没有权限管理成员。", f"/forums/{forum_id}")
    
    forum = get_forum_by_id(forum_id)
    members = get_forum_members(forum_id)
    
    member_list = []
    for m in members:
        member_list.append({
            "id": m[0], "username": m[1], "is_site_admin": m[2], "role": m[3], "joined_at": m[4]
        })
    
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    
    return templates.TemplateResponse("forum_members.html", {
        "request": request,
        "username": username,
        "forum": forum,
        "members": member_list,
        "role": role,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/forums/{forum_id}/kick/{target_username}")
async def kick_forum_member(forum_id: int, target_username: str, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    role = get_forum_role(forum_id, user_id)
    
    if role not in ['admin', 'moderator'] and not is_admin_flag:
        return render_error(request, "你没有权限踢人。", f"/forums/{forum_id}")
    
    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/forums/{forum_id}/members")
    
    target_role = get_forum_role(forum_id, target_id)
    
    # 不能踢论坛管理员（除非是超级管理员）
    if target_role == 'admin' and not is_admin_flag:
        return render_error(request, "不能踢出论坛管理员。", f"/forums/{forum_id}/members")
    
    # 版主不能踢版主
    if role == 'moderator' and target_role == 'moderator':
        return render_error(request, "版主不能踢出其他版主。", f"/forums/{forum_id}/members")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM forum_members WHERE forum_id = %s AND user_id = %s", (forum_id, target_id))
        conn.commit()
    
    return RedirectResponse(f"/forums/{forum_id}/members", status_code=302)


@app.post("/forums/{forum_id}/set_role/{target_username}")
async def set_member_role(forum_id: int, target_username: str, request: Request, role: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    is_admin_flag = is_admin(username)
    my_role = get_forum_role(forum_id, user_id)
    
    # 只有论坛管理员或超级管理员可以设置角色
    if my_role != 'admin' and not is_admin_flag:
        return render_error(request, "你没有权限设置角色。", f"/forums/{forum_id}/members")
    
    if role not in ['admin', 'moderator', 'member']:
        return render_error(request, "无效的角色。", f"/forums/{forum_id}/members")
    
    target_id = get_user_id(target_username)
    if not target_id:
        return render_error(request, "用户不存在。", f"/forums/{forum_id}/members")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE forum_members SET role = %s WHERE forum_id = %s AND user_id = %s", (role, forum_id, target_id))
        conn.commit()
    
    return RedirectResponse(f"/forums/{forum_id}/members", status_code=302)


@app.post("/forums/{forum_id}/leave")
async def leave_forum(forum_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    role = get_forum_role(forum_id, user_id)
    
    # 论坛管理员不能直接退出
    if role == 'admin':
        return render_error(request, "论坛管理员不能直接退出，请先转让管理员权限。", f"/forums/{forum_id}")
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM forum_members WHERE forum_id = %s AND user_id = %s", (forum_id, user_id))
        conn.commit()
    
    return RedirectResponse("/forums", status_code=302)


@app.get("/admin/forum_requests", response_class=HTMLResponse)
async def admin_forum_requests(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        return RedirectResponse("/login", status_code=302)
    
    user_id = get_user_id(username)
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT fr.id, fr.name, fr.description, fr.reason, fr.status, fr.created_at, u.username
            FROM forum_requests fr
            JOIN users u ON fr.user_id = u.id
            WHERE fr.status = 'pending'
            ORDER BY fr.created_at ASC
        """)
        pending_requests = cur.fetchall()
    
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    
    return templates.TemplateResponse("admin_forum_requests.html", {
        "request": request,
        "username": username,
        "pending_requests": pending_requests,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": True,
    })


@app.post("/admin/forum_requests/{request_id}/approve")
async def approve_forum_request(request_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        return RedirectResponse("/login", status_code=302)
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # 获取申请信息
        cur.execute("SELECT user_id, name, description FROM forum_requests WHERE id = %s", (request_id,))
        req = cur.fetchone()
        if not req:
            return render_error(request, "申请不存在。", "/admin/forum_requests")
        
        creator_id, name, description = req
        
        # 生成邀请码
        invite_code = generate_invite_code()
        
        # 创建论坛
        cur.execute(
            "INSERT INTO forums (name, description, creator_id, is_approved, is_private, invite_code, created_at) VALUES (%s, %s, %s, 1, 1, %s, NOW())",
            (name, description, creator_id, invite_code)
        )
        forum_id = cur.lastrowid
        
        # 申请者成为论坛管理员
        cur.execute(
            "INSERT INTO forum_members (forum_id, user_id, role, joined_at) VALUES (%s, %s, 'admin', NOW())",
            (forum_id, creator_id)
        )
        
        # 更新申请状态
        cur.execute("UPDATE forum_requests SET status = 'approved' WHERE id = %s", (request_id,))
        
        # 发送通知
        cur.execute(
            "INSERT INTO notifications (user_id, type, message, is_read, created_at) VALUES (%s, 'forum_approved', %s, 0, NOW())",
            (creator_id, f"恭喜！你申请的论坛「{name}」已被批准。")
        )
        
        conn.commit()
    
    return RedirectResponse("/admin/forum_requests", status_code=302)


@app.post("/admin/forum_requests/{request_id}/reject")
async def reject_forum_request(request_id: int, request: Request, reply: str = Form(""), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        return RedirectResponse("/login", status_code=302)
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        cur.execute("SELECT user_id, name FROM forum_requests WHERE id = %s", (request_id,))
        req = cur.fetchone()
        if not req:
            return render_error(request, "申请不存在。", "/admin/forum_requests")
        
        creator_id, name = req
        
        cur.execute("UPDATE forum_requests SET status = 'rejected', admin_reply = %s WHERE id = %s", (reply, request_id))
        
        # 发送通知
        message = f"你申请的论坛「{name}」未被批准。"
        if reply:
            message += f" 原因：{reply}"
        cur.execute(
            "INSERT INTO notifications (user_id, type, message, is_read, created_at) VALUES (%s, 'forum_rejected', %s, 0, NOW())",
            (creator_id, message)
        )
        
        conn.commit()
    
    return RedirectResponse("/admin/forum_requests", status_code=302)


@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query("", alias="q"),
    scope: str = Query("all", regex="^(all|title|content|author|topic)$"),
    page: int = Query(1, ge=1),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    keyword = q.strip()

    PAGE_SIZE = 10
    offset = (page - 1) * PAGE_SIZE

    results = []
    total_posts = 0

    if keyword:
        like = f"%{keyword}%"

        # 根据 scope 构造 WHERE 条件
        if scope == "title":
            where_clause = "p.title LIKE %s"
            params_like = (like,)
        elif scope == "content":
            where_clause = "p.content LIKE %s"
            params_like = (like,)
        elif scope == "author":
            where_clause = "u.username LIKE %s"
            params_like = (like,)
        elif scope == "topic":
            where_clause = "t.name IS NOT NULL AND t.name LIKE %s"
            params_like = (like,)
        else:  # all
            where_clause = """
                p.title LIKE %s
             OR p.content LIKE %s
             OR u.username LIKE %s
             OR (t.name IS NOT NULL AND t.name LIKE %s)
            """
            params_like = (like, like, like, like)

        with get_db_conn() as conn:
            cur = conn.cursor()

            # 统计总数
            cur.execute(f"""
                SELECT COUNT(*)
                FROM posts p
                JOIN users u ON p.user_id = u.id
                LEFT JOIN topics t ON p.topic_id = t.id
                WHERE p.is_deleted = 0
                  AND ( {where_clause} )
            """, params_like)
            total_posts = cur.fetchone()[0] or 0

            # 分页查询
            cur.execute(f"""
                SELECT p.id, p.title, p.content, p.created_at, u.username, t.name
                FROM posts p
                JOIN users u ON p.user_id = u.id
                LEFT JOIN topics t ON p.topic_id = t.id
                WHERE p.is_deleted = 0
                  AND ( {where_clause} )
                ORDER BY p.id DESC
                LIMIT %s OFFSET %s
            """, params_like + (PAGE_SIZE, offset))
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

    total_pages = (total_posts + PAGE_SIZE - 1) // PAGE_SIZE if keyword else 0
    has_prev = page > 1
    has_next = page < total_pages if total_pages > 0 else False

    return templates.TemplateResponse(
        "search.html",
        _inject_nav(request, {
            "request": request,
            "q": keyword,
            "scope": scope,
            "results": results,
            "page": page,
            "total_pages": total_pages,
            "has_prev": has_prev,
            "has_next": has_next,
        }),
    )


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", _inject_nav(request, {"request": request}))


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", _inject_nav(request, {"request": request}))


@app.get("/logout")
async def logout(request: Request, token: str = Cookie(None)):
    if token:
        session_token = get_session_from_token(token)
        if session_token:
            with get_db_conn() as conn:
                cur = conn.cursor()
                cur.execute("UPDATE user_sessions SET is_active = 0 WHERE session_token = %s", (session_token,))
                conn.commit()

    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("token")
    return resp


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        # 查一下当前用户信息（这里先只要 is_admin / is_banned）
        cur.execute("SELECT id, is_admin, is_banned FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            resp = RedirectResponse("/login", status_code=302)
            resp.delete_cookie("token")
            return resp

        user_id, user_is_admin, user_is_banned = row

    return templates.TemplateResponse(
        "settings.html",
        _inject_nav(request, {
            "request": request,
            "user_id": user_id,
            "is_banned": bool(user_is_banned),
            "is_admin": bool(user_is_admin),
        }),
    )


@app.get("/settings/sessions", response_class=HTMLResponse)
async def sessions_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    current_session = get_session_from_token(token)

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, session_token, device_info, ip_address, created_at, last_active FROM user_sessions WHERE user_id = %s AND is_active = 1 ORDER BY last_active DESC",
            (user_id,)
        )
        sessions = cur.fetchall()

    session_list = []
    for s in sessions:
        session_list.append({
            "id": s[0],
            "session_token": s[1],
            "device_info": s[2],
            "ip_address": s[3],
            "created_at": s[4],
            "last_active": s[5],
            "is_current": s[1] == current_session
        })

    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse("sessions.html", {
        "request": request,
        "username": username,
        "sessions": session_list,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/settings/sessions/{session_id}/revoke")
async def revoke_session(session_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM user_sessions WHERE id = %s", (session_id,))
        row = cur.fetchone()

    if not row or row[0] != user_id:
        return render_error(request, "无权操作。", "/settings/sessions")

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE user_sessions SET is_active = 0 WHERE id = %s", (session_id,))
        conn.commit()

    return RedirectResponse("/settings/sessions", status_code=302)


@app.post("/settings/sessions/revoke_all")
async def revoke_all_sessions(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    current_session = get_session_from_token(token)

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE user_sessions SET is_active = 0 WHERE user_id = %s AND session_token != %s",
            (user_id, current_session)
        )
        conn.commit()

    return RedirectResponse("/settings/sessions", status_code=302)


@app.post("/settings/password")
async def change_password(
    request: Request,
    old_password: str = Form(),
    new_password: str = Form(),
    new_password2: str = Form(),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    with get_db_conn() as conn:
        cur = conn.cursor()

        # 取出当前用户的 hash 密码
        cur.execute("SELECT password FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            resp = RedirectResponse("/login", status_code=302)
            resp.delete_cookie("token")
            return resp

        hashed = row[0]

        # 验证旧密码
        if not pwd_context.verify(old_password, hashed):
            return render_error(request, "旧密码错误。", back_url="/settings", status_code=400)

        # 检查新密码一致
        if new_password != new_password2:
            return render_error(request, "两次输入的新密码不一致。", back_url="/settings", status_code=400)

        if len(new_password) < 6:
            return render_error(request, "新密码太短，至少 6 位。", back_url="/settings", status_code=400)

        # 更新密码
        new_hashed = pwd_context.hash(new_password)
        cur.execute("UPDATE users SET password = %s WHERE username = %s", (new_hashed, username))
        conn.commit()

    return render_error(request, "密码修改成功，下次请使用新密码登录。", back_url="/", status_code=200)


@app.get("/settings/delete_account", response_class=HTMLResponse)
async def delete_account_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    user_id = get_user_id(username)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)
    
    return templates.TemplateResponse("delete_account.html", {
        "request": request,
        "username": username,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    })


@app.post("/settings/delete_account")
async def delete_account_submit(request: Request, password: str = Form(...), confirm: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    # 确认输入
    if confirm != "DELETE":
        return render_error(request, "请输入 DELETE 确认删除。", "/settings/delete_account")
    
    # 验证密码
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, password FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
    
    if not row or not pwd_context.verify(password, row[1]):
        return render_error(request, "密码错误。", "/settings/delete_account")
    
    user_id = row[0]
    
    # 删除用户相关数据
    with get_db_conn() as conn:
        cur = conn.cursor()
        
        # 删除用户的帖子（软删除）
        cur.execute("UPDATE posts SET is_deleted = 1 WHERE user_id = %s", (user_id,))
        
        # 删除用户的评论（软删除）
        cur.execute("UPDATE comments SET is_deleted = 1 WHERE user_id = %s", (user_id,))
        
        # 删除点赞
        cur.execute("DELETE FROM post_likes WHERE user_id = %s", (user_id,))
        
        # 删除通知
        cur.execute("DELETE FROM notifications WHERE user_id = %s", (user_id,))
        
        # 删除私信
        cur.execute("DELETE FROM private_messages WHERE sender_id = %s OR receiver_id = %s", (user_id, user_id))
        
        # 删除会话
        cur.execute("DELETE FROM user_sessions WHERE user_id = %s", (user_id,))
        
        # 退出所有论坛
        cur.execute("DELETE FROM forum_members WHERE user_id = %s", (user_id,))
        
        # 退出所有群组
        cur.execute("DELETE FROM group_members WHERE user_id = %s", (user_id,))
        
        # 最后删除用户
        cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
        
        conn.commit()
    
    # 清除 cookie 并跳转
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("token")
    return resp


@app.get("/user/{username}", response_class=HTMLResponse)
async def user_profile(username: str, request: Request, token: str = Cookie(None)):
    current_username = get_username_from_token(token)
    with get_db_conn() as conn:
        cur = conn.cursor()

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
        _inject_nav(request, {
            "request": request,
            "profile_username": username,
            "profile_user_id": user_id,
            "profile_is_admin": bool(user_is_admin),
            "profile_is_banned": bool(user_is_banned),
            "posts": posts_with_likes,
            "current_is_admin": current_is_admin,
        }),
    )


@app.get("/user/{username}/comments", response_class=HTMLResponse)
async def user_comments(username: str, request: Request, token: str = Cookie(None)):
    current_username = get_username_from_token(token)
    with get_db_conn() as conn:
        cur = conn.cursor()

        # 查这个用户是否存在
        cur.execute("SELECT id, is_admin, is_banned FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            return HTMLResponse("用户不存在。<a href='/'>返回首页</a>", status_code=404)

        user_id, user_is_admin, user_is_banned = row

        # 查这个用户的评论（帖子未删、评论未删）
        cur.execute("""
            SELECT
                c.id,              -- 0 评论ID
                c.content,         -- 1 评论内容
                c.created_at,      -- 2 评论时间
                p.id AS post_id,   -- 3 所属帖子ID
                p.title AS post_title,  -- 4 帖子标题
                t.id AS topic_id,       -- 5 板块ID（可能为 NULL）
                t.name AS topic_name    -- 6 板块名称（可能为 NULL）
            FROM comments c
            JOIN posts p ON c.post_id = p.id
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE c.user_id = %s
            AND c.is_deleted = 0
            AND p.is_deleted = 0
            ORDER BY c.created_at DESC
            LIMIT 100
        """, (user_id,))
        comments = cur.fetchall()

        current_is_admin = is_admin(current_username) if current_username else False

        # 未读通知数（用于导航）
        unread_count = 0
        if current_username:
            cu_id = get_user_id(current_username)
            if cu_id:
                unread_count = get_unread_notifications_count(cu_id)

    return templates.TemplateResponse(
        "user_comments.html",
        _inject_nav(request, {
            "request": request,
            "profile_username": username,
            "profile_user_id": user_id,
            "profile_is_admin": bool(user_is_admin),
            "profile_is_banned": bool(user_is_banned),
            "comments": comments,
            "current_is_admin": current_is_admin,
            "unread_count": unread_count,
        }),
    )


@app.get("/my/comments")
async def my_comments_redirect(token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    return RedirectResponse(f"/user/{username}/comments", status_code=302)


@app.get("/user/{username}/likes", response_class=HTMLResponse)
async def user_likes(username: str, request: Request, token: str = Cookie(None)):
    current_username = get_username_from_token(token)
    with get_db_conn() as conn: 
        cur = conn.cursor()

        # 查这个用户是否存在
        cur.execute("SELECT id, is_admin, is_banned FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        if not row:
            return HTMLResponse("用户不存在。<a href='/'>返回首页</a>", status_code=404)

        user_id, user_is_admin, user_is_banned = row

        # 查这个用户点赞过的帖子（只显示未删除帖子）
        cur.execute("""
            SELECT
                p.id AS post_id,          -- 0
                p.title,                  -- 1
                p.created_at,             -- 2
                u.username AS author,     -- 3
                t.id AS topic_id,         -- 4
                t.name AS topic_name,     -- 5
                pl.created_at AS liked_at -- 6 点赞时间
            FROM post_likes pl
            JOIN posts p ON pl.post_id = p.id
            JOIN users u ON p.user_id = u.id
            LEFT JOIN topics t ON p.topic_id = t.id
            WHERE pl.user_id = %s
            AND p.is_deleted = 0
            ORDER BY pl.created_at DESC
            LIMIT 100
        """, (user_id,))
        likes = cur.fetchall()

    current_is_admin = is_admin(current_username) if current_username else False

    unread_count = 0
    if current_username:
        cu_id = get_user_id(current_username)
        if cu_id:
            unread_count = get_unread_notifications_count(cu_id)

    return templates.TemplateResponse(
        "user_likes.html",
        _inject_nav(request, {
            "request": request,
            "profile_username": username,
            "profile_user_id": user_id,
            "profile_is_admin": bool(user_is_admin),
            "profile_is_banned": bool(user_is_banned),
            "likes": likes,
            "current_is_admin": current_is_admin,
            "unread_count": unread_count,
        }),
    )


@app.get("/my/likes")
async def my_likes_redirect(token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    return RedirectResponse(f"/user/{username}/likes", status_code=302)


@app.get("/notifications", response_class=HTMLResponse)
async def notifications_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

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
        _inject_nav(request, {
            "request": request,
            "notifications": rows,
            "unread_count": unread_count,
        }),
    )


@app.post("/notifications/read_all")
async def notifications_read_all(token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE notifications SET is_read = 1 WHERE user_id = %s AND is_read = 0", (user_id,))
        conn.commit()
    return RedirectResponse("/notifications", status_code=302)


# ================== 私信路由：聊天列表 / 新对话 / 聊天页 / 发送消息 ==================


@app.get("/messages", response_class=HTMLResponse)
async def messages_list(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    convs = get_conversations(user_id)
    unread_msgs_total = get_unread_messages_count(user_id)
    unread_notifs = get_unread_notifications_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse(
        "message_list.html",
        _inject_nav(request, {
            "request": request,
            "conversations": convs,
        }),
    )


@app.get("/messages/new", response_class=HTMLResponse)
async def new_message_page(request: Request, q: Optional[str] = Query(None), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    unread_msgs_total = get_unread_messages_count(user_id)
    unread_notifs = get_unread_notifications_count(user_id)
    is_admin_flag = is_admin(username)

    users = []
    if q:
        with get_db_conn() as conn:
            cur = conn.cursor()
            like_pattern = f"%{q}%"
            cur.execute(
                "SELECT id, username, is_admin FROM users WHERE username LIKE %s AND username != %s AND is_banned = 0 LIMIT 20",
                (like_pattern, username),
            )
            rows = cur.fetchall()
        for r in rows:
            uid, uname, is_admin_flag_row = r
            users.append({"id": uid, "username": uname, "is_admin": bool(is_admin_flag_row)})

    return templates.TemplateResponse(
        "new_chat.html",
        _inject_nav(request, {
            "request": request,
            "q": q,
            "users": users,
        }),
    )


@app.post("/messages/new", response_class=HTMLResponse)
async def new_message_submit(request: Request, target_username: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    if target_username == username:
        return render_error(request, "不能和自己聊天。", back_url="/messages/new", status_code=400)

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s AND is_banned = 0", (target_username,))
        row = cur.fetchone()
        if not row:
            return render_error(request, "对方用户不存在。", back_url="/messages/new", status_code=404)

    return RedirectResponse(f"/messages/{target_username}", status_code=302)


@app.get("/messages/{other_username}", response_class=HTMLResponse)
async def chat_page(other_username: str, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (other_username,))
        row = cur.fetchone()
        if not row:
            return render_error(request, "对方用户不存在。", back_url="/messages", status_code=404)
        other_id = row[0]

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE private_messages SET is_read = 1 WHERE receiver_id = %s AND sender_id = %s AND is_read = 0",
            (user_id, other_id),
        )
        conn.commit()

    msgs = get_messages(user_id, other_id)
    unread_msgs_total = get_unread_messages_count(user_id)
    unread_notifs = get_unread_notifications_count(user_id)
    is_admin_flag = is_admin(username)

    return templates.TemplateResponse(
        "chat.html",
        _inject_nav(request, {
            "request": request,
            "other_username": other_username,
            "current_user_id": user_id,
            "messages": msgs,
        }),
    )


@app.get("/report/user/{target_username}", response_class=HTMLResponse)
async def report_user_page(target_username: str, request: Request, token: str = Cookie(None)):
    # 1. 验证登录
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    # 2. 不能举报自己
    if username == target_username:
        return render_error(request, "不能举报自己。", "/messages")

    # 3. 检查目标用户存在
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, is_admin FROM users WHERE username = %s", (target_username,))
        row = cur.fetchone()

    if not row:
        return render_error(request, "用户不存在。", "/messages")

    # 4. 不能举报管理员
    if row[1]:
        return render_error(request, "不能举报管理员。", "/messages")

    # 5. 获取 navbar 需要的变量
    user_id = get_user_id(username)
    unread_count = get_unread_notifications_count(user_id)
    message_unread_count = get_unread_messages_count(user_id)
    is_admin_flag = is_admin(username)

    # 6. 渲染模板
    return templates.TemplateResponse("report_user.html", _inject_nav(request, {
        "request": request,
        "username": username,
        "target_username": target_username,
        "unread_count": unread_count,
        "message_unread_count": message_unread_count,
        "is_admin": is_admin_flag,
    }))


@app.post("/report/user/{target_username}", response_class=HTMLResponse)
async def report_user_submit(target_username: str, request: Request, reason: str = Form(...), token: str = Cookie(None)):
    # 1. 验证登录
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    # 2. 不能举报自己
    if username == target_username:
        return render_error(request, "不能举报自己。", "/messages")

    # 3. 获取举报者 ID
    reporter_id = get_user_id(username)
    if not reporter_id:
        return render_error(request, "用户不存在。", "/messages")

    # 4. 检查目标用户存在且不是管理员
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, is_admin FROM users WHERE username = %s", (target_username,))
        row = cur.fetchone()

    if not row:
        return render_error(request, "用户不存在。", "/messages")

    if row[1]:
        return render_error(request, "不能举报管理员。", "/messages")

    target_id = row[0]

    # 5. 检查是否已经举报过（避免重复举报）
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM reports WHERE type = 'user' AND target_id = %s AND reporter_id = %s AND status = 'pending'",
            (target_id, reporter_id)
        )
        existing = cur.fetchone()

    if existing:
        return render_error(request, "你已经举报过该用户，请等待处理。", f"/messages/{target_username}")

    # 6. 插入举报记录
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO reports (type, target_id, reporter_id, reason, status, created_at) VALUES ('user', %s, %s, %s, 'pending', NOW())",
            (target_id, reporter_id, reason)
        )
        conn.commit()

    # 7. 重定向回聊天页面
    return RedirectResponse(f"/messages/{target_username}", status_code=302)


@app.post("/messages/{other_username}/send")
async def send_message(request: Request, other_username: str, content: str = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    sender_id = get_user_id(username)
    if not sender_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (other_username,))
        row = cur.fetchone()
        if not row:
            return render_error(request, "接收者不存在。", back_url="/messages", status_code=404)
        receiver_id = row[0]

        now = datetime.utcnow()
        cur.execute(
            "INSERT INTO private_messages (sender_id, receiver_id, content, is_read, created_at) VALUES (%s, %s, %s, 0, %s)",
            (sender_id, receiver_id, content, now),
        )
        conn.commit()

    message_obj = {
        "type": "new_message",
        "from": username,
        "to": other_username,
        "content": content,
        "created_at": now.strftime("%Y-%m-%d %H:%M"),
    }
    try:
        await chat_manager.broadcast_to_users([username, other_username], message_obj)
    except Exception:
        pass

    return RedirectResponse(f"/messages/{other_username}", status_code=302)



# ================== WebSocket：评论实时 + 在线人数 + 正在输入 ==================


@app.websocket("/ws/comments/{post_id}")
async def comments_websocket(websocket: WebSocket, post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
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


@app.websocket("/ws/chat/{other_username}")
async def chat_websocket(websocket: WebSocket, other_username: str, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        await websocket.close()
        return

    # verify other exists (optional)
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (other_username,))
        row = cur.fetchone()
        if not row:
            await websocket.close()
            return

    await chat_manager.connect(websocket, username)

    try:
        while True:
            data_text = await websocket.receive_text()
            try:
                data = json.loads(data_text)
            except Exception:
                continue

            # expected: {"type":"msg","content":"...","to":"other_username"}
            if data.get("type") == "msg":
                content = data.get("content", "").strip()
                to_user = data.get("to") or other_username
                if not content:
                    continue

                sender = username

                # insert into DB
                sender_id = get_user_id(sender)
                receiver_id = None
                with get_db_conn() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT id FROM users WHERE username = %s", (to_user,))
                    r = cur.fetchone()
                    if not r:
                        continue
                    receiver_id = r[0]
                    now = datetime.utcnow()
                    cur.execute(
                        "INSERT INTO private_messages (sender_id, receiver_id, content, is_read, created_at) VALUES (%s, %s, %s, 0, %s)",
                        (sender_id, receiver_id, content, now),
                    )
                    conn.commit()

                msg_obj = {
                    "type": "new_message",
                    "from": sender,
                    "to": to_user,
                    "content": content,
                    "created_at": now.strftime("%Y-%m-%d %H:%M"),
                }
                try:
                    await chat_manager.broadcast_to_users([sender, to_user], msg_obj)
                except Exception:
                    pass
    except WebSocketDisconnect:
        chat_manager.disconnect(websocket)


# ================== 管理员路由：删帖 / 删评 / 用户管理 / 板块管理 / 举报管理 ==================


@app.post("/admin/post/{post_id}/delete")
async def admin_delete_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp
    
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE posts SET is_deleted = 1 WHERE id = %s", (post_id,))
        conn.commit()
    return RedirectResponse("/", status_code=302)


@app.post("/admin/comment/{comment_id}/delete")
async def admin_delete_comment(comment_id: int, post_id: int = Form(...), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE comments SET is_deleted = 1 WHERE id = %s", (comment_id,))
        conn.commit()
    return RedirectResponse(f"/post/{post_id}", status_code=302)


@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, username, is_admin, is_banned FROM users ORDER BY id ASC")
        users = cur.fetchall()

    return templates.TemplateResponse(
        "admin_users.html",
        _inject_nav(request, {"request": request, "users": users}),
    )


@app.post("/admin/user/{user_id}/ban")
async def admin_ban_user(user_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

            # 先查出这个用户的信息
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
        # If we've just banned the user, close any active websockets for them
        if new_flag == 1:
            try:
                await comment_manager.close_by_username(target_username)
            except Exception:
                pass
            try:
                await chat_manager.close_by_username(target_username)
            except Exception:
                pass
    return RedirectResponse("/admin/users", status_code=302)


@app.get("/admin/topics", response_class=HTMLResponse)
async def admin_topics(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT t.id, t.name, t.description, t.is_approved, t.created_at, u.username
            FROM topics t
            LEFT JOIN users u ON t.created_by = u.id
            ORDER BY t.created_at DESC
        """)
        topics = cur.fetchall()

    return templates.TemplateResponse(
        "admin_topics.html",
        _inject_nav(request, {"request": request, "topics": topics}),
    )


@app.post("/admin/topic/{topic_id}/approve")
async def admin_approve_topic(topic_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 先查出这个 topic 的信息
        cur.execute("SELECT name, created_by FROM topics WHERE id = %s", (topic_id,))
        row = cur.fetchone()
        if not row:
            return RedirectResponse("/admin/topics", status_code=302)

        topic_name, created_by = row

        # 标记为通过
        cur.execute("UPDATE topics SET is_approved = 1 WHERE id = %s", (topic_id,))
        conn.commit()

    # 给创建者发送通知（如果有创建者）
    if created_by:
        msg = f"你申请的板块《{topic_name}》已通过审核"
        create_notification(
            user_id=created_by,
            type_="topic_approved",
            message=msg,
            # 可以不关联 post/comment
            from_user_id=get_user_id(username),  # 管理员 ID
        )

    return RedirectResponse("/admin/topics", status_code=302)


@app.post("/admin/topic/{topic_id}/delete")
async def admin_delete_topic(topic_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM topics WHERE id = %s", (topic_id,))
        conn.commit()
    return RedirectResponse("/admin/topics", status_code=302)


@app.get("/admin/reports", response_class=HTMLResponse)
async def admin_reports(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn: 
        cur = conn.cursor()

        # 查所有未处理的举报，并在 type='user' 时 JOIN 出被举报用户的用户名
        cur.execute("""
            SELECT 
                r.id, r.type, r.target_id, r.reason, r.status, r.created_at,
                reporter.username as reporter_username,
                CASE 
                    WHEN r.type = 'user' THEN target_user.username
                    ELSE NULL
                END as target_username
            FROM reports r
            LEFT JOIN users reporter ON r.reporter_id = reporter.id
            LEFT JOIN users target_user ON r.type = 'user' AND r.target_id = target_user.id
            WHERE r.status = 'pending'
            ORDER BY r.created_at DESC
        """)
        reports = cur.fetchall()

    return templates.TemplateResponse(
        "admin_reports.html",
        _inject_nav(request, {"request": request, "reports": reports}),
    )


@app.post("/admin/report/{report_id}/handle")
async def admin_handle_report(report_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 先查出举报详情
        cur.execute("""
            SELECT type, target_id, reporter_id
            FROM reports
            WHERE id = %s
        """, (report_id,))
        row = cur.fetchone()
        if not row:
            return RedirectResponse("/admin/reports", status_code=302)

        report_type, target_id, reporter_id = row

        # 标记已处理
        cur.execute("UPDATE reports SET status = 'handled' WHERE id = %s", (report_id,))
        conn.commit()

    # 给举报人发通知
    # 拼一个简短说明
    if report_type == "post":
        target_label = f"帖子 ID {target_id}"
    else:
        target_label = f"评论 ID {target_id}"

    msg = f"你对 {target_label} 的举报已由管理员处理"

    create_notification(
        user_id=reporter_id,
        type_="report_handled",
        message=msg,
        # 可选：粗暴地把 related_post_id 留空，因为我们只有 ID
        from_user_id=get_user_id(username),  # 管理员 ID
    )

    return RedirectResponse("/admin/reports", status_code=302)


@app.get("/admin/messages/{user1}/{user2}", response_class=HTMLResponse)
async def admin_view_chat(user1: str, user2: str, request: Request, token: str = Cookie(None)):
    # 1. 验证登录
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    # 2. 验证是管理员
    if not is_admin(username):
        return render_error(request, "无权限访问。", "/")

    # 3. 获取两个用户的 ID
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE username = %s", (user1,))
        row1 = cur.fetchone()
        cur.execute("SELECT id FROM users WHERE username = %s", (user2,))
        row2 = cur.fetchone()

    if not row1 or not row2:
        return render_error(request, "用户不存在。", "/admin/reports")

    user1_id = row1[0]
    user2_id = row2[0]

    # 4. 获取两人之间的所有聊天记录
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT pm.id, pm.sender_id, pm.receiver_id, pm.content, pm.created_at,
                   sender.username as sender_username,
                   receiver.username as receiver_username
            FROM private_messages pm
            JOIN users sender ON pm.sender_id = sender.id
            JOIN users receiver ON pm.receiver_id = receiver.id
            WHERE (pm.sender_id = %s AND pm.receiver_id = %s)
               OR (pm.sender_id = %s AND pm.receiver_id = %s)
            ORDER BY pm.created_at ASC
        """, (user1_id, user2_id, user2_id, user1_id))
        messages = cur.fetchall()

    # 5. 转换为字典列表
    message_list = []
    for m in messages:
        message_list.append({
            "id": m[0],
            "sender_id": m[1],
            "receiver_id": m[2],
            "content": m[3],
            "created_at": m[4],
            "sender_username": m[5],
            "receiver_username": m[6],
        })

    # 6. 获取 navbar 变量 (use _inject_nav)
    ctx = _nav_context_from_request(request)
    ctx.update({
        "request": request,
        "user1": user1,
        "user2": user2,
        "messages": message_list,
    })

    return templates.TemplateResponse("admin_view_chat.html", ctx)


@app.get("/admin/inbox", response_class=HTMLResponse)
async def admin_inbox(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username) or not is_admin(username):
        resp = RedirectResponse("/", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn: 
        cur = conn.cursor()

        # 统计未处理的板块申请和举报数量
        cur.execute("SELECT COUNT(*) FROM topics WHERE is_approved = 0")
        pending_topics = cur.fetchone()[0] or 0

        cur.execute("SELECT COUNT(*) FROM reports WHERE status = 'pending'")
        pending_reports = cur.fetchone()[0] or 0

    return templates.TemplateResponse(
        "admin_inbox.html",
        _inject_nav(request, {
            "request": request,
            "pending_topics": pending_topics,
            "pending_reports": pending_reports,
        }),
    )


# ================== 登录 / 注册 ==================


@app.post("/register")
async def register(request: Request, username: str = Form(), password: str = Form()):
    if len(password) > 50:
        return render_error(request, "密码太长啦，请不要超过 50 个字符。", back_url="/register", status_code=400)

    hashed = pwd_context.hash(password)
    try:
        with get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO users (username, password) VALUES (%s, %s)",
                (username, hashed),
            )
            conn.commit()
        return RedirectResponse("/login", status_code=302)
    except pymysql.err.IntegrityError:
        return render_error(request, "用户名已存在，请换一个。", back_url="/register", status_code=400)


@app.post("/login")
async def login(request: Request, username: str = Form(), password: str = Form()):
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT password, is_banned FROM users WHERE username = %s", (username,))
        row = cur.fetchone()

    if not row or not pwd_context.verify(password, row[0]):
        return render_error(request, "用户不存在或密码错误。", back_url="/login", status_code=400)

    if row[1] == 1:
        return render_error(request, "此账号已被封禁，请联系管理员。", back_url="/", status_code=403)

    # 成功登录，创建 session_token 并保存到 user_sessions
    session_token = str(uuid.uuid4())
    user_agent = request.headers.get("user-agent", "Unknown")
    client_ip = request.client.host if request.client else "0.0.0.0"

    user_id = get_user_id(username)
    # 获取旧的活跃会话
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, device_info, ip_address, created_at FROM user_sessions WHERE user_id = %s AND is_active = 1",
            (user_id,)
        )
        old_sessions = cur.fetchall()
        
    print(f"DEBUG: user_id={user_id}, old_sessions={old_sessions}")
    # 如果有旧会话，发送通知（站内通知）
    if old_sessions:
        device_short = user_agent[:50] + "..." if len(user_agent) > 50 else user_agent
        message = f"新设备登录提醒：{device_short} (IP: {client_ip}) 刚刚登录了你的账号"
        now = datetime.utcnow()
        with get_db_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO notifications (user_id, type, message, is_read, created_at) VALUES (%s, %s, %s, %s, %s)",
                (user_id, 'new_login', message, 0, now)
            )
            conn.commit()

    # 插入新 session
    now = datetime.utcnow()
    with get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO user_sessions (user_id, session_token, device_info, ip_address, is_active, created_at, last_active) VALUES (%s, %s, %s, %s, 1, %s, %s)",
            (user_id, session_token, user_agent, client_ip, now, now)
        )
        conn.commit()

    token = create_token_with_session(username, session_token)
    resp = RedirectResponse("/", status_code=302)
    resp.set_cookie("token", token, httponly=True, max_age=7*24*60*60)
    return resp


# ================== 发帖 / 板块创建 ==================


@app.get("/post/new", response_class=HTMLResponse)
async def new_post_page(request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 取出所有已批准的板块
        cur.execute("""
            SELECT id, name
            FROM topics
            WHERE is_approved = 1
            ORDER BY name ASC
        """)
        topics = cur.fetchall()

    return templates.TemplateResponse(
        "new_post.html",
        _inject_nav(request, {"request": request, "topics": topics}),
    )


@app.post("/post/new")
async def new_post(
    title: str = Form(),
    content: str = Form(),
    topic_id: Optional[int] = Form(None),
    token: str = Cookie(None),
):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

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
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp
    return templates.TemplateResponse(
        "new_topic.html",
        _inject_nav(request, {"request": request}),
    )


@app.post("/topic/new")
async def new_topic(name: str = Form(), description: str = Form(""), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    name = name.strip()
    if not name:
        return HTMLResponse("板块名称不能为空。<a href='/topic/new'>返回</a>", status_code=400)

    with get_db_conn() as conn:
        cur = conn.cursor()
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
async def topic_page(
    topic_id: int,
    request: Request,
    token: str = Cookie(None),
    page: int = Query(1, ge=1),
):
    username = get_username_from_token(token)
    PAGE_SIZE = 10
    offset = (page - 1) * PAGE_SIZE

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 查这个 topic
        cur.execute("SELECT id, name FROM topics WHERE id = %s AND is_approved = 1", (topic_id,))
        topic = cur.fetchone()
        if not topic:
            return HTMLResponse("板块不存在或未通过审核。<a href='/'>返回首页</a>", status_code=404)

        # 统计该板块下帖子总数
        cur.execute("""
            SELECT COUNT(*)
            FROM posts
            WHERE is_deleted = 0 AND topic_id = %s
        """, (topic_id,))
        total_posts = cur.fetchone()[0] or 0

        # 查属于这个 topic 的帖子（分页）
        cur.execute("""
            SELECT p.id, p.title, p.content, p.created_at, u.username
            FROM posts p
            JOIN users u ON p.user_id = u.id
            WHERE p.is_deleted = 0 AND p.topic_id = %s
            ORDER BY p.id DESC
            LIMIT %s OFFSET %s
        """, (topic_id, PAGE_SIZE, offset))
        posts = cur.fetchall()

    admin_flag = is_admin(username) if username else False

    total_pages = (total_posts + PAGE_SIZE - 1) // PAGE_SIZE
    has_prev = page > 1
    has_next = page < total_pages if total_pages > 0 else False

    return templates.TemplateResponse(
        "topic_posts.html",
        _inject_nav(request, {
            "request": request,
            "topic": topic,
            "posts": posts,
            "page": page,
            "total_pages": total_pages,
            "has_prev": has_prev,
            "has_next": has_next,
        }),
    )


# ================== 帖子详情 + 评论 ==================


@app.get("/post/{post_id}", response_class=HTMLResponse)
async def post_detail(post_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 查询帖子
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

        # 查询评论
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
    current_user_id = None
    if username:
        current_user_id = get_user_id(username)
        if current_user_id:
            user_like_flag = user_liked_post(current_user_id, post_id)

    is_author = bool(current_user_id and current_user_id == post["user_id"])

    unread_count = 0
    if username and current_user_id:
        unread_count = get_unread_notifications_count(current_user_id)

    return templates.TemplateResponse(
        "post_detail.html",
        _inject_nav(request, {
            "request": request,
            "post": post,
            "comments": comments,
            "likes_count": likes_count,
            "user_liked": user_like_flag,
            "is_author": is_author,
            "unread_count": unread_count,
        }),
    )


@app.post("/post/{post_id}/like")
async def like_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    now = datetime.utcnow()

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 检查帖子存在且未删除
        cur.execute("SELECT id, user_id, title FROM posts WHERE id = %s AND is_deleted = 0", (post_id,))
        row = cur.fetchone()
        if not row:
            return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

        post_id_db, post_owner_id, post_title = row

        if user_liked_post(user_id, post_id):
            cur.execute("DELETE FROM post_likes WHERE post_id = %s AND user_id = %s", (post_id, user_id))
        else:
            cur.execute(
                "INSERT INTO post_likes (post_id, user_id, created_at) VALUES (%s, %s, %s)",
                (post_id, user_id, now),
            )
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

    return RedirectResponse(f"/post/{post_id}", status_code=302)


@app.post("/api/post/{post_id}/like")
async def api_like_post(post_id: int, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = JSONResponse({"error": "not_logged_in"}, status_code=401)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        return JSONResponse({"error": "no_user"}, status_code=401)
    
    with get_db_conn() as conn:
        cur = conn.cursor()

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
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    reporter_id = get_user_id(username)
    if not reporter_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    now = datetime.utcnow()

    with get_db_conn() as conn:
        cur = conn.cursor()

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
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    reporter_id = get_user_id(username)
    if not reporter_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    now = datetime.utcnow()

    with get_db_conn() as conn:
        cur = conn.cursor()

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
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    user_id = get_user_id(username)
    if not user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
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


@app.post("/comment/{comment_id}/delete")
async def delete_comment(comment_id: int, post_id: int = Form(...), token: str = Cookie(None)):
    """
    允许评论作者本人或管理员软删除评论（is_deleted=1），删除后回到帖子详情。
    """
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    current_user_id = get_user_id(username)
    if not current_user_id:
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()
        # 查评论信息
        cur.execute("SELECT user_id, is_deleted FROM comments WHERE id = %s", (comment_id,))
        row = cur.fetchone()
        if not row or row[1] == 1:
            return HTMLResponse("评论不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

        comment_user_id, _ = row

        # 权限：作者本人或管理员
        if current_user_id != comment_user_id and not is_admin(username):
            return HTMLResponse("你没有权限删除这条评论。<a href='/'>返回首页</a>", status_code=403)

        # 软删除
        cur.execute("UPDATE comments SET is_deleted = 1 WHERE id = %s", (comment_id,))
        conn.commit()

    return RedirectResponse(f"/post/{post_id}", status_code=302)


# ================== 帖子编辑 ==================


@app.get("/post/{post_id}/edit", response_class=HTMLResponse)
async def edit_post_page(post_id: int, request: Request, token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

            # 查帖子信息
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
        _inject_nav(request, {"request": request, "post": post}),
    )


@app.post("/post/{post_id}/edit")
async def edit_post(post_id: int, title: str = Form(), content: str = Form(), token: str = Cookie(None)):
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 查帖子信息
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


@app.post("/post/{post_id}/delete")
async def delete_post(post_id: int, token: str = Cookie(None)):
    """
    允许帖子作者本人或管理员删除帖子（软删除：is_deleted=1）。
    删除后重定向回首页。
    """
    username = get_username_from_token(token)
    if not username or is_banned(username):
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("token")
        return resp

    with get_db_conn() as conn:
        cur = conn.cursor()

        # 查帖子信息
        cur.execute("SELECT user_id, is_deleted FROM posts WHERE id = %s", (post_id,))
        row = cur.fetchone()
        if not row or row[1] == 1:
            return HTMLResponse("帖子不存在或已被删除。<a href='/'>返回首页</a>", status_code=404)

        post_user_id, _ = row
        current_user_id = get_user_id(username)
        if not current_user_id:
            resp = RedirectResponse("/login", status_code=302)
            resp.delete_cookie("token")
            return resp

        # 权限：作者本人或管理员
        if current_user_id != post_user_id and not is_admin(username):
            return HTMLResponse("你没有权限删除这个帖子。<a href='/'>返回首页</a>", status_code=403)

        # 软删除
        cur.execute("UPDATE posts SET is_deleted = 1 WHERE id = %s", (post_id,))
        conn.commit()

    return RedirectResponse("/", status_code=302)



