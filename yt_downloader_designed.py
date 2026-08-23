"""
YouTube Downloader Platform - Production Ready Single File Application
No FFmpeg required, all in one file.
"""

import os
import re
import json
import uuid
import time
import shutil
import logging
import hashlib
import secrets
import threading
import asyncio
import smtplib
import random
import string
import sys
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from contextlib import asynccontextmanager

# Third-party imports
import yt_dlp
import yt_dlp.version
import httpx
from fastapi import FastAPI, Request, Response, Depends, HTTPException, Query, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from sqlalchemy import create_engine, Column, String, Integer, BigInteger, DateTime, Text, Boolean, ForeignKey, Float, func
from sqlalchemy.orm import declarative_base, sessionmaker, Session, relationship
from sqlalchemy.exc import IntegrityError
from itsdangerous import URLSafeTimedSerializer
from dotenv import load_dotenv
import uvicorn
import bcrypt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ------------------------------
# Embedded VidsSave provider (merged from vidsave_provider.py)
# ------------------------------
logger = logging.getLogger(__name__)

# Environment variables for VidsSave
VIDSSAVE_AUTH = os.getenv("VIDSSAVE_AUTH", "20250901majwlqo")
VIDSSAVE_DOMAIN = os.getenv("VIDSSAVE_DOMAIN", "api-ak.vidssave.com")
VIDSSAVE_ENABLED = bool(VIDSSAVE_AUTH and VIDSSAVE_DOMAIN)
VIDSSAVE_API_URL = "https://api.vidssave.com/api/contentsite_api/media/parse"
VIDSSAVE_TIMEOUT = 30  # seconds


class VidsSaveError(Exception):
    """Base exception for VidsSave provider errors."""
    pass


class VidsSaveParseError(VidsSaveError):
    """Raised when parsing fails."""
    pass


class VidsSaveDownloadError(VidsSaveError):
    """Raised when download fails."""
    pass


class VidsSaveProvider:
    """Provider for VidsSave service."""

    def __init__(self, auth: Optional[str] = None, domain: Optional[str] = None):
        self.auth = auth or VIDSSAVE_AUTH
        self.domain = domain or VIDSSAVE_DOMAIN
        if not self.auth or not self.domain:
            logger.warning("VidsSave credentials not fully configured. Set VIDSSAVE_AUTH and VIDSSAVE_DOMAIN.")

    def _build_payload(self, link: str) -> Dict[str, str]:
        """Build the form data payload for VidsSave API."""
        return {
            "auth": self.auth,
            "domain": self.domain,
            "origin": "cache",
            "link": link,
        }

    async def parse(self, url: str) -> Dict[str, Any]:
        """
        Parse a YouTube URL using VidsSave API.

        Returns:
            Dict with keys: title, thumbnail, duration, video_id, formats (list)
            Each format contains: type, quality, format, size, download_url
        """
        if not self.auth or not self.domain:
            raise VidsSaveParseError("VidsSave credentials not configured.")

        payload = self._build_payload(url)
        logger.info(f"[VidsSave] Parse started for URL: {url}")

        try:
            async with httpx.AsyncClient(timeout=VIDSSAVE_TIMEOUT) as client:
                response = await client.post(
                    VIDSSAVE_API_URL,
                    data=payload,
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )
                response.raise_for_status()
                data = response.json()
        except httpx.TimeoutException as e:
            logger.error(f"[VidsSave] Parse timeout: {e}")
            raise VidsSaveParseError("Request to VidsSave timed out.")
        except httpx.HTTPStatusError as e:
            logger.error(f"[VidsSave] HTTP error: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 403:
                raise VidsSaveParseError("VidsSave authentication failed or access forbidden.")
            elif e.response.status_code >= 500:
                raise VidsSaveParseError("VidsSave server error.")
            else:
                raise VidsSaveParseError(f"VidsSave returned HTTP {e.response.status_code}")
        except json.JSONDecodeError as e:
            logger.error(f"[VidsSave] Invalid JSON response: {e}")
            raise VidsSaveParseError("Invalid response from VidsSave.")
        except Exception as e:
            logger.error(f"[VidsSave] Unexpected error: {e}")
            raise VidsSaveParseError(f"Unexpected error: {str(e)}")

        # Parse response (based on VidsSave response structure)
        status = data.get("status")
        status_code = data.get("status_code")

        if not (
            status == 1
            or status == "1"
            or status_code == "success"
        ):
            logger.error(f"[VidsSave] Parse unsuccessful: {data}")
            raise VidsSaveParseError(
                f"VidsSave returned unsuccessful status: "
                f"status={status}, status_code={status_code}"
            )

        if "data" not in data:
            raise VidsSaveParseError("VidsSave response has no data field.")

        media_data = data.get("data", {})
        video_id = media_data.get("video_id") or media_data.get("id")
        title = media_data.get("title") or "Untitled"
        thumbnail = media_data.get("thumbnail") or media_data.get("thumb") or ""
        duration = media_data.get("duration") or 0

        # Extract resources (formats)
        resources = media_data.get("resources", [])
        if not resources:
            logger.warning("[VidsSave] No resources found in response.")
            resources = media_data.get("formats", []) or media_data.get("links", [])

        formats = []
        for res in resources:
            if not isinstance(res, dict):
                continue

            fmt_type = res.get("type") or res.get("format_type") or "video"
            quality = res.get("quality") or res.get("label") or res.get("resolution") or "default"
            file_format = res.get("format") or res.get("ext") or "mp4"
            size = res.get("size") or res.get("filesize") or 0
            download_url = res.get("download_url") or res.get("url") or res.get("link")

            if not download_url:
                continue

            if "audio" in str(fmt_type).lower() or "mp3" in str(file_format).lower():
                type_ = "audio"
            else:
                type_ = "video"

            formats.append({
                "format_id": f"{type_}_{quality}_{file_format}",
                "type": type_,
                "quality": quality,
                "format": file_format,
                "size": size,
                "download_url": download_url,
            })

        if not formats:
            raise VidsSaveParseError(
                "VidsSave returned no downloadable resources."
            )

        logger.info(f"[VidsSave] Parse successful. Found {len(formats)} formats.")

        return {
            "video_id": video_id,
            "title": title,
            "thumbnail": thumbnail,
            "duration": duration,
            "formats": formats,
        }

    async def download(self, download_url: str) -> bytes:
        """
        Download the file from the given URL (returns bytes).
        Used as fallback; normally we use streaming.
        """
        try:
            async with httpx.AsyncClient(timeout=VIDSSAVE_TIMEOUT) as client:
                response = await client.get(download_url)
                response.raise_for_status()
                return response.content
        except Exception as e:
            logger.error(f"[VidsSave] Download failed: {e}")
            raise VidsSaveDownloadError(f"Download failed: {str(e)}")

# Load environment variables
load_dotenv()

# Import VidsSave provider

# ------------------------------
# Configuration
# ------------------------------
APP_NAME = os.getenv("APP_NAME", "YT Downloader")

SECRET_KEY = os.getenv(
    "SECRET_KEY",
    "a9f7c2d8e1b34f6a9c8d2e7b1f5a0c6d4e8f2a7b9c1d3e5f7a8b2c4d6e9f1"
)

HOST = os.getenv("HOST", "0.0.0.0")

PORT = int(os.getenv("PORT", 8000))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "sqlite:///./app.db"
)

GOOGLE_CLIENT_ID = os.getenv(
    "GOOGLE_CLIENT_ID",
    "7017036578-h5s1qthshv2s05jeha0lqv8e2tt741la.apps.googleusercontent.com"
)

GOOGLE_CLIENT_SECRET = os.getenv(
    "GOOGLE_CLIENT_SECRET",
    "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"
)

GOOGLE_REDIRECT_URI = os.getenv(
    "GOOGLE_REDIRECT_URI",
    "https://mouhamed.devs.surf/auth/google/callback"
)

ADMIN_EMAIL = os.getenv(
    "ADMIN_EMAIL",
    "mouhamed.support@gmail.com"
)

MAX_FILE_SIZE = int(
    os.getenv(
        "MAX_FILE_SIZE",
        1000 * 1024 * 1024
    )
)  # 1 GB default

FILE_EXPIRATION = int(
    os.getenv("FILE_EXPIRATION", 3600)
)  # seconds

RATE_LIMIT = int(
    os.getenv("RATE_LIMIT", 10)
)  # requests per window

RATE_LIMIT_WINDOW = 60  # seconds

DOWNLOAD_TIMEOUT = int(
    os.getenv("DOWNLOAD_TIMEOUT", 300)
)  # 5 minutes

PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://mouhamed.devs.surf").rstrip("/")


# ------------------------------
# VidsSave configuration
# ------------------------------

VIDSSAVE_AUTH = os.getenv("VIDSSAVE_AUTH", "20250901majwlqo")
VIDSSAVE_DOMAIN = os.getenv("VIDSSAVE_DOMAIN", "api-ak.vidssave.com")
VIDSSAVE_ENABLED = bool(VIDSSAVE_AUTH and VIDSSAVE_DOMAIN)


# ------------------------------
# Email (SMTP) settings
# ------------------------------

SMTP_HOST = os.getenv(
    "SMTP_HOST",
    "smtp.gmail.com"
)

SMTP_PORT = int(
    os.getenv("SMTP_PORT", 587)
)

SMTP_USERNAME = os.getenv(
    "SMTP_USERNAME",
    "mouhamed.support@gmail.com"
)

SMTP_PASSWORD = os.getenv(
    "SMTP_PASSWORD",
    "cpwnaiapkwkzbztc"
)

SMTP_FROM_EMAIL = os.getenv(
    "SMTP_FROM_EMAIL",
    "mouhamed.support@gmail.com"
)

SMTP_FROM_NAME = os.getenv(
    "SMTP_FROM_NAME",
    "YT Downloader MOUHAMED"
)
# Verification code settings
VERIFICATION_CODE_EXPIRY = 600  # 10 minutes
VERIFICATION_MAX_ATTEMPTS = 5
VERIFICATION_MAX_RESEND = 5

# Directory for downloads
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# ------------------------------
# Logging Setup
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------
# Database Setup
# ------------------------------
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ------------------------------
# Models (extended for authentication)
# ------------------------------
class User(Base):
    __tablename__ = "users"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String(255), unique=True, index=True, nullable=True)
    phone = Column(String(20), unique=True, index=True, nullable=True)   # kept for compatibility
    username = Column(String(80), unique=True, index=True, nullable=True)
    password_hash = Column(String(255), nullable=True)
    name = Column(String(255), nullable=False, default="User")
    avatar_url = Column(Text, nullable=True)
    is_admin = Column(Boolean, default=False)
    is_email_verified = Column(Boolean, default=False)
    is_phone_verified = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_login = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    oauth_accounts = relationship("OAuthAccount", back_populates="user", cascade="all, delete-orphan")
    api_keys = relationship("ApiKey", back_populates="user", cascade="all, delete-orphan")
    download_tasks = relationship("DownloadTask", back_populates="user", cascade="all, delete-orphan")
    download_history = relationship("DownloadHistory", back_populates="user", cascade="all, delete-orphan")
    usage_records = relationship("UsageRecord", back_populates="user", cascade="all, delete-orphan")
    email_verifications = relationship("EmailVerification", back_populates="user", cascade="all, delete-orphan")
    password_resets = relationship("PasswordReset", back_populates="user", cascade="all, delete-orphan")

class OAuthAccount(Base):
    __tablename__ = "oauth_accounts"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    provider = Column(String(50), nullable=False)
    provider_user_id = Column(String(255), nullable=False)
    access_token = Column(Text, nullable=True)
    refresh_token = Column(Text, nullable=True)
    expires_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="oauth_accounts")

class ApiKey(Base):
    __tablename__ = "api_keys"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    name = Column(String(100), nullable=False)
    key_hash = Column(String(64), nullable=False, unique=True)
    prefix = Column(String(10), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_used = Column(DateTime, nullable=True)
    usage_count = Column(Integer, default=0)

    user = relationship("User", back_populates="api_keys")

class DownloadTask(Base):
    __tablename__ = "download_tasks"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    url = Column(Text, nullable=False)
    video_id = Column(String(50), nullable=True)
    title = Column(Text, nullable=True)
    format_id = Column(String(50), nullable=True)
    format_type = Column(String(20), nullable=True)
    quality = Column(String(20), nullable=True)
    status = Column(String(20), default="queued")
    progress = Column(Float, default=0.0)
    downloaded_bytes = Column(BigInteger, default=0)
    total_bytes = Column(BigInteger, nullable=True)
    speed = Column(Float, nullable=True)
    eta = Column(Integer, nullable=True)
    file_path = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="download_tasks")

class DownloadHistory(Base):
    __tablename__ = "download_history"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    task_id = Column(String(36), ForeignKey("download_tasks.id"), nullable=True)
    url = Column(Text, nullable=False)
    video_id = Column(String(50), nullable=True)
    title = Column(Text, nullable=True)
    format_type = Column(String(20), nullable=True)
    quality = Column(String(20), nullable=True)
    file_size = Column(BigInteger, nullable=True)
    status = Column(String(20), default="completed")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="download_history")

class UsageRecord(Base):
    __tablename__ = "usage_records"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    endpoint = Column(String(100), nullable=False)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    request_id = Column(String(36), nullable=True)
    duration_ms = Column(Integer, nullable=True)
    status_code = Column(Integer, nullable=True)
    api_key_id = Column(String(36), ForeignKey("api_keys.id"), nullable=True)

    user = relationship("User", back_populates="usage_records")

class SystemLog(Base):
    __tablename__ = "system_logs"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    level = Column(String(20), default="INFO")
    message = Column(Text, nullable=False)
    user_id = Column(String(36), nullable=True)
    task_id = Column(String(36), nullable=True)
    request_id = Column(String(36), nullable=True)
    endpoint = Column(String(100), nullable=True)
    duration_ms = Column(Integer, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class EmailVerification(Base):
    __tablename__ = "email_verifications"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    email = Column(String(255), nullable=False)
    code = Column(String(10), nullable=False)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=VERIFICATION_MAX_ATTEMPTS)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime, nullable=False)
    verified_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="email_verifications")

class PasswordReset(Base):
    __tablename__ = "password_resets"
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String(36), ForeignKey("users.id"), nullable=False)
    code = Column(String(10), nullable=False)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=VERIFICATION_MAX_ATTEMPTS)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at = Column(DateTime, nullable=False)
    used_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="password_resets")

# Create tables
Base.metadata.create_all(bind=engine)

# ------------------------------
# In-Memory State for Progress
# ------------------------------
active_downloads = {}
download_lock = threading.Lock()

# Rate limiting storage
rate_limit_store = {}
rate_limit_lock = threading.Lock()

# ------------------------------
# Helper: format bytes
# ------------------------------
def format_bytes(bytes_val: int) -> str:
    if bytes_val is None or bytes_val == 0:
        return "0 Bytes"
    k = 1024
    sizes = ["Bytes", "KB", "MB", "GB"]
    i = int((bytes_val).bit_length() / 10)
    if i >= len(sizes):
        i = len(sizes) - 1
    return f"{bytes_val / (k ** i):.2f} {sizes[i]}"

# ------------------------------
# Security Helpers
# ------------------------------
serializer = URLSafeTimedSerializer(SECRET_KEY)

def create_session_token(user_id: str) -> str:
    return serializer.dumps({"user_id": user_id}, salt="session")

def decode_session_token(token: str) -> Optional[str]:
    try:
        data = serializer.loads(token, salt="session", max_age=86400 * 7)
        return data.get("user_id")
    except Exception:
        return None

def create_csrf_token() -> str:
    return secrets.token_urlsafe(32)

def hash_api_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()

def generate_api_key() -> tuple[str, str, str]:
    raw = f"app_live_{secrets.token_urlsafe(32)}"
    prefix = raw[:15]
    hashed = hash_api_key(raw)
    return prefix, raw, hashed

def hash_password(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))

def generate_verification_code(length: int = 6) -> str:
    return ''.join(random.choices(string.digits, k=length))

# ------------------------------
# Email Sender
# ------------------------------
def send_email(to_email: str, subject: str, html_content: str):
    if not SMTP_HOST or not SMTP_USERNAME or not SMTP_PASSWORD:
        logger.warning("SMTP not configured, skipping email send")
        return
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        part = MIMEText(html_content, "html")
        msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        logger.info(f"Email sent to {to_email}")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")

def send_verification_email(email: str, code: str):
    subject = f"{APP_NAME} - Email Verification"
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; padding: 20px; background: #f4f4f4;">
        <div style="background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
            <h2 style="color: #6c5ce7;">Verify Your Email</h2>
            <p>Thank you for signing up. Please use the code below to verify your email address:</p>
            <div style="font-size: 32px; font-weight: bold; color: #6c5ce7; text-align: center; padding: 20px; background: #f0f0ff; border-radius: 8px; letter-spacing: 4px;">{code}</div>
            <p style="color: #777;">This code will expire in {VERIFICATION_CODE_EXPIRY // 60} minutes.</p>
            <p>If you didn't request this, please ignore this email.</p>
        </div>
    </body>
    </html>
    """
    send_email(email, subject, html)

def send_password_reset_email(email: str, code: str):
    subject = f"{APP_NAME} - Password Reset"
    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: auto; padding: 20px; background: #f4f4f4;">
        <div style="background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
            <h2 style="color: #6c5ce7;">Reset Your Password</h2>
            <p>We received a request to reset your password. Use the code below:</p>
            <div style="font-size: 32px; font-weight: bold; color: #6c5ce7; text-align: center; padding: 20px; background: #f0f0ff; border-radius: 8px; letter-spacing: 4px;">{code}</div>
            <p style="color: #777;">This code will expire in {VERIFICATION_CODE_EXPIRY // 60} minutes.</p>
            <p>If you didn't request this, please ignore this email.</p>
        </div>
    </body>
    </html>
    """
    send_email(email, subject, html)

# ------------------------------
# SMS functions removed entirely
# ------------------------------

# ------------------------------
# Dependency: Get current user from session
# ------------------------------
def get_current_user(request: Request, db: Session = Depends(lambda: SessionLocal())):
    token = request.cookies.get("session")
    if not token:
        return None
    user_id = decode_session_token(token)
    if not user_id:
        return None
    user = db.query(User).filter(User.id == user_id).first()
    return user

def get_current_user_required(request: Request, db: Session = Depends(lambda: SessionLocal())):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    return user

def get_current_user_or_api_key(request: Request, db: Session = Depends(lambda: SessionLocal())):
    user = get_current_user(request, db)
    if user:
        return user

    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        key = auth_header.split(" ")[1].strip()
        hashed = hash_api_key(key)
        api_key = db.query(ApiKey).filter(ApiKey.key_hash == hashed, ApiKey.is_active == True).first()
        if api_key:
            api_key.last_used = datetime.now(timezone.utc)
            api_key.usage_count += 1
            db.add(api_key)
            db.commit()
            return db.query(User).filter(User.id == api_key.user_id).first()
    return None

def get_current_user_required_api(request: Request, db: Session = Depends(lambda: SessionLocal())):
    user = get_current_user_or_api_key(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="UNAUTHORIZED")
    return user

# ------------------------------
# Rate Limiting
# ------------------------------
def rate_limited(identifier: str, limit: int = RATE_LIMIT, window: int = RATE_LIMIT_WINDOW) -> bool:
    now = time.time()
    with rate_limit_lock:
        if identifier not in rate_limit_store:
            rate_limit_store[identifier] = []
        timestamps = rate_limit_store[identifier]
        timestamps = [ts for ts in timestamps if now - ts < window]
        rate_limit_store[identifier] = timestamps
        if len(timestamps) >= limit:
            return True
        timestamps.append(now)
        return False

# ------------------------------
# URL Validation and SSRF Protection
# ------------------------------
def is_valid_youtube_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        domain = parsed.netloc.lower()
        if ":" in domain:
            domain = domain.split(":")[0]
        if domain == "youtu.be":
            return True
        if domain in ["youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"]:
            if parsed.path == "/watch":
                return True
            if parsed.path.startswith("/shorts/"):
                return True
            if parsed.path.startswith("/live/"):
                return True
            if parsed.path.startswith("/embed/"):
                return True
            return False
        return False
    except Exception:
        return False

def extract_video_id(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        if parsed.netloc.lower() == "youtu.be":
            return parsed.path.lstrip("/").split("/")[0]
        if parsed.path == "/watch":
            return parse_qs(parsed.query).get("v", [None])[0]
        if parsed.path.startswith("/shorts/"):
            return parsed.path.split("/")[2]
        if parsed.path.startswith("/embed/"):
            return parsed.path.split("/")[2]
        if parsed.path.startswith("/live/"):
            return parsed.path.split("/")[2]
    except Exception:
        pass
    return None

def validate_youtube_url(url: str) -> tuple[bool, str, Optional[str]]:
    if not url:
        return False, "INVALID_URL", None
    if not is_valid_youtube_url(url):
        return False, "INVALID_URL", None
    video_id = extract_video_id(url)
    if not video_id:
        return False, "INVALID_URL", None
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        return False, "INVALID_URL", None
    return True, "", video_id

# ------------------------------
# JavaScript Runtime Detection for yt-dlp
# ------------------------------
def detect_js_runtime():
    """Detect available JavaScript runtime for yt-dlp."""
    runtimes = []
    # Check deno
    try:
        subprocess.run(["deno", "--version"], capture_output=True, check=True, timeout=2)
        runtimes.append("deno")
    except:
        pass
    # Check node
    try:
        subprocess.run(["node", "--version"], capture_output=True, check=True, timeout=2)
        runtimes.append("node")
    except:
        pass
    # Check bun
    try:
        subprocess.run(["bun", "--version"], capture_output=True, check=True, timeout=2)
        runtimes.append("bun")
    except:
        pass
    return runtimes[0] if runtimes else None

JS_RUNTIME = detect_js_runtime()
if JS_RUNTIME:
    logger.info(f"Detected JavaScript runtime: {JS_RUNTIME}")
else:
    logger.warning("No JavaScript runtime detected. yt-dlp may have limited functionality.")

# Check if ffmpeg is available
FFMPEG_AVAILABLE = shutil.which("ffmpeg") is not None
if FFMPEG_AVAILABLE:
    logger.info("FFmpeg is available")
else:
    logger.warning("FFmpeg not found. Only direct formats will be available.")

# ------------------------------
# yt-dlp Configuration (No FFmpeg) - Updated with headers and JS runtime
# ------------------------------
def get_ydl_opts_no_ffmpeg(format_id: Optional[str] = None, progress_hook=None):
    opts = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'skip_download': False,
        'logger': logger,
        'progress_hooks': [progress_hook] if progress_hook else [],
        'outtmpl': str(DOWNLOAD_DIR / '%(id)s.%(ext)s'),
        'nocheckcertificate': True,
        'retries': 5,
        'fragment_retries': 5,
        'socket_timeout': 30,
        'noprogress': True,
        'consoletitle': False,
        'continuedl': True,
        'overwrites': True,
        'no_color': True,
        'no_mtime': False,
        'no_post_overwrites': True,
        'no_ffmpeg': True,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        },
        'extractor_args': {
            'youtube': {
                'skip': ['dash', 'hls'],
            }
        },
        'cookiefile': None,
    }
    # Add JS runtime if detected
    if JS_RUNTIME:
        opts['js_runtimes'] = JS_RUNTIME
    if format_id:
        opts['format'] = format_id
    else:
        opts['format'] = 'best[acodec!=none][vcodec!=none]/best'
    return opts

def fetch_video_info(url: str) -> Dict[str, Any]:
    opts = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'skip_download': True,
        'logger': logger,
        'nocheckcertificate': True,
        'no_ffmpeg': True,
        'socket_timeout': 30,
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'headers': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        },
        'extractor_args': {
            'youtube': {
                'skip': ['dash', 'hls'],
            }
        },
        'cookiefile': None,
    }
    if JS_RUNTIME:
        opts['js_runtimes'] = JS_RUNTIME
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                raise Exception("No info returned")
            return info
    except yt_dlp.utils.DownloadError as e:
        error_msg = str(e).lower()
        if "private" in error_msg:
            raise HTTPException(status_code=400, detail="PRIVATE_VIDEO")
        elif "unavailable" in error_msg:
            raise HTTPException(status_code=400, detail="VIDEO_NOT_AVAILABLE")
        elif "403" in error_msg or "forbidden" in error_msg:
            raise HTTPException(status_code=400, detail="ACCESS_DENIED")
        else:
            logger.error(f"yt-dlp DownloadError: {e}")
            raise HTTPException(status_code=400, detail=f"DOWNLOAD_FAILED: {str(e)[:200]}")
    except Exception as e:
        logger.error(f"Error fetching video info: {str(e)}")
        raise HTTPException(status_code=500, detail="INTERNAL_ERROR")

def filter_direct_formats(info: Dict[str, Any]) -> Dict[str, Any]:
    formats = info.get("formats", [])
    direct_video = []
    direct_audio = []
    for fmt in formats:
        acodec = fmt.get("acodec", "none")
        vcodec = fmt.get("vcodec", "none")
        ext = fmt.get("ext", "")
        format_id = fmt.get("format_id", "")
        if fmt.get("format_note") in ("storyboard", "thumb"):
            continue
        if acodec == "none" and vcodec == "none":
            continue
        if acodec != "none" and vcodec != "none":
            direct_video.append({
                "format_id": format_id,
                "ext": ext,
                "resolution": f"{fmt.get('height', '?')}p",
                "height": fmt.get('height'),
                "width": fmt.get('width'),
                "fps": fmt.get('fps'),
                "vcodec": vcodec,
                "acodec": acodec,
                "filesize": fmt.get('filesize') or fmt.get('filesize_approx'),
                "tbr": fmt.get('tbr'),
                "format_note": fmt.get('format_note', ''),
            })
        elif acodec != "none" and vcodec == "none":
            direct_audio.append({
                "format_id": format_id,
                "ext": ext,
                "abr": fmt.get('abr'),
                "acodec": acodec,
                "filesize": fmt.get('filesize') or fmt.get('filesize_approx'),
                "format_note": fmt.get('format_note', ''),
            })

    direct_video.sort(key=lambda x: (x.get('height') or 0, x.get('tbr') or 0), reverse=True)
    direct_audio.sort(key=lambda x: (x.get('abr') or 0), reverse=True)

    return {
        "video_formats": direct_video,
        "audio_formats": direct_audio,
    }

# ------------------------------
# Initialize VidsSave Provider
# ------------------------------
vidsave_provider = None
if VIDSSAVE_ENABLED:
    vidsave_provider = VidsSaveProvider(auth=VIDSSAVE_AUTH, domain=VIDSSAVE_DOMAIN)
    logger.info("VidsSave provider enabled")
else:
    logger.warning("VidsSave provider disabled. Set VIDSSAVE_AUTH and VIDSSAVE_DOMAIN to enable.")

# ------------------------------
# Task Management
# ------------------------------
def create_download_task(db: Session, user_id: str, url: str, format_id: str, format_type: str, title: str, video_id: str) -> DownloadTask:
    task = DownloadTask(
        user_id=user_id,
        url=url,
        video_id=video_id,
        title=title,
        format_id=format_id,
        format_type=format_type,
        status="queued",
        progress=0.0,
    )
    db.add(task)
    db.commit()
    db.refresh(task)
    return task

def start_download_task(task_id: str, download_url: Optional[str] = None, content_type: Optional[str] = None):
    from threading import Thread
    t = Thread(target=download_worker, args=(task_id, download_url, content_type), daemon=True)
    t.start()

def download_worker(task_id: str, download_url: Optional[str] = None, content_type: Optional[str] = None):
    db = SessionLocal()
    try:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id).first()
        if not task:
            logger.error(f"Task {task_id} not found")
            return

        # If we have a direct download URL from VidsSave, use it
        if download_url:
            logger.info(f"[VidsSave] Starting download for task {task_id}")
            task.status = "downloading"
            db.commit()

            chunk_size = 65536  # 64KB for better performance
            safe_title = re.sub(r'[^\w\-_. ]', '', task.title or 'video')[:100]
            # Determine extension from content_type or format_type
            ext = "mp4"
            if content_type:
                if "audio" in content_type.lower() or "mp3" in content_type.lower():
                    ext = "mp3"
                elif "mp4" in content_type.lower() or "video" in content_type.lower():
                    ext = "mp4"
            else:
                ext = task.format_type if task.format_type in ["mp4", "mp3"] else "mp4"
            file_path = DOWNLOAD_DIR / f'{task_id}_{safe_title}.{ext}'

            try:
                with httpx.stream("GET", download_url, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    task.total_bytes = total_size
                    db.commit()

                    with open(file_path, "wb") as f:
                        downloaded = 0
                        for chunk in response.iter_bytes(chunk_size):
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = (downloaded / total_size * 100) if total_size else 0
                            with download_lock:
                                active_downloads[task_id] = {
                                    "progress": progress,
                                    "downloaded_bytes": downloaded,
                                    "total_bytes": total_size,
                                    "speed": 0,
                                    "eta": 0,
                                    "status": "downloading",
                                }
                            task.progress = progress
                            task.downloaded_bytes = downloaded
                            db.commit()

                # Download completed
                task.status = "completed"
                task.progress = 100.0
                task.file_path = str(file_path)
                task.completed_at = datetime.now(timezone.utc)
                task.downloaded_bytes = file_path.stat().st_size
                db.commit()

                # Save history
                history = DownloadHistory(
                    user_id=task.user_id,
                    task_id=task.id,
                    url=task.url,
                    video_id=task.video_id,
                    title=task.title,
                    format_type=task.format_type,
                    quality="direct",
                    file_size=file_path.stat().st_size,
                    status="completed",
                )
                db.add(history)
                db.commit()

                log = SystemLog(level="INFO", message=f"Download completed for task {task_id} via VidsSave", task_id=task_id)
                db.add(log)
                db.commit()

                schedule_file_deletion(file_path, FILE_EXPIRATION)

            except httpx.HTTPStatusError as e:
                logger.error(f"[VidsSave] Download HTTP error: {e.response.status_code}")
                if e.response.status_code == 403:
                    task.status = "failed"
                    task.error_message = "DOWNLOAD_HTTP_403: Download URL expired or forbidden"
                    db.commit()
                else:
                    task.status = "failed"
                    task.error_message = f"HTTP {e.response.status_code}: {str(e)}"
                    db.commit()
                # Clean up partial file
                if file_path.exists():
                    file_path.unlink()
                return
            except Exception as e:
                logger.error(f"[VidsSave] Download failed: {e}")
                task.status = "failed"
                task.error_message = str(e)[:500]
                db.commit()
                if file_path.exists():
                    file_path.unlink()
                return

        else:
            # Fallback to yt-dlp
            logger.info(f"Downloading using yt-dlp for task {task_id}")
            task.status = "starting"
            db.commit()

            def progress_hook(d):
                if d['status'] == 'downloading':
                    progress = d.get('_percent_str', '0%').strip('%')
                    try:
                        progress_float = float(progress)
                    except:
                        progress_float = 0.0
                    downloaded = d.get('downloaded_bytes', 0)
                    total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
                    speed = d.get('speed', 0)
                    eta = d.get('eta', 0)
                    with download_lock:
                        active_downloads[task_id] = {
                            "progress": progress_float,
                            "downloaded_bytes": downloaded,
                            "total_bytes": total,
                            "speed": speed,
                            "eta": eta,
                            "status": "downloading",
                        }
                    task.progress = progress_float
                    task.downloaded_bytes = downloaded
                    task.total_bytes = total
                    task.speed = speed
                    task.eta = eta
                    task.updated_at = datetime.now(timezone.utc)
                    if int(progress_float) % 5 == 0:
                        db.commit()
                elif d['status'] == 'finished':
                    pass

            ydl_opts = get_ydl_opts_no_ffmpeg(format_id=task.format_id, progress_hook=progress_hook)
            safe_title = re.sub(r'[^\w\-_. ]', '', task.title or 'video')[:100]
            ydl_opts['outtmpl'] = str(DOWNLOAD_DIR / f'{task_id}_{safe_title}.%(ext)s')

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(task.url, download=False)
                    if not info:
                        raise Exception("Failed to fetch video info")
                    total_bytes = info.get('filesize') or info.get('filesize_approx') or 0
                    if total_bytes > MAX_FILE_SIZE:
                        raise HTTPException(status_code=413, detail="FILE_TOO_LARGE")
                    task.total_bytes = total_bytes
                    db.commit()
                    ydl.download([task.url])

                downloaded_file = None
                for f in DOWNLOAD_DIR.iterdir():
                    if f.name.startswith(task_id + "_"):
                        downloaded_file = f
                        break
                if not downloaded_file:
                    raise Exception("Downloaded file not found")

                task.status = "completed"
                task.progress = 100.0
                task.file_path = str(downloaded_file)
                task.completed_at = datetime.now(timezone.utc)
                task.downloaded_bytes = downloaded_file.stat().st_size
                db.commit()

                history = DownloadHistory(
                    user_id=task.user_id,
                    task_id=task.id,
                    url=task.url,
                    video_id=task.video_id,
                    title=task.title,
                    format_type=task.format_type,
                    quality=task.quality or "direct",
                    file_size=downloaded_file.stat().st_size,
                    status="completed",
                )
                db.add(history)
                db.commit()

                log = SystemLog(level="INFO", message=f"Download completed for task {task_id} via yt-dlp", task_id=task_id)
                db.add(log)
                db.commit()

                schedule_file_deletion(downloaded_file, FILE_EXPIRATION)

            except yt_dlp.utils.DownloadError as e:
                error_msg = str(e).lower()
                if "403" in error_msg or "forbidden" in error_msg:
                    task.status = "failed"
                    task.error_message = "DOWNLOAD_HTTP_403: Access forbidden by YouTube"
                else:
                    task.status = "failed"
                    task.error_message = str(e)[:500]
                db.commit()
                log = SystemLog(level="ERROR", message=f"yt-dlp download error: {e}", task_id=task_id)
                db.add(log)
                db.commit()
            except HTTPException as he:
                raise
            except Exception as e:
                raise

    except HTTPException as he:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id).first()
        if task:
            task.status = "failed"
            task.error_message = he.detail
            db.commit()
        log = SystemLog(level="ERROR", message=f"Download failed: {he.detail}", task_id=task_id)
        db.add(log)
        db.commit()
    except Exception as e:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id).first()
        if task:
            task.status = "failed"
            task.error_message = str(e)[:500]
            db.commit()
        log = SystemLog(level="ERROR", message=f"Download failed: {str(e)}", task_id=task_id)
        db.add(log)
        db.commit()
        logger.error(f"Download worker error for task {task_id}: {e}", exc_info=True)
    finally:
        with download_lock:
            if task_id in active_downloads:
                active_downloads[task_id]["status"] = task.status if task else "failed"
        db.close()

def schedule_file_deletion(file_path: Path, delay_seconds: int):
    def delete_file():
        time.sleep(delay_seconds)
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Deleted file {file_path} after expiration")
        except Exception as e:
            logger.error(f"Failed to delete file {file_path}: {e}")
    t = threading.Thread(target=delete_file, daemon=True)
    t.start()

def cleanup_expired_files():
    now = time.time()
    for f in DOWNLOAD_DIR.iterdir():
        if f.is_file():
            mtime = f.stat().st_mtime
            if now - mtime > FILE_EXPIRATION:
                try:
                    f.unlink()
                    logger.info(f"Cleaned up expired file {f}")
                except Exception as e:
                    logger.error(f"Failed to cleanup {f}: {e}")

def start_cleanup_scheduler():
    def run():
        while True:
            time.sleep(60)
            cleanup_expired_files()
    t = threading.Thread(target=run, daemon=True)
    t.start()

# ------------------------------
# FastAPI App with Lifespan
# ------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    start_cleanup_scheduler()
    # Log runtime info
    logger.info(f"yt-dlp version: {yt_dlp.version.__version__}")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"JavaScript runtime detected: {JS_RUNTIME or 'none'}")
    logger.info(f"FFmpeg available: {FFMPEG_AVAILABLE}")
    logger.info(f"VidsSave enabled: {VIDSSAVE_ENABLED}")
    logger.info("Application started successfully")
    yield
    # Shutdown (nothing to do)

app = FastAPI(title=APP_NAME, version="1.0.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:;"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    return response

# Request logging and rate limiting middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    path = request.url.path

    # Exclude progress endpoint from rate limiting to avoid 429 during downloads
    if path.startswith("/api/") and not path.startswith("/api/v1/tasks/"):
        user = None
        token = request.cookies.get("session")
        if token:
            user_id = decode_session_token(token)
            if user_id:
                user = user_id
            else:
                user = request.client.host if request.client else "unknown"
        else:
            auth_header = request.headers.get("Authorization")
            if auth_header and auth_header.startswith("Bearer "):
                key = auth_header.split(" ")[1]
                hashed = hash_api_key(key)
                db = SessionLocal()
                api_key = db.query(ApiKey).filter(ApiKey.key_hash == hashed).first()
                db.close()
                user = api_key.user_id if api_key else request.client.host
            else:
                user = request.client.host if request.client else "unknown"
        if rate_limited(str(user)):
            return JSONResponse(status_code=429, content={"detail": "RATE_LIMITED"})

    response = await call_next(request)

    duration_ms = int((time.time() - start_time) * 1000)
    db = SessionLocal()
    try:
        log = SystemLog(
            level="INFO",
            message=f"Request {request.method} {path}",
            request_id=request_id,
            endpoint=path,
            duration_ms=duration_ms,
            user_id=request.state.user_id if hasattr(request.state, 'user_id') else None,
            task_id=None,
            error=None,
        )
        if response.status_code >= 400:
            log.level = "ERROR"
            log.error = f"Status: {response.status_code}"
        db.add(log)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log request: {e}")
    finally:
        db.close()

    return response

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "INTERNAL_SERVER_ERROR"})

# ------------------------------
# HTML Templates (Modern Design) - unchanged
# ------------------------------
def render_base_page(content: str, title: str = APP_NAME, user: Optional[User] = None, request: Request = None) -> str:
    user_avatar = user.avatar_url if user else None
    is_admin = user.is_admin if user else False

    nav_links = f"""
        <a href="/" class="nav-link">Home</a>
        <a href="/dashboard" class="nav-link">Dashboard</a>
        {f'<a href="/dashboard/api-keys" class="nav-link">API</a>' if user else ''}
        {f'<a href="/admin" class="nav-link">Admin</a>' if is_admin else ''}
    """

    if user:
        avatar_html = f'<img src="{user_avatar}" alt="Avatar" loading="lazy">' if user_avatar else user.name[0].upper()
        user_menu = f"""
            <div class="user-menu">
                <div class="avatar">{avatar_html}</div>
                <span class="user-name">{user.name}</span>
                <a href="/auth/logout" class="nav-link">Logout</a>
            </div>
        """
    else:
        user_menu = """
            <div class="auth-actions">
                <a href="/login" class="nav-link">Sign in</a>
                <a href="/register" class="btn btn-small">Get started</a>
            </div>
        """

    return f"""
    <!DOCTYPE html>
    <html lang="en" data-theme="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
        <meta name="theme-color" content="#070b14">
        <meta name="description" content="{APP_NAME} — responsive YouTube media downloader with MP4 and MP3 support.">
        <title>{title} · {APP_NAME}</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
            :root {{
                --bg:#070b14; --bg-soft:#0d1322; --card:rgba(16,24,39,.8); --text:#f8fafc; --muted:#94a3b8;
                --accent:#7c5cff; --accent2:#2dd4bf; --border:rgba(148,163,184,.16); --shadow:0 22px 60px rgba(0,0,0,.28);
                --radius:22px; --trans:.22s ease;
            }}
            *{{box-sizing:border-box}} html,body{{margin:0;min-height:100%}} body{{font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--text);background:radial-gradient(circle at 10% 8%,rgba(124,92,255,.18),transparent 28%),radial-gradient(circle at 90% 8%,rgba(45,212,191,.12),transparent 24%),linear-gradient(180deg,#070b14,#09101b);line-height:1.6}}
            a{{color:inherit;text-decoration:none}} img{{max-width:100%}}
            header{{position:sticky;top:0;z-index:50;background:rgba(7,11,20,.76);backdrop-filter:blur(18px);border-bottom:1px solid var(--border)}}
            .nav{{width:min(1180px,calc(100% - 32px));margin:auto;min-height:72px;display:flex;align-items:center;justify-content:space-between;gap:18px}}
            .brand{{display:flex;align-items:center;gap:11px;font-weight:800}} .brand-mark{{width:40px;height:40px;display:grid;place-items:center;border-radius:13px;background:linear-gradient(135deg,var(--accent),#4f46e5);box-shadow:0 12px 28px rgba(124,92,255,.28)}}
            .nav-links,.auth-actions,.user-menu{{display:flex;align-items:center;gap:6px;flex-wrap:wrap}} .nav-link{{color:var(--muted);padding:9px 11px;border-radius:10px;font-size:.9rem;transition:var(--trans)}} .nav-link:hover{{color:var(--text);background:rgba(255,255,255,.05)}} .user-name{{color:var(--muted);font-size:.85rem;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}} .avatar{{width:34px;height:34px;border-radius:50%;overflow:hidden;display:grid;place-items:center;background:#1e293b;font-size:.82rem;font-weight:700}} .avatar img{{width:100%;height:100%;object-fit:cover}}
            .btn{{border:0;border-radius:14px;padding:12px 17px;color:#fff;background:linear-gradient(135deg,var(--accent),#5b46d8);font-weight:700;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:8px;box-shadow:0 10px 28px rgba(124,92,255,.22);transition:var(--trans)}} .btn:hover{{transform:translateY(-1px)}} .btn:disabled{{opacity:.55;cursor:not-allowed;transform:none}} .btn-outline{{background:rgba(255,255,255,.03);border:1px solid var(--border);box-shadow:none}} .btn-small{{padding:9px 13px;border-radius:11px;font-size:.86rem}} .btn-sm{{padding:8px 12px;border-radius:10px;font-size:.82rem}} .btn-danger{{background:linear-gradient(135deg,#ef4444,#b91c1c)}}
            main{{width:min(1180px,calc(100% - 32px));margin:0 auto;flex:1;padding:42px 0 70px}} footer{{border-top:1px solid var(--border);background:rgba(5,8,14,.72)}} .footer-inner{{width:min(1180px,calc(100% - 32px));margin:auto;padding:22px 0;display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;color:#718096;font-size:.82rem}}
            .hero{{position:relative;overflow:hidden;padding:62px 24px 34px;text-align:center;border:1px solid var(--border);border-radius:32px;background:linear-gradient(180deg,rgba(15,23,42,.8),rgba(10,15,28,.58));box-shadow:var(--shadow)}} .eyebrow{{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border:1px solid rgba(124,92,255,.28);border-radius:999px;background:rgba(124,92,255,.08);color:#c4b5fd;font-size:.8rem;font-weight:700}} .hero h1{{margin:18px auto 12px;max-width:900px;font-size:clamp(2.25rem,7vw,4.75rem);line-height:1.02;letter-spacing:-.055em}} .accent-text{{background:linear-gradient(135deg,#9a8cff,#5eead4);-webkit-background-clip:text;background-clip:text;color:transparent}} .hero p{{max-width:760px;margin:auto;color:var(--muted);font-size:1.02rem}} .hero-actions{{display:flex;gap:12px;justify-content:center;flex-wrap:wrap;margin-top:24px}} .hero-note{{margin-top:13px;color:#718096;font-size:.82rem}}
            .url-form{{width:min(860px,100%);margin:28px auto 0;padding:8px;display:flex;gap:8px;border:1px solid rgba(124,92,255,.28);background:rgba(5,9,16,.72);border-radius:20px}} .url-input{{width:100%;min-width:0;border:0;outline:0;background:transparent;color:var(--text);padding:15px 16px;font-size:1rem;border-radius:14px}} .url-input::placeholder{{color:#64748b}}
            .feature-grid{{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:16px;margin-top:22px}} .feature-card,.card{{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);box-shadow:var(--shadow)}} .feature-card{{padding:22px}} .feature-icon{{width:42px;height:42px;display:grid;place-items:center;border-radius:13px;background:rgba(124,92,255,.12);border:1px solid rgba(124,92,255,.2);margin-bottom:14px}} .feature-card h3{{margin:0 0 7px;font-size:1rem}} .feature-card p{{margin:0;color:var(--muted);font-size:.9rem}}
            .card{{padding:24px}} .section-title{{font-size:1.3rem;letter-spacing:-.03em;margin:34px 0 12px}} .api-strip{{padding:22px;display:grid;grid-template-columns:1.1fr .9fr;gap:18px;align-items:center}} .api-code,.code-panel{{background:#050911;border:1px solid var(--border);border-radius:15px;padding:16px;overflow:auto}} pre{{margin:0;white-space:pre-wrap;word-break:break-word;color:#dbeafe;font: .82rem/1.7 ui-monospace,SFMono-Regular,Menlo,monospace}} code{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#c4b5fd}}
            .video-info{{display:grid;grid-template-columns:minmax(260px,340px) 1fr;gap:24px;align-items:center}} .thumbnail{{border-radius:18px;overflow:hidden;border:1px solid var(--border);background:#050810}} .thumbnail img{{display:block;width:100%;aspect-ratio:16/9;object-fit:cover}} .format-list{{display:grid;gap:10px}} .grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:16px}} .table-wrap{{overflow-x:auto;border:1px solid var(--border);border-radius:14px}} .table{{width:100%;border-collapse:collapse;min-width:720px}} .table th,.table td{{padding:12px 14px;border-bottom:1px solid var(--border);text-align:left}} .table th{{color:#cbd5e1;background:rgba(255,255,255,.025);font-size:.82rem}} .table td{{color:var(--muted);font-size:.86rem}} .muted{{color:var(--muted)}} .stack{{display:grid;gap:14px}} .badge{{display:inline-flex;padding:4px 8px;border-radius:999px;font-size:.72rem;font-weight:700}} .badge.completed{{background:rgba(34,197,94,.12);color:#86efac}} .badge.failed{{background:rgba(239,68,68,.12);color:#fca5a5}}
            .progress-bar{{height:10px;background:#09101d;border:1px solid var(--border);border-radius:999px;overflow:hidden}} .progress-fill{{height:100%;width:0;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:inherit;transition:width .2s}} .spinner{{width:34px;height:34px;margin:0 auto 12px;border:3px solid rgba(255,255,255,.12);border-top-color:var(--accent);border-radius:50%;animation:spin .8s linear infinite}} @keyframes spin{{to{{transform:rotate(360deg)}}}} .hidden{{display:none!important}}
            @media(max-width:900px){{.feature-grid{{grid-template-columns:1fr}}.api-strip{{grid-template-columns:1fr}}.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}.video-info{{grid-template-columns:1fr}}}}
            @media(max-width:640px){{.nav{{width:min(100% - 20px,1180px);min-height:64px}}.nav-links>.nav-link{{display:none}}.user-name{{display:none}}main{{width:min(100% - 20px,1180px);padding-top:24px}}.hero{{padding:34px 16px 24px;border-radius:24px}}.hero h1{{font-size:clamp(2rem,13vw,3.2rem)}}.url-form{{flex-direction:column;border-radius:18px}}.url-form .btn{{width:100%}}.card{{padding:18px;border-radius:18px}}.grid{{grid-template-columns:1fr}}}}
        </style>
    </head>
    <body>
        <div class="site-shell">
            <header><nav class="nav"><a class="brand" href="/"><span class="brand-mark">Y</span><span>{APP_NAME}</span></a><div class="nav-links">{nav_links}{user_menu}</div></nav></header>
            <main>{content}</main>
            <footer><div class="footer-inner"><span>© 2026 {APP_NAME}. All rights reserved.</span><span>Use this service only for content you are authorized to download.</span></div></footer>
        </div>
        <script>
            function showToast(message,type='success'){{const old=document.getElementById('site-toast');if(old)old.remove();const toast=document.createElement('div');toast.id='site-toast';toast.textContent=message;toast.style.cssText='position:fixed;right:18px;bottom:18px;z-index:1000;max-width:min(92vw,420px);padding:13px 16px;border-radius:14px;color:#fff;font:600 14px Inter,sans-serif;background:'+(type==='error'?'#991b1b':'#14532d')+';box-shadow:0 18px 40px rgba(0,0,0,.35)';document.body.appendChild(toast);setTimeout(()=>toast.remove(),3200)}}
        </script>
    </body>
    </html>
    """

def render_home_page(request: Request, user: Optional[User] = None) -> str:
    if not user:
        content = """
        <section class="hero">
            <span class="eyebrow">⚡ Fast · Responsive · No-FFmpeg</span>
            <h1>Simple media downloads with a <span class="accent-text">better workflow</span></h1>
            <p>Sign in, paste a YouTube link, inspect the available MP4/MP3 options, and download from a clean workspace built for phones and desktops.</p>
            <div class="hero-actions"><a href="/register" class="btn">Create free account</a><a href="/login" class="btn btn-outline">Sign in</a></div>
            <div class="hero-note">Account required before analysis and downloads.</div>
        </section>
        <div class="feature-grid">
            <div class="feature-card"><div class="feature-icon">🎬</div><h3>MP4 video</h3><p>Choose from available direct video qualities without local FFmpeg processing.</p></div>
            <div class="feature-card"><div class="feature-icon">🎵</div><h3>MP3 audio</h3><p>Select an available audio option and download it from your account.</p></div>
            <div class="feature-card"><div class="feature-icon">🔑</div><h3>Developer API</h3><p>Create a private API key and automate analysis/download jobs from your own tools.</p></div>
        </div>
        <h2 class="section-title">How it works</h2>
        <div class="card"><div class="grid" style="grid-template-columns:repeat(3,minmax(0,1fr));"><div><strong>01 · Create account</strong><p class="muted">Register and verify your email.</p></div><div><strong>02 · Analyze</strong><p class="muted">Paste a YouTube URL and choose a direct format.</p></div><div><strong>03 · Download</strong><p class="muted">Track progress and get the finished file.</p></div></div></div>
        <h2 class="section-title">API example</h2>
        <div class="card api-strip"><div><strong>Base URL</strong><p class="muted"><code>{PUBLIC_BASE_URL}</code></p><p class="muted">API keys are sent as <code>Authorization: Bearer YOUR_API_KEY</code>.</p></div><div class="api-code">curl -X POST {PUBLIC_BASE_URL}/api/v1/analyze \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{{"url":"https://www.youtube.com/watch?v=VIDEO_ID"}}'</div></div>
        <div class="card" style="margin-top:16px;text-align:center;"><strong>Usage notice</strong><p class="muted" style="margin:.35rem 0 0;">Use the service only for media you are authorized to download and follow the terms of the relevant platform and applicable law.</p></div>
        """
        return render_base_page(content,"Home",user=user,request=request)

    content = """
    <section class="hero">
        <span class="eyebrow">🎯 Ready when you are</span>
        <h1>Paste a link. <span class="accent-text">Pick a format.</span></h1>
        <p>Analyze a YouTube URL, compare the available direct options, and start your download with progress tracking.</p>
        <form id="analyze-form" class="url-form"><input type="url" id="youtube-url" class="url-input" placeholder="https://www.youtube.com/watch?v=..." required autocomplete="off"><button type="submit" class="btn" id="analyze-btn">Analyze video</button></form>
        <div class="hero-note">Public API base URL: {PUBLIC_BASE_URL}</div>
    </section>
    <div class="loading-container" id="loading" style="display:none;text-align:center;padding:34px 10px;"><div class="spinner"></div><p class="muted">Analyzing…</p></div>
    <div class="card result-card" id="result-card" style="display:none;">
        <div class="video-info"><div class="thumbnail"><img id="video-thumbnail" src="" alt="Thumbnail"></div><div><h2 id="video-title" style="margin:0 0 6px;font-size:clamp(1.35rem,3vw,1.9rem);">Title</h2><p id="video-channel" class="muted">Channel</p><div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px;"><span class="eyebrow" id="video-duration">⏱ Duration</span><span class="eyebrow" id="video-id">🎬 ID</span></div></div></div>
        <div id="format-section" style="margin-top:26px;"><h3 style="margin:0 0 12px;">Available formats</h3><div class="format-list" id="format-list"></div><button id="download-btn" class="btn" style="width:100%;margin-top:18px;" disabled>Download selected</button></div>
        <div id="download-progress" style="display:none;margin-top:22px;"><div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:8px;"><span id="progress-status" class="muted">Starting…</span><span id="progress-stats" class="muted">0%</span></div><div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div></div>
    </div>
    <div class="feature-grid" style="margin-top:22px;"><div class="feature-card"><div class="feature-icon">🚀</div><h3>Fast workflow</h3><p>Analyze, choose and download from the same page.</p></div><div class="feature-card"><div class="feature-icon">🛡️</div><h3>Account based</h3><p>History, keys and downloads stay linked to your account.</p></div><div class="feature-card"><div class="feature-icon">📘</div><h3>API ready</h3><p>Open Dashboard → API to copy integration examples.</p></div></div>
    <script>
        const analyzeForm=document.getElementById('analyze-form'),urlInput=document.getElementById('youtube-url'),analyzeBtn=document.getElementById('analyze-btn'),loading=document.getElementById('loading'),resultCard=document.getElementById('result-card'),formatList=document.getElementById('format-list'),downloadBtn=document.getElementById('download-btn'),progressSection=document.getElementById('download-progress'),progressFill=document.getElementById('progress-fill'),progressStatus=document.getElementById('progress-status'),progressStats=document.getElementById('progress-stats');
        let selectedFormat=null,isDownloading=false;
        analyzeForm.addEventListener('submit',async(e)=>{e.preventDefault();const url=urlInput.value.trim();if(!url||(!url.includes('youtube.com')&&!url.includes('youtu.be'))){showToast('Enter a valid YouTube URL','error');return;}analyzeBtn.disabled=true;analyzeBtn.textContent='Analyzing…';loading.style.display='block';resultCard.style.display='none';selectedFormat=null;downloadBtn.disabled=true;try{const r=await fetch('/api/v1/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url})});const d=await r.json();if(!r.ok)throw new Error(d.detail||'Failed to analyze');displayResult(d)}catch(err){showToast(err.message||'Failed to analyze video','error')}finally{loading.style.display='none';analyzeBtn.disabled=false;analyzeBtn.textContent='Analyze video'}});
        function displayResult(data){document.getElementById('video-thumbnail').src=data.thumbnail||'';document.getElementById('video-title').textContent=data.title||'Unknown Title';document.getElementById('video-channel').textContent=data.channel||'Unknown Channel';document.getElementById('video-duration').textContent='⏱ '+(data.duration?formatDuration(data.duration):'Unknown');document.getElementById('video-id').textContent='🎬 '+(data.video_id||'N/A');formatList.innerHTML='';let has=false;const source=data.source||'ytdlp';const add=(fmt,type)=>{has=true;const div=document.createElement('div');div.className='format-item';div.style.cssText='display:flex;justify-content:space-between;align-items:center;gap:12px;padding:15px 16px;background:var(--bg-soft);border:1px solid var(--border);border-radius:15px;cursor:pointer';div.innerHTML=`<div><strong>${type==='video'?'🎥 MP4':'🎵 MP3'}</strong><div class="muted">${type==='video'?(fmt.resolution||'Unknown'):(fmt.abr?fmt.abr+'kbps':'Audio')} ${fmt.ext||''}</div></div><div class="muted">${fmt.filesize?formatBytes(fmt.filesize):'Unknown size'}</div>`;div.onclick=()=>selectFormat(div,fmt,type,source);div.dataset.formatId=fmt.format_id;formatList.appendChild(div)};(data.video_formats||[]).forEach(f=>add(f,'video'));(data.audio_formats||[]).forEach(f=>add(f,'audio'));if(!has)formatList.innerHTML='<p class="muted">No direct formats available.</p>';resultCard.style.display='block';progressSection.style.display='none';downloadBtn.disabled=true}
        function selectFormat(el,fmt,type,source){document.querySelectorAll('.format-item').forEach(x=>x.style.borderColor='var(--border)');el.style.borderColor='var(--accent)';selectedFormat={format_id:fmt.format_id,type,source,download_url:fmt.download_url||null};downloadBtn.disabled=false}
        downloadBtn.addEventListener('click',async()=>{if(!selectedFormat||isDownloading)return;const url=urlInput.value.trim();isDownloading=true;downloadBtn.disabled=true;downloadBtn.textContent='Starting…';progressSection.style.display='block';progressFill.style.width='0%';try{const r=await fetch('/api/v1/download',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url,format_id:selectedFormat.format_id,format_type:selectedFormat.type,source:selectedFormat.source})});const d=await r.json();if(!r.ok)throw new Error(d.detail||'Download failed');pollProgress(d.task_id)}catch(err){showToast(err.message||'Download failed','error');isDownloading=false;downloadBtn.disabled=false;downloadBtn.textContent='Download selected'}});
        function pollProgress(taskId){const interval=setInterval(async()=>{try{const r=await fetch(`/api/v1/tasks/${taskId}/progress`);const d=await r.json();if(!r.ok)throw new Error(d.detail);const p=Number(d.progress||0);progressFill.style.width=p+'%';progressStats.textContent=Math.round(p)+'%';progressStatus.textContent=d.status||'Working…';if(d.status==='completed'){clearInterval(interval);showToast('Download completed','success');isDownloading=false;downloadBtn.disabled=false;downloadBtn.textContent='Download selected';const a=document.createElement('a');a.href=`/api/v1/download-file/${taskId}`;document.body.appendChild(a);a.click();a.remove()}else if(d.status==='failed'){clearInterval(interval);showToast(d.error||'Download failed','error');isDownloading=false;downloadBtn.disabled=false;downloadBtn.textContent='Download selected'}}catch(err){clearInterval(interval);showToast(err.message||'Progress error','error');isDownloading=false;downloadBtn.disabled=false;downloadBtn.textContent='Download selected'}},1400)}
        function formatDuration(s){const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),sec=Math.floor(s%60);return(h?h+':':'')+(m<10?'0':'')+m+':'+(sec<10?'0':'')+sec}function formatBytes(b){if(!b)return'0 Bytes';const k=1024,s=['Bytes','KB','MB','GB'],i=Math.min(3,Math.floor(Math.log(b)/Math.log(k)));return(b/Math.pow(k,i)).toFixed(2)+' '+s[i]}
    </script>
    """
    return render_base_page(content,"Home",user=user,request=request)

@app.get("/login")
async def login_page(request: Request):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        if user:
            return RedirectResponse(url="/dashboard")
        content = """
        <div class="auth-container">
            <div class="auth-card">
                <h1>Welcome Back</h1>
                <p class="sub">Sign in to continue</p>
                <form id="login-form">
                    <div class="form-group">
                        <label for="email">Email</label>
                        <input type="email" id="email" placeholder="you@example.com" required>
                    </div>
                    <div class="form-group">
                        <label for="password">Password</label>
                        <input type="password" id="password" placeholder="Enter your password" required>
                    </div>
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:16px;">
                        <label style="display:flex; align-items:center; gap:6px; font-size:0.9rem;">
                            <input type="checkbox" id="remember"> Remember me
                        </label>
                        <a href="/forgot-password" style="color:var(--accent); text-decoration:none; font-size:0.9rem;">Forgot password?</a>
                    </div>
                    <button type="submit" class="btn">Sign In</button>
                </form>
                <div class="divider">or</div>
                <a href="/auth/google" class="btn google-btn" style="text-decoration:none; display:flex; align-items:center; justify-content:center; gap:8px;">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
                    Continue with Google
                </a>
                <div class="footer-links">
                    Don't have an account? <a href="/register">Sign Up</a>
                </div>
            </div>
        </div>
        <script>
            document.getElementById('login-form').addEventListener('submit', async (e) => {
                e.preventDefault();
                const email = document.getElementById('email').value.trim();
                const password = document.getElementById('password').value;
                const remember = document.getElementById('remember').checked;
                try {
                    const res = await fetch('/auth/login', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ email, password, remember })
                    });
                    const data = await res.json();
                    if (res.ok) {
                        showToast('Login successful', 'success');
                        window.location.href = '/dashboard';
                    } else {
                        showToast(data.detail || 'Login failed', 'error');
                    }
                } catch (err) {
                    showToast('Network error', 'error');
                }
            });
        </script>
        """
        return HTMLResponse(render_base_page(content, "Login", user=None, request=request))
    finally:
        db.close()

@app.post("/auth/login")
async def login(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    email = body.get("email")
    password = body.get("password")
    remember = body.get("remember", False)
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user or not user.password_hash:
            raise HTTPException(status_code=401, detail="Invalid credentials")

        if not user.is_email_verified:
            raise HTTPException(status_code=403, detail="Email not verified")

        if not verify_password(password, user.password_hash):
            raise HTTPException(status_code=401, detail="Invalid credentials")

        user.last_login = datetime.now(timezone.utc)
        db.commit()

        user_id = user.id
        token = create_session_token(user_id)
        max_age = 86400 * 7 if remember else 86400
        response = JSONResponse({"status": "success"})
        response.set_cookie(
            key="session",
            value=token,
            httponly=True,
            secure=os.getenv("SECURE_COOKIES", "false").lower() == "true",
            samesite="lax",
            max_age=max_age,
        )
        return response
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/register")
async def register_page(request: Request):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        if user:
            return RedirectResponse(url="/dashboard")
        content = """
        <div class="auth-container">
            <div class="auth-card">
                <h1>Create Account</h1>
                <p class="sub">Join us and start downloading</p>
                <form id="register-form">
                    <div class="form-group">
                        <label for="username">Username</label>
                        <input type="text" id="username" placeholder="johndoe" required>
                    </div>
                    <div class="form-group">
                        <label for="email">Email</label>
                        <input type="email" id="email" placeholder="you@example.com" required>
                    </div>
                    <div class="form-group">
                        <label for="password">Password</label>
                        <input type="password" id="password" placeholder="Min 8 characters" required>
                    </div>
                    <div class="form-group">
                        <label for="confirm">Confirm Password</label>
                        <input type="password" id="confirm" placeholder="Confirm password" required>
                    </div>
                    <button type="submit" class="btn">Create Account</button>
                </form>
                <div class="divider">or</div>
                <a href="/auth/google" class="btn google-btn" style="text-decoration:none; display:flex; align-items:center; justify-content:center; gap:8px;">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 0 1-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
                    Continue with Google
                </a>
                <div class="footer-links">
                    Already have an account? <a href="/login">Sign In</a>
                </div>
            </div>
        </div>
        <script>
            document.getElementById('register-form').addEventListener('submit', async (e) => {
                e.preventDefault();
                const username = document.getElementById('username').value.trim();
                const email = document.getElementById('email').value.trim();
                const password = document.getElementById('password').value;
                const confirm = document.getElementById('confirm').value;

                if (password !== confirm) {
                    showToast('Passwords do not match', 'error');
                    return;
                }
                if (password.length < 8) {
                    showToast('Password must be at least 8 characters', 'error');
                    return;
                }

                try {
                    const res = await fetch('/auth/register', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ username, email, password })
                    });
                    const data = await res.json();
                    if (res.ok) {
                        showToast('Registration successful! Please verify your email.', 'success');
                        window.location.href = '/verify-email?email=' + encodeURIComponent(email);
                    } else {
                        showToast(data.detail || 'Registration failed', 'error');
                    }
                } catch (err) {
                    showToast('Network error', 'error');
                }
            });
        </script>
        """
        return HTMLResponse(render_base_page(content, "Register", user=None, request=request))
    finally:
        db.close()

@app.post("/auth/register")
async def register(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    username = body.get("username")
    email = body.get("email")
    password = body.get("password")

    if not username or not email or not password:
        raise HTTPException(status_code=400, detail="Missing fields")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password too short")

    db = SessionLocal()
    try:
        existing = db.query(User).filter((User.username == username) | (User.email == email)).first()
        if existing:
            raise HTTPException(status_code=400, detail="Username or email already taken")

        user = User(
            username=username,
            email=email,
            password_hash=hash_password(password),
            name=username,
            is_email_verified=False,
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        code = generate_verification_code()
        expires = datetime.now(timezone.utc) + timedelta(seconds=VERIFICATION_CODE_EXPIRY)
        verif = EmailVerification(
            user_id=user.id,
            email=email,
            code=code,
            expires_at=expires,
        )
        db.add(verif)
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    send_verification_email(email, code)
    return JSONResponse({"status": "success", "message": "Verification code sent"})

@app.get("/verify-email")
async def verify_email_page(request: Request, email: Optional[str] = None):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        if user and user.is_email_verified:
            return RedirectResponse(url="/dashboard")
        content = f"""
        <div class="auth-container">
            <div class="auth-card">
                <h1>Verify Your Email</h1>
                <p class="sub">We sent a code to <strong>{email or 'your email'}</strong></p>
                <form id="verify-form">
                    <div class="form-group">
                        <label for="code">Verification Code</label>
                        <input type="text" id="code" placeholder="Enter 6-digit code" required maxlength="6" pattern="[0-9]{{6}}">
                    </div>
                    <button type="submit" class="btn">Verify</button>
                </form>
                <div style="margin-top:16px; text-align:center;">
                    <button id="resend-btn" class="btn btn-outline" style="width:auto;">Resend Code</button>
                </div>
                <div class="footer-links">
                    <a href="/login">Back to Login</a>
                </div>
            </div>
        </div>
        <script>
            const email = "{email or ''}";
            document.getElementById('verify-form').addEventListener('submit', async (e) => {{
                e.preventDefault();
                const code = document.getElementById('code').value.trim();
                if (!code) return;
                try {{
                    const res = await fetch('/auth/verify-email', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ email, code }})
                    }});
                    const data = await res.json();
                    if (res.ok) {{
                        showToast('Email verified successfully', 'success');
                        window.location.href = '/login';
                    }} else {{
                        showToast(data.detail || 'Verification failed', 'error');
                    }}
                }} catch (err) {{
                    showToast('Network error', 'error');
                }}
            }});

            document.getElementById('resend-btn').addEventListener('click', async () => {{
                if (!email) {{ showToast('Email not provided', 'error'); return; }}
                try {{
                    const res = await fetch('/auth/resend-verification', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ email }})
                    }});
                    const data = await res.json();
                    if (res.ok) {{
                        showToast('New code sent', 'success');
                    }} else {{
                        showToast(data.detail || 'Failed to resend', 'error');
                    }}
                }} catch (err) {{
                    showToast('Network error', 'error');
                }}
            }});
        </script>
        """
        return HTMLResponse(render_base_page(content, "Verify Email", user=None, request=request))
    finally:
        db.close()

@app.post("/auth/verify-email")
async def verify_email(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    email = body.get("email")
    code = body.get("code")
    if not email or not code:
        raise HTTPException(status_code=400, detail="Missing fields")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user.is_email_verified:
            raise HTTPException(status_code=400, detail="Already verified")

        verif = db.query(EmailVerification).filter(
            EmailVerification.user_id == user.id,
            EmailVerification.email == email,
            EmailVerification.code == code,
            EmailVerification.verified_at == None
        ).first()
        if not verif:
            record = db.query(EmailVerification).filter(
                EmailVerification.user_id == user.id,
                EmailVerification.email == email,
                EmailVerification.verified_at == None
            ).order_by(EmailVerification.created_at.desc()).first()
            if record:
                record.attempts += 1
                if record.attempts >= record.max_attempts:
                    db.delete(record)
                    db.commit()
                    raise HTTPException(status_code=400, detail="Too many attempts, request a new code")
                db.commit()
            raise HTTPException(status_code=400, detail="Invalid code")

        if datetime.now(timezone.utc) > verif.expires_at:
            db.delete(verif)
            db.commit()
            raise HTTPException(status_code=400, detail="Code expired, request a new one")

        verif.verified_at = datetime.now(timezone.utc)
        user.is_email_verified = True
        db.commit()
        return JSONResponse({"status": "success"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Email verification error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.post("/auth/resend-verification")
async def resend_verification(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    email = body.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email required")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        if user.is_email_verified:
            raise HTTPException(status_code=400, detail="Already verified")

        db.query(EmailVerification).filter(
            EmailVerification.user_id == user.id,
            EmailVerification.verified_at == None
        ).delete()
        db.commit()

        code = generate_verification_code()
        expires = datetime.now(timezone.utc) + timedelta(seconds=VERIFICATION_CODE_EXPIRY)
        verif = EmailVerification(
            user_id=user.id,
            email=email,
            code=code,
            expires_at=expires,
        )
        db.add(verif)
        db.commit()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Resend verification error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    send_verification_email(email, code)
    return JSONResponse({"status": "success"})

@app.get("/forgot-password")
async def forgot_password_page(request: Request):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        if user:
            return RedirectResponse(url="/dashboard")
        content = """
        <div class="auth-container">
            <div class="auth-card">
                <h1>Reset Password</h1>
                <p class="sub">Enter your email to receive a reset code</p>
                <form id="forgot-form">
                    <div class="form-group">
                        <label for="email">Email</label>
                        <input type="email" id="email" placeholder="you@example.com" required>
                    </div>
                    <button type="submit" class="btn">Send Reset Code</button>
                </form>
                <div class="footer-links">
                    <a href="/login">Back to Login</a>
                </div>
            </div>
        </div>
        <script>
            document.getElementById('forgot-form').addEventListener('submit', async (e) => {
                e.preventDefault();
                const email = document.getElementById('email').value.trim();
                try {
                    const res = await fetch('/auth/forgot-password', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ email })
                    });
                    const data = await res.json();
                    if (res.ok) {
                        showToast('Reset code sent to your email', 'success');
                        window.location.href = '/reset-password?email=' + encodeURIComponent(email);
                    } else {
                        showToast(data.detail || 'Failed', 'error');
                    }
                } catch (err) {
                    showToast('Network error', 'error');
                }
            });
        </script>
        """
        return HTMLResponse(render_base_page(content, "Forgot Password", user=None, request=request))
    finally:
        db.close()

@app.post("/auth/forgot-password")
async def forgot_password(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    email = body.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Email required")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user:
            return JSONResponse({"status": "success", "message": "If the email exists, a reset code was sent."})

        db.query(PasswordReset).filter(PasswordReset.user_id == user.id, PasswordReset.used_at == None).delete()
        db.commit()

        code = generate_verification_code()
        expires = datetime.now(timezone.utc) + timedelta(seconds=VERIFICATION_CODE_EXPIRY)
        reset = PasswordReset(
            user_id=user.id,
            code=code,
            expires_at=expires,
        )
        db.add(reset)
        db.commit()
    except Exception as e:
        logger.error(f"Forgot password error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    send_password_reset_email(email, code)
    return JSONResponse({"status": "success"})

@app.get("/reset-password")
async def reset_password_page(request: Request, email: Optional[str] = None):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        if user:
            return RedirectResponse(url="/dashboard")
        content = f"""
        <div class="auth-container">
            <div class="auth-card">
                <h1>Reset Password</h1>
                <p class="sub">Enter the code and new password</p>
                <form id="reset-form">
                    <div class="form-group">
                        <label for="code">Reset Code</label>
                        <input type="text" id="code" placeholder="Enter 6-digit code" required maxlength="6" pattern="[0-9]{{6}}">
                    </div>
                    <div class="form-group">
                        <label for="new-password">New Password</label>
                        <input type="password" id="new-password" placeholder="Min 8 characters" required>
                    </div>
                    <div class="form-group">
                        <label for="confirm-password">Confirm Password</label>
                        <input type="password" id="confirm-password" placeholder="Confirm new password" required>
                    </div>
                    <button type="submit" class="btn">Reset Password</button>
                </form>
                <div class="footer-links">
                    <a href="/login">Back to Login</a>
                </div>
            </div>
        </div>
        <script>
            const email = "{email or ''}";
            document.getElementById('reset-form').addEventListener('submit', async (e) => {{
                e.preventDefault();
                const code = document.getElementById('code').value.trim();
                const password = document.getElementById('new-password').value;
                const confirm = document.getElementById('confirm-password').value;
                if (password !== confirm) {{
                    showToast('Passwords do not match', 'error');
                    return;
                }}
                if (password.length < 8) {{
                    showToast('Password must be at least 8 characters', 'error');
                    return;
                }}
                try {{
                    const res = await fetch('/auth/reset-password', {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ email, code, password }})
                    }});
                    const data = await res.json();
                    if (res.ok) {{
                        showToast('Password reset successfully', 'success');
                        window.location.href = '/login';
                    }} else {{
                        showToast(data.detail || 'Reset failed', 'error');
                    }}
                }} catch (err) {{
                    showToast('Network error', 'error');
                }}
            }});
        </script>
        """
        return HTMLResponse(render_base_page(content, "Reset Password", user=None, request=request))
    finally:
        db.close()

@app.post("/auth/reset-password")
async def reset_password(request: Request):
    try:
        body = await request.json()
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    email = body.get("email")
    code = body.get("code")
    password = body.get("password")
    if not email or not code or not password:
        raise HTTPException(status_code=400, detail="Missing fields")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password too short")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        reset = db.query(PasswordReset).filter(
            PasswordReset.user_id == user.id,
            PasswordReset.code == code,
            PasswordReset.used_at == None
        ).first()
        if not reset:
            record = db.query(PasswordReset).filter(
                PasswordReset.user_id == user.id,
                PasswordReset.used_at == None
            ).order_by(PasswordReset.created_at.desc()).first()
            if record:
                record.attempts += 1
                if record.attempts >= record.max_attempts:
                    db.delete(record)
                    db.commit()
                    raise HTTPException(status_code=400, detail="Too many attempts, request a new code")
                db.commit()
            raise HTTPException(status_code=400, detail="Invalid code")

        if datetime.now(timezone.utc) > reset.expires_at:
            db.delete(reset)
            db.commit()
            raise HTTPException(status_code=400, detail="Code expired")

        user.password_hash = hash_password(password)
        reset.used_at = datetime.now(timezone.utc)
        db.commit()
        return JSONResponse({"status": "success"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reset password error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

# ==================== PHONE ROUTES REMOVED ====================

# Google OAuth
@app.get("/auth/google")
async def google_auth_redirect():
    if not GOOGLE_CLIENT_ID or not GOOGLE_REDIRECT_URI:
        raise HTTPException(status_code=400, detail="Google OAuth not configured")
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?client_id={GOOGLE_CLIENT_ID}&redirect_uri={GOOGLE_REDIRECT_URI}&response_type=code&scope=openid%20email%20profile&access_type=offline&prompt=consent"
    return RedirectResponse(url=auth_url)

@app.get("/auth/google/callback")
async def google_auth_callback(code: str = Query(...)):
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET or not GOOGLE_REDIRECT_URI:
        raise HTTPException(status_code=400, detail="Google OAuth not configured")
    async with httpx.AsyncClient() as client:
        token_resp = await client.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        })
        if token_resp.status_code != 200:
            logger.error(f"Google token exchange failed: {token_resp.text}")
            raise HTTPException(status_code=400, detail="OAuth failed")
        token_data = token_resp.json()
        access_token = token_data.get("access_token")
        user_resp = await client.get("https://www.googleapis.com/oauth2/v2/userinfo", headers={"Authorization": f"Bearer {access_token}"})
        if user_resp.status_code != 200:
            raise HTTPException(status_code=400, detail="Failed to get user info")
        user_info = user_resp.json()

    google_id = user_info.get("id")
    email = user_info.get("email")
    name = user_info.get("name")
    avatar = user_info.get("picture")

    if not email:
        raise HTTPException(status_code=400, detail="Email not provided")

    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if not user:
            user = User(
                email=email,
                name=name or email,
                avatar_url=avatar,
                is_email_verified=True,
                is_phone_verified=False,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
        else:
            user.name = name or user.name
            user.avatar_url = avatar or user.avatar_url
            user.is_email_verified = True
            user.last_login = datetime.now(timezone.utc)
            db.commit()

        oauth = db.query(OAuthAccount).filter(
            OAuthAccount.provider == "google",
            OAuthAccount.provider_user_id == google_id
        ).first()
        if not oauth:
            oauth = OAuthAccount(user_id=user.id, provider="google", provider_user_id=google_id)
            db.add(oauth)
        oauth.access_token = access_token
        oauth.expires_at = datetime.now(timezone.utc) + timedelta(seconds=token_data.get("expires_in", 3600))
        db.commit()

        token = create_session_token(user.id)
        response = RedirectResponse(url="/dashboard")
        response.set_cookie(
            key="session",
            value=token,
            httponly=True,
            secure=os.getenv("SECURE_COOKIES", "false").lower() == "true",
            samesite="lax",
            max_age=86400 * 7,
        )
        return response
    except Exception as e:
        logger.error(f"OAuth error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/auth/logout")
async def logout():
    response = RedirectResponse(url="/")
    response.delete_cookie("session")
    return response

# ------------------------------
# Protected Pages
# ------------------------------
@app.get("/dashboard")
async def dashboard_page(request: Request, user: User = Depends(get_current_user_required)):
    db = SessionLocal()
    try:
        total_downloads = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id).count()
        successful = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id, DownloadHistory.status == "completed").count()
        failed = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id, DownloadHistory.status == "failed").count()
        total_api_requests = db.query(UsageRecord).filter(UsageRecord.user_id == user.id).count()
        storage_used = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id, DownloadHistory.file_size != None).with_entities(func.sum(DownloadHistory.file_size)).scalar() or 0
        recent = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id).order_by(DownloadHistory.created_at.desc()).limit(5).all()

        rows = []
        for h in recent:
            title_short = h.title[:50] if h.title else ''
            date_str = h.created_at.strftime('%Y-%m-%d %H:%M') if h.created_at else ''
            rows.append(f"<tr><td>{title_short}</td><td>{h.format_type}</td><td>{h.quality}</td><td><span class='badge {h.status}'>{h.status}</span></td><td>{date_str}</td><td><a href='/api/v1/download-file/{h.task_id}' class='btn btn-sm'>Download</a></td></tr>")
        rows_html = ''.join(rows)
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    content = f"""
    <h1 style="font-size:2rem; font-weight:700; margin-bottom:8px;">Welcome back, {user.name}</h1>
    <p style="color:var(--text-secondary); margin-bottom:30px;">Here's a summary of your activity.</p>
    <div class="grid">
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Total Downloads</h3><p style="font-size:2.2rem; font-weight:700;">{total_downloads}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Successful</h3><p style="font-size:2.2rem; font-weight:700; color:#2ecc71;">{successful}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Failed</h3><p style="font-size:2.2rem; font-weight:700; color:#e74c3c;">{failed}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">API Requests</h3><p style="font-size:2.2rem; font-weight:700;">{total_api_requests}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Storage Used</h3><p style="font-size:2.2rem; font-weight:700;">{format_bytes(storage_used)}</p></div>
    </div>
    <h2 style="margin-top:40px; font-size:1.4rem; font-weight:600;">Recent Downloads</h2>
    <div class="card" style="margin-top:16px;">
        <table class="table">
            <thead><tr><th>Title</th><th>Type</th><th>Quality</th><th>Status</th><th>Date</th><th>Action</th></tr></thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    <div style="margin-top:24px; display:flex; gap:12px; flex-wrap:wrap;">
        <a href="/dashboard/api-keys" class="btn btn-outline">Manage API Keys</a>
        <a href="/dashboard/history" class="btn btn-outline">View Full History</a>
    </div>
    """
    return HTMLResponse(render_base_page(content, "Dashboard", user=user, request=request))

@app.get("/dashboard/history")
async def history_page(request: Request, user: User = Depends(get_current_user_required), page: int = 1, per_page: int = 10, search: str = "", status: str = ""):
    db = SessionLocal()
    try:
        query = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id)
        if search:
            query = query.filter(DownloadHistory.title.ilike(f"%{search}%"))
        if status:
            query = query.filter(DownloadHistory.status == status)
        total = query.count()
        total_pages = max(1, (total + per_page - 1) // per_page)
        items = query.order_by(DownloadHistory.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()

        rows = []
        for h in items:
            title_short = h.title[:80] if h.title else ''
            date_str = h.created_at.strftime('%Y-%m-%d %H:%M') if h.created_at else ''
            rows.append(f"<tr><td>{title_short}</td><td>{h.format_type}</td><td>{h.quality}</td><td><span class='badge {h.status}'>{h.status}</span></td><td>{date_str}</td><td><a href='/api/v1/download-file/{h.task_id}' class='btn btn-sm'>Download</a> <button onclick='deleteHistory(\"{h.id}\")' class='btn btn-sm btn-danger'>Delete</button></td></tr>")
        rows_html = ''.join(rows)
    except Exception as e:
        logger.error(f"History error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    pagination = ""
    for p in range(1, total_pages+1):
        pagination += f"<button class='{'active' if p==page else ''}' onclick=\"window.location.href='/dashboard/history?page={p}&search={search}&status={status}';\">{p}</button>"

    content = f"""
    <h1 style="font-size:1.8rem; font-weight:700;">Download History</h1>
    <div style="display:flex; gap:12px; margin:20px 0; flex-wrap:wrap;">
        <input type="text" id="search-input" value="{search}" placeholder="Search by title..." onkeydown="if(event.key==='Enter') window.location.href='/dashboard/history?search='+encodeURIComponent(this.value)+'&status={status}'" class="url-input" style="max-width:300px; background:var(--bg-card); border:1px solid var(--border); border-radius:var(--radius-sm); padding:10px 16px; color:var(--text-primary);">
        <select id="status-filter" onchange="window.location.href='/dashboard/history?search={search}&status='+this.value" class="btn btn-outline" style="appearance:auto; padding:10px 20px;">
            <option value="">All</option>
            <option value="completed" {'selected' if status=='completed' else ''}>Completed</option>
            <option value="failed" {'selected' if status=='failed' else ''}>Failed</option>
        </select>
    </div>

    <div class="card">
        <table class="table">
            <thead><tr><th>Title</th><th>Type</th><th>Quality</th><th>Status</th><th>Date</th><th>Action</th></tr></thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
        <div class="pagination">{pagination}</div>
    </div>

    <script>
        function deleteHistory(id) {{
            if (confirm('Are you sure you want to delete this record?')) {{
                fetch('/api/v1/history/' + id, {{ method: 'DELETE' }})
                .then(res => res.json())
                .then(data => {{
                    showToast('Deleted successfully', 'success');
                    window.location.reload();
                }})
                .catch(err => {{
                    showToast('Error deleting record', 'error');
                }});
            }}
        }}
    </script>
    """
    return HTMLResponse(render_base_page(content, "History", user=user, request=request))

@app.get("/dashboard/api-keys")
async def api_keys_page(request: Request, user: User = Depends(get_current_user_required)):
    db = SessionLocal()
    try:
        keys = db.query(ApiKey).filter(ApiKey.user_id == user.id).all()
    except Exception as e:
        logger.error(f"API keys error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    rows=[]
    for key in keys:
        created_str=key.created_at.strftime('%Y-%m-%d') if key.created_at else ''
        last_used_str=key.last_used.strftime('%Y-%m-%d %H:%M') if key.last_used else 'Never'
        rows.append(f'<tr><td>{key.name}</td><td><code>{key.prefix}…</code></td><td>{created_str}</td><td>{last_used_str}</td><td>{key.usage_count}</td><td><button onclick="revokeKey(&quot;{key.id}&quot;)" class="btn btn-sm btn-danger">Revoke</button></td></tr>')
    keys_html=''.join(rows) or '<tr><td colspan="6" class="muted">No API keys yet.</td></tr>'

    curl_example=f"""curl -X POST {PUBLIC_BASE_URL}/api/v1/analyze \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{{"url":"https://www.youtube.com/watch?v=VIDEO_ID"}}' """
    python_example=f"""import requests

BASE_URL = "{PUBLIC_BASE_URL}"
API_KEY = "YOUR_API_KEY"

r = requests.post(
    f"{{BASE_URL}}/api/v1/analyze",
    headers={{"Authorization": f"Bearer {{API_KEY}}"}},
    json={{"url": "https://www.youtube.com/watch?v=VIDEO_ID"}},
    timeout=60,
)
print(r.json())"""

    content=f"""
    <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:18px;flex-wrap:wrap;"><div><span class="eyebrow">🔑 Developer access</span><h1 style="margin:12px 0 4px;font-size:clamp(1.8rem,4vw,2.5rem);">API Keys</h1><p class="muted" style="margin:0;">Programmatic access for {PUBLIC_BASE_URL}</p></div><a class="btn btn-outline" href="/">Back to downloader</a></div>
    <div class="card" style="margin-top:22px;"><h3 style="margin-top:0;">Create a new key</h3><p class="muted">Keys are shown only once. Keep them private and never publish them in browser code or public repositories.</p><form id="create-key-form" style="display:flex;gap:10px;flex-wrap:wrap;"><input type="text" id="key-name" class="url-input" placeholder="e.g. My Telegram Bot" required style="flex:1;min-width:220px;background:var(--bg-soft);border:1px solid var(--border);border-radius:14px;padding:14px 16px;"><button type="submit" class="btn">Create API key</button></form><div id="new-key-result" class="hidden" style="margin-top:16px;"><p><strong>Copy this key now — it will not be shown again.</strong></p><div class="api-code" id="new-key-value"></div><button onclick="copyKey()" class="btn btn-outline btn-sm" style="margin-top:10px;">Copy key</button></div></div>
    <div class="card" style="margin-top:16px;"><h3 style="margin-top:0;">How to use your API key</h3><div class="stack"><div><strong>1. Base URL</strong><p class="muted">Use <code>{PUBLIC_BASE_URL}</code>. The Fly internal listener <code>{HOST}:{PORT}</code> stays private and is not the URL clients should call.</p></div><div><strong>2. Authentication</strong><p class="muted">Send <code>Authorization: Bearer YOUR_API_KEY</code> with each protected API request.</p></div><div><strong>3. Analyze</strong><p class="muted">POST JSON with <code>{{"url":"..."}}</code> to <code>/api/v1/analyze</code>.</p></div><div><strong>4. Download</strong><p class="muted">Use a returned <code>format_id</code> with <code>/api/v1/download</code>, then poll <code>/api/v1/tasks/TASK_ID/progress</code>.</p></div></div></div>
    <div class="card" style="margin-top:16px;"><h3 style="margin-top:0;">cURL example</h3><div class="code-panel"><pre>{curl_example}</pre></div><h3 style="margin:22px 0 10px;">Python example</h3><div class="code-panel"><pre>{python_example}</pre></div></div>
    <div class="card" style="margin-top:16px;"><h3 style="margin-top:0;">Existing keys</h3><div class="table-wrap"><table class="table"><thead><tr><th>Name</th><th>Prefix</th><th>Created</th><th>Last used</th><th>Usage</th><th>Action</th></tr></thead><tbody>{keys_html}</tbody></table></div></div>
    <div class="card" style="margin-top:16px;"><strong>Usage & rights</strong><p class="muted" style="margin-bottom:0;">Use the API only for media you are authorized to download. Keep API keys confidential, respect rate limits, and comply with the terms of the relevant media platform and applicable law.</p></div>
    <script>
        document.getElementById('create-key-form').addEventListener('submit',async(e)=>{{e.preventDefault();const name=document.getElementById('key-name').value.trim();if(!name)return;try{{const r=await fetch('/api/v1/api-keys',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{name}})}});const d=await r.json();if(!r.ok)throw new Error(d.detail||'Error creating key');document.getElementById('new-key-value').textContent=d.key;document.getElementById('new-key-result').classList.remove('hidden');showToast('API key created','success')}}catch(err){{showToast(err.message||'Network error','error')}}}});
        function copyKey(){{const text=document.getElementById('new-key-value').textContent;navigator.clipboard.writeText(text).then(()=>showToast('Copied','success')).catch(()=>showToast('Copy failed','error'))}}
        function revokeKey(id){{if(!confirm('Revoke this API key?'))return;fetch('/api/v1/api-keys/'+id,{{method:'DELETE'}}).then(async r=>{{const d=await r.json();if(!r.ok)throw new Error(d.detail||'Error');showToast('Key revoked','success');setTimeout(()=>location.reload(),500)}}).catch(err=>showToast(err.message||'Error','error'))}}
    </script>
    """
    return HTMLResponse(render_base_page(content,"API Keys",user=user,request=request))

@app.get("/admin")
async def admin_page(request: Request, user: User = Depends(get_current_user_required)):
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="FORBIDDEN")
    db = SessionLocal()
    try:
        total_users = db.query(User).count()
        active_tasks = db.query(DownloadTask).filter(DownloadTask.status.in_(["downloading", "starting", "queued"])).count()
        downloads = db.query(DownloadHistory).count()
        failed_downloads = db.query(DownloadHistory).filter(DownloadHistory.status == "failed").count()
        api_requests = db.query(UsageRecord).count()
        storage_used = db.query(DownloadHistory).with_entities(func.sum(DownloadHistory.file_size)).scalar() or 0
        logs = db.query(SystemLog).order_by(SystemLog.created_at.desc()).limit(50).all()

        rows = []
        for log in logs:
            time_str = log.created_at.strftime('%H:%M:%S') if log.created_at else ''
            rows.append(f"<tr><td>{time_str}</td><td>{log.level}</td><td>{log.message}</td><td>{log.endpoint}</td></tr>")
        rows_html = ''.join(rows)
    except Exception as e:
        logger.error(f"Admin error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    content = f"""
    <h1 style="font-size:1.8rem; font-weight:700;">Admin Panel</h1>
    <p style="color:var(--text-secondary); margin-bottom:24px;">System overview and monitoring.</p>
    <div class="grid">
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Total Users</h3><p style="font-size:2.2rem; font-weight:700;">{total_users}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Active Tasks</h3><p style="font-size:2.2rem; font-weight:700;">{active_tasks}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Total Downloads</h3><p style="font-size:2.2rem; font-weight:700;">{downloads}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Failed Downloads</h3><p style="font-size:2.2rem; font-weight:700; color:#e74c3c;">{failed_downloads}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">API Requests</h3><p style="font-size:2.2rem; font-weight:700;">{api_requests}</p></div>
        <div class="card"><h3 style="color:var(--text-secondary); font-size:0.9rem; font-weight:500;">Storage Used</h3><p style="font-size:2.2rem; font-weight:700;">{format_bytes(storage_used)}</p></div>
    </div>
    <h2 style="margin-top:40px; font-size:1.4rem; font-weight:600;">System Logs</h2>
    <div class="card" style="margin-top:16px; max-height:400px; overflow-y:auto;">
        <table class="table">
            <thead><tr><th>Time</th><th>Level</th><th>Message</th><th>Endpoint</th></tr></thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """
    return HTMLResponse(render_base_page(content, "Admin", user=user, request=request))

# ------------------------------
# API Routes (modified to use VidsSave)
# ------------------------------
@app.post("/api/v1/analyze")
async def analyze_video(request: Request, user: User = Depends(get_current_user_required_api)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    url = body.get("url")
    if not url:
        raise HTTPException(status_code=400, detail="INVALID_URL")

    valid, error_code, video_id = validate_youtube_url(url)
    if not valid:
        raise HTTPException(status_code=400, detail=error_code)

    # Try VidsSave first if enabled
    if VIDSSAVE_ENABLED and vidsave_provider:
        try:
            result = await vidsave_provider.parse(url)
            # Convert VidsSave formats to our format
            video_formats = []
            audio_formats = []
            for fmt in result.get("formats", []):
                if fmt.get("type") == "video":
                    video_formats.append({
                        "format_id": fmt.get("format_id"),
                        "ext": fmt.get("format", "mp4"),
                        "resolution": fmt.get("quality"),
                        "height": None,
                        "width": None,
                        "fps": None,
                        "vcodec": None,
                        "acodec": None,
                        "filesize": fmt.get("size"),
                        "tbr": None,
                        "format_note": "",
                        "download_url": fmt.get("download_url"),
                    })
                else:
                    audio_formats.append({
                        "format_id": fmt.get("format_id"),
                        "ext": fmt.get("format", "mp3"),
                        "abr": fmt.get("quality"),
                        "acodec": None,
                        "filesize": fmt.get("size"),
                        "format_note": "",
                        "download_url": fmt.get("download_url"),
                    })

            logger.info(f"[VidsSave] Parse successful. Formats: {[f['format_id'] for f in video_formats + audio_formats]}")
            return JSONResponse({
                "video_id": result.get("video_id") or video_id,
                "title": result.get("title", "Untitled"),
                "channel": "Unknown",
                "duration": result.get("duration", 0),
                "thumbnail": result.get("thumbnail", ""),
                "video_formats": video_formats,
                "audio_formats": audio_formats,
                "source": "vidsave",
            })
        except VidsSaveError as e:
            logger.warning(f"VidsSave failed: {e}. Falling back to yt-dlp.")
        except Exception as e:
            logger.error(f"Unexpected error in VidsSave: {e}")

    # Fallback to yt-dlp
    logger.info("Falling back to yt-dlp for video info.")
    info = fetch_video_info(url)
    title = info.get("title", "Unknown Title")
    channel = info.get("uploader") or info.get("channel") or "Unknown Channel"
    duration = info.get("duration", 0)
    thumbnail = info.get("thumbnail", "")
    formats = filter_direct_formats(info)

    return JSONResponse({
        "video_id": video_id,
        "title": title,
        "channel": channel,
        "duration": duration,
        "thumbnail": thumbnail,
        "video_formats": formats["video_formats"],
        "audio_formats": formats["audio_formats"],
        "source": "ytdlp",
    })

@app.post("/api/v1/download")
async def create_download(request: Request, user: User = Depends(get_current_user_required_api)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    url = body.get("url")
    format_id = body.get("format_id")
    format_type = body.get("format_type", "video")
    source = body.get("source", "ytdlp")  # Default to yt-dlp if not provided

    if not url or not format_id:
        raise HTTPException(status_code=400, detail="INVALID_URL_OR_FORMAT")

    valid, error_code, video_id = validate_youtube_url(url)
    if not valid:
        raise HTTPException(status_code=400, detail=error_code)

    download_url = None
    content_type = None
    title = "Unknown"

    if source == "vidsave":
        # Use VidsSave to get download URL
        if not VIDSSAVE_ENABLED or not vidsave_provider:
            raise HTTPException(status_code=503, detail="VidsSave not available")

        try:
            result = await vidsave_provider.parse(url)
            title = result.get("title", "Unknown")
            # Find the matching format by format_id
            for fmt in result.get("formats", []):
                if fmt.get("format_id") == format_id:
                    download_url = fmt.get("download_url")
                    content_type = "video/mp4" if fmt.get("type") == "video" else "audio/mpeg"
                    break
            if not download_url:
                # Fallback: try to match by type and quality
                for fmt in result.get("formats", []):
                    if fmt.get("type") == format_type and fmt.get("quality") == format_id.split('_')[1]:
                        download_url = fmt.get("download_url")
                        content_type = "video/mp4" if fmt.get("type") == "video" else "audio/mpeg"
                        break
            if not download_url:
                raise HTTPException(status_code=404, detail="Format not found in VidsSave response")
            logger.info(f"[VidsSave] Selected format: {format_id}, download URL found")
        except VidsSaveError as e:
            logger.error(f"[VidsSave] Error during download preparation: {e}")
            raise HTTPException(status_code=502, detail=f"VidsSave error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in VidsSave download prep: {e}")
            raise HTTPException(status_code=500, detail="Internal error")
    else:
        # Use yt-dlp as fallback
        logger.info(f"Using yt-dlp for format {format_id}")
        info = fetch_video_info(url)
        title = info.get("title", "Unknown")
        formats = filter_direct_formats(info)
        all_formats = formats["video_formats"] + formats["audio_formats"]
        format_match = next((f for f in all_formats if f["format_id"] == format_id), None)
        if not format_match:
            raise HTTPException(status_code=400, detail="FORMAT_NOT_AVAILABLE")

    db = SessionLocal()
    try:
        task = create_download_task(
            db=db,
            user_id=user.id,
            url=url,
            format_id=format_id,
            format_type=format_type,
            title=title,
            video_id=video_id
        )
        task_id = task.id
        # Start download worker with download_url if available (only for vidsave)
        start_download_task(task_id, download_url=download_url, content_type=content_type)
        return JSONResponse({"task_id": task_id, "status": "queued"})
    except Exception as e:
        logger.error(f"Failed to create task: {e}")
        raise HTTPException(status_code=500, detail="DOWNLOAD_FAILED")
    finally:
        db.close()

@app.get("/api/v1/tasks/{task_id}")
async def get_task_status(task_id: str, user: User = Depends(get_current_user_required_api)):
    db = SessionLocal()
    try:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id, DownloadTask.user_id == user.id).first()
        if not task:
            raise HTTPException(status_code=404, detail="TASK_NOT_FOUND")
        return JSONResponse({
            "task_id": task.id,
            "status": task.status,
            "progress": task.progress,
            "downloaded_bytes": task.downloaded_bytes,
            "total_bytes": task.total_bytes,
            "speed": task.speed,
            "eta": task.eta,
            "error": task.error_message,
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get task status error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/api/v1/tasks/{task_id}/progress")
async def get_task_progress(task_id: str, user: User = Depends(get_current_user_required_api)):
    with download_lock:
        if task_id in active_downloads:
            return JSONResponse(active_downloads[task_id])
    db = SessionLocal()
    try:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id, DownloadTask.user_id == user.id).first()
        if not task:
            raise HTTPException(status_code=404, detail="TASK_NOT_FOUND")
        return JSONResponse({
            "status": task.status,
            "progress": task.progress,
            "downloaded_bytes": task.downloaded_bytes,
            "total_bytes": task.total_bytes,
            "speed": task.speed,
            "eta": task.eta,
            "error": task.error_message,
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get task progress error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/api/v1/download-file/{task_id}")
async def download_file(task_id: str, user: User = Depends(get_current_user_required_api)):
    db = SessionLocal()
    try:
        task = db.query(DownloadTask).filter(DownloadTask.id == task_id, DownloadTask.user_id == user.id).first()
        if not task or not task.file_path:
            raise HTTPException(status_code=404, detail="FILE_NOT_FOUND")
        if not os.path.exists(task.file_path):
            raise HTTPException(status_code=404, detail="FILE_EXPIRED")
        # Determine content type from file extension
        file_path = Path(task.file_path)
        if file_path.suffix == ".mp3":
            media_type = "audio/mpeg"
        elif file_path.suffix == ".mp4":
            media_type = "video/mp4"
        else:
            media_type = "application/octet-stream"
        return FileResponse(path=task.file_path, filename=file_path.name, media_type=media_type)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download file error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/api/v1/history")
async def get_history(user: User = Depends(get_current_user_required_api), page: int = 1, per_page: int = 10, search: str = "", status: str = ""):
    db = SessionLocal()
    try:
        query = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id)
        if search:
            query = query.filter(DownloadHistory.title.ilike(f"%{search}%"))
        if status:
            query = query.filter(DownloadHistory.status == status)
        total = query.count()
        items = query.order_by(DownloadHistory.created_at.desc()).offset((page-1)*per_page).limit(per_page).all()
    except Exception as e:
        logger.error(f"Get history error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    return JSONResponse({
        "total": total,
        "page": page,
        "per_page": per_page,
        "items": [
            {
                "id": h.id,
                "title": h.title,
                "format_type": h.format_type,
                "quality": h.quality,
                "status": h.status,
                "file_size": h.file_size,
                "created_at": h.created_at.isoformat(),
                "download_url": f"/api/v1/download-file/{h.task_id}" if h.task_id else None,
            }
            for h in items
        ]
    })

@app.delete("/api/v1/history/{history_id}")
async def delete_history_entry(history_id: str, user: User = Depends(get_current_user_required_api)):
    db = SessionLocal()
    try:
        entry = db.query(DownloadHistory).filter(DownloadHistory.id == history_id, DownloadHistory.user_id == user.id).first()
        if entry:
            db.delete(entry)
            db.commit()
            return JSONResponse({"status": "deleted"})
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete history error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.get("/api/v1/me")
async def get_me(user: User = Depends(get_current_user_required_api)):
    return JSONResponse({
        "id": user.id,
        "email": user.email,
        "phone": user.phone,
        "username": user.username,
        "name": user.name,
        "avatar_url": user.avatar_url,
        "is_admin": user.is_admin,
        "is_email_verified": user.is_email_verified,
        "is_phone_verified": user.is_phone_verified,
        "created_at": user.created_at.isoformat(),
    })

@app.get("/api/v1/usage")
async def get_usage(user: User = Depends(get_current_user_required_api)):
    db = SessionLocal()
    try:
        total_downloads = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id).count()
        api_requests = db.query(UsageRecord).filter(UsageRecord.user_id == user.id).count()
        storage_used = db.query(DownloadHistory).filter(DownloadHistory.user_id == user.id).with_entities(func.sum(DownloadHistory.file_size)).scalar() or 0
    except Exception as e:
        logger.error(f"Get usage error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

    return JSONResponse({
        "total_downloads": total_downloads,
        "api_requests": api_requests,
        "storage_used": storage_used,
        "limits": {
            "max_file_size": MAX_FILE_SIZE,
            "rate_limit": RATE_LIMIT,
            "window_seconds": RATE_LIMIT_WINDOW,
        }
    })

@app.post("/api/v1/api-keys")
async def create_api_key(request: Request, user: User = Depends(get_current_user_required)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    name = body.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="NAME_REQUIRED")
    prefix, raw_key, hashed = generate_api_key()
    db = SessionLocal()
    try:
        new_key = ApiKey(user_id=user.id, name=name, key_hash=hashed, prefix=prefix)
        db.add(new_key)
        db.commit()
        db.refresh(new_key)
        return JSONResponse({"id": new_key.id, "name": name, "prefix": prefix, "key": raw_key})
    except Exception as e:
        logger.error(f"Create API key error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

@app.delete("/api/v1/api-keys/{key_id}")
async def revoke_api_key(key_id: str, user: User = Depends(get_current_user_required)):
    db = SessionLocal()
    try:
        key = db.query(ApiKey).filter(ApiKey.id == key_id, ApiKey.user_id == user.id).first()
        if key:
            key.is_active = False
            db.commit()
            return JSONResponse({"status": "revoked"})
        raise HTTPException(status_code=404, detail="NOT_FOUND")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Revoke API key error: {e}")
        raise HTTPException(status_code=500, detail="Internal error")
    finally:
        db.close()

# ------------------------------
# Health Endpoints
# ------------------------------
@app.get("/health")
async def health():
    db_status = "ok"
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
    except Exception:
        db_status = "error"
    storage_status = "ok" if os.access(DOWNLOAD_DIR, os.W_OK) else "error"
    return JSONResponse({
        "status": "ok",
        "database": db_status,
        "storage": storage_status,
        "public_url": PUBLIC_BASE_URL,
    })

@app.get("/ready")
async def ready():
    db_ok = True
    try:
        db = SessionLocal()
        db.execute("SELECT 1")
        db.close()
    except:
        db_ok = False
    storage_ok = os.access(DOWNLOAD_DIR, os.W_OK)
    if db_ok and storage_ok:
        return JSONResponse({"status": "ready"})
    else:
        return JSONResponse(status_code=503, content={"status": "not_ready", "database": db_ok, "storage": storage_ok})

# ------------------------------
# Root page
# ------------------------------
@app.get("/")
async def home(request: Request):
    db = SessionLocal()
    try:
        user = get_current_user(request, db)
        return HTMLResponse(render_home_page(request, user))
    finally:
        db.close()

# ------------------------------
# Main
# ------------------------------
if __name__ == "__main__":
    print("=" * 50)
    print(f"🚀 {APP_NAME} is starting...")
    print("=" * 50)
    print(f"📍 Local: http://127.0.0.1:{PORT}")
    print(f"📍 Network: http://{HOST}:{PORT}")
    print(f"🌐 Public: {PUBLIC_BASE_URL}")
    print("=" * 50)

    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        workers=1,
        log_level="info",
        access_log=True,
    )
