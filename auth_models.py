from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


@dataclass
class User:
    """User representation stored in the local auth store."""

    user_id: str
    username: str
    email: str
    full_name: str
    roles: List[str] = field(default_factory=lambda: ["user"])
    password_hash: str = ""
    is_active: bool = True
    must_change_password: bool = False
    created_at: str = field(default_factory=utc_now_iso)
    last_login_at: Optional[str] = None
    password_last_changed_at: Optional[str] = None
    schema_version: str = "1.0"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "User":
        return cls(
            user_id=str(data.get("user_id", "")),
            username=str(data.get("username", "")),
            email=str(data.get("email", "")),
            full_name=str(data.get("full_name", data.get("username", ""))),
            roles=list(data.get("roles", [])),
            password_hash=str(data.get("password_hash", "")),
            is_active=bool(data.get("is_active", True)),
            must_change_password=bool(data.get("must_change_password", False)),
            created_at=str(data.get("created_at", utc_now_iso())),
            last_login_at=data.get("last_login_at"),
            password_last_changed_at=data.get("password_last_changed_at"),
            schema_version=str(data.get("schema_version", "1.0")),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "email": self.email,
            "full_name": self.full_name,
            "roles": list(self.roles),
            "password_hash": self.password_hash,
            "is_active": self.is_active,
            "must_change_password": self.must_change_password,
            "created_at": self.created_at,
            "last_login_at": self.last_login_at,
            "password_last_changed_at": self.password_last_changed_at,
            "schema_version": self.schema_version,
        }


@dataclass
class PasswordResetToken:
    token: str
    user_id: str
    expires_at: str
    used: bool = False
    admin_user_id: Optional[str] = None
    created_at: str = field(default_factory=utc_now_iso)
    used_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PasswordResetToken":
        return cls(
            token=str(data.get("token", "")),
            user_id=str(data.get("user_id", "")),
            expires_at=str(data.get("expires_at", "")),
            used=bool(data.get("used", False)),
            admin_user_id=data.get("admin_user_id"),
            created_at=str(data.get("created_at", utc_now_iso())),
            used_at=data.get("used_at"),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token": self.token,
            "user_id": self.user_id,
            "expires_at": self.expires_at,
            "used": self.used,
            "admin_user_id": self.admin_user_id,
            "created_at": self.created_at,
            "used_at": self.used_at,
        }
