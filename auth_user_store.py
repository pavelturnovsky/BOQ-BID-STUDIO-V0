from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Optional

from auth_models import PasswordResetToken, User, utc_now_iso

DEFAULT_STORAGE_DIR = Path.home() / ".boq_bid_studio"


class AuthUserStore:
    """Simple JSON-backed user store with reset token tracking."""

    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else DEFAULT_STORAGE_DIR
        self.global_dir = (self.base_dir / "global").expanduser()
        self.global_dir.mkdir(parents=True, exist_ok=True)
        self.users_path = self.global_dir / "users.json"
        self.tokens_path = self.global_dir / "password_reset_tokens.json"
        self.audit_log_path = self.global_dir / "auth_audit_log.jsonl"
        self._users: List[User] = self._load_users()
        self._tokens: List[PasswordResetToken] = self._load_tokens()

    # ------------ Persistence helpers ------------
    def _load_users(self) -> List[User]:
        if self.users_path.exists():
            try:
                raw = json.loads(self.users_path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    return [User.from_dict(item) for item in raw]
            except (OSError, json.JSONDecodeError):
                pass
        return []

    def _save_users(self) -> None:
        data = [user.to_dict() for user in self._users]
        self.users_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_tokens(self) -> List[PasswordResetToken]:
        if self.tokens_path.exists():
            try:
                raw = json.loads(self.tokens_path.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    return [PasswordResetToken.from_dict(item) for item in raw]
            except (OSError, json.JSONDecodeError):
                pass
        return []

    def _save_tokens(self) -> None:
        data = [token.to_dict() for token in self._tokens]
        self.tokens_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ------------ User helpers ------------
    def list_users(self) -> List[User]:
        return list(self._users)

    def get_user_by_username(self, username: str) -> Optional[User]:
        normalized = username.lower()
        for user in self._users:
            if user.username.lower() == normalized:
                return user
        return None

    def get_user_by_email(self, email: str) -> Optional[User]:
        normalized = email.lower()
        for user in self._users:
            if user.email.lower() == normalized:
                return user
        return None

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        for user in self._users:
            if user.user_id == user_id:
                return user
        return None

    def create_user(self, user: User) -> User:
        self._users.append(user)
        self._save_users()
        return user

    def update_user(self, updated: User) -> User:
        self._users = [user if user.user_id != updated.user_id else updated for user in self._users]
        self._save_users()
        return updated

    # ------------ Token helpers ------------
    def list_tokens_for_user(self, user_id: str) -> List[PasswordResetToken]:
        return [token for token in self._tokens if token.user_id == user_id]

    def get_token(self, token_value: str) -> Optional[PasswordResetToken]:
        for token in self._tokens:
            if token.token == token_value:
                return token
        return None

    def save_token(self, token: PasswordResetToken) -> PasswordResetToken:
        existing = self.get_token(token.token)
        if existing:
            self._tokens = [token if t.token == token.token else t for t in self._tokens]
        else:
            self._tokens.append(token)
        self._save_tokens()
        return token

    # ------------ Audit log ------------
    def log_event(
        self,
        *,
        event_type: str,
        user_id: Optional[str] = None,
        admin_user_id: Optional[str] = None,
        target_user_id: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> None:
        entry = {
            "timestamp": utc_now_iso(),
            "event_type": event_type,
            "user_id": user_id,
            "admin_user_id": admin_user_id,
            "target_user_id": target_user_id,
            "detail": detail,
        }
        with self.audit_log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def recent_events(self, limit: int = 50) -> List[dict]:
        if not self.audit_log_path.exists():
            return []
        lines: List[dict] = []
        try:
            with self.audit_log_path.open("r", encoding="utf-8") as f:
                for line in f.readlines()[-limit:]:
                    try:
                        lines.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            return []
        return lines
