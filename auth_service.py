from __future__ import annotations

import secrets
import string
from datetime import datetime, timedelta
from typing import List, Optional

import bcrypt

from auth_models import PasswordResetToken, User, utc_now_iso
from auth_user_store import AuthUserStore

PASSWORD_MIN_LENGTH = 8


class AuthService:
    """Authentication and user-management service built on AuthUserStore."""

    def __init__(self, store: Optional[AuthUserStore] = None) -> None:
        self.store = store or AuthUserStore()
        self.bootstrap_admin()

    # ------------ Helpers ------------
    def is_admin(self, user: Optional[User]) -> bool:
        return bool(user and "admin" in user.roles)

    def _hash_password(self, password: str) -> str:
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    def _generate_token_value(self) -> str:
        alphabet = string.ascii_uppercase + string.digits
        parts = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)]
        return "-".join(parts)

    def _validate_password(self, password: str) -> Optional[str]:
        if len(password) < PASSWORD_MIN_LENGTH:
            return f"Heslo musí mít alespoň {PASSWORD_MIN_LENGTH} znaků."
        return None

    # ------------ Bootstrap ------------
    def bootstrap_admin(self) -> None:
        admins = [user for user in self.store.list_users() if "admin" in user.roles]
        if admins:
            return
        env_username = None
        env_password_hash = None
        try:
            import os

            env_username = os.environ.get("ADMIN_USERNAME")
            env_password_hash = os.environ.get("ADMIN_PASSWORD_HASH")
        except Exception:
            pass
        if env_username and env_password_hash:
            new_user = User(
                user_id=f"u_{secrets.token_hex(6)}",
                username=env_username,
                email="admin@example.com",
                full_name="Administrátor",
                roles=["admin"],
                password_hash=env_password_hash,
                is_active=True,
                must_change_password=False,
                password_last_changed_at=utc_now_iso(),
            )
            self.store.create_user(new_user)
            self.store.log_event(event_type="bootstrap_admin", user_id=new_user.user_id)

    # ------------ Public API ------------
    def list_users(self) -> List[User]:
        return self.store.list_users()

    def authenticate(self, username_or_email: str, password: str) -> Optional[User]:
        identifier = username_or_email.strip().lower()
        user = self.store.get_user_by_username(identifier) or self.store.get_user_by_email(identifier)
        if not user:
            self.store.log_event(event_type="login_failure", detail=f"user={identifier}")
            return None
        if not user.is_active:
            self.store.log_event(event_type="login_inactive", user_id=user.user_id)
            return None
        try:
            if not bcrypt.checkpw(password.encode("utf-8"), user.password_hash.encode("utf-8")):
                self.store.log_event(event_type="login_failure", user_id=user.user_id)
                return None
        except ValueError:
            self.store.log_event(event_type="login_failure", user_id=user.user_id, detail="invalid_hash")
            return None

        user.last_login_at = utc_now_iso()
        self.store.update_user(user)
        self.store.log_event(event_type="login_success", user_id=user.user_id)
        return user

    def create_user(
        self,
        *,
        full_name: str,
        username: str,
        email: str,
        password: str,
        roles: Optional[List[str]] = None,
        is_active: bool = True,
        must_change_password: bool = False,
    ) -> User:
        username = username.strip()
        email = email.strip()
        if self.store.get_user_by_username(username):
            raise ValueError("Uživatelské jméno je již obsazené.")
        if self.store.get_user_by_email(email):
            raise ValueError("E-mail je již zaregistrován.")

        validation_error = self._validate_password(password)
        if validation_error:
            raise ValueError(validation_error)

        roles = roles or ["user"]
        if not self.store.list_users():
            # First user fallback
            if "admin" not in roles:
                roles.append("admin")
        user = User(
            user_id=f"u_{secrets.token_hex(8)}",
            username=username,
            email=email,
            full_name=full_name.strip() or username,
            roles=roles,
            password_hash=self._hash_password(password),
            is_active=is_active,
            must_change_password=must_change_password,
            password_last_changed_at=utc_now_iso(),
        )
        self.store.create_user(user)
        self.store.log_event(event_type="user_created", user_id=user.user_id)
        return user

    def change_password(self, user: User, old_password: str, new_password: str) -> bool:
        stored = self.store.get_user_by_id(user.user_id)
        if not stored:
            return False
        try:
            if not bcrypt.checkpw(old_password.encode("utf-8"), stored.password_hash.encode("utf-8")):
                self.store.log_event(event_type="password_change_failed", user_id=user.user_id)
                return False
        except ValueError:
            return False
        validation_error = self._validate_password(new_password)
        if validation_error:
            raise ValueError(validation_error)
        stored.password_hash = self._hash_password(new_password)
        stored.password_last_changed_at = utc_now_iso()
        stored.must_change_password = False
        self.store.update_user(stored)
        self.store.log_event(event_type="password_changed_user", user_id=user.user_id)
        return True

    def admin_set_password(self, admin: User, target_user_id: str, new_password: str) -> bool:
        if not self.is_admin(admin):
            self.store.log_event(
                event_type="admin_action_denied",
                admin_user_id=admin.user_id,
                target_user_id=target_user_id,
                detail="admin_set_password",
            )
            return False
        target = self.store.get_user_by_id(target_user_id)
        if not target:
            return False
        validation_error = self._validate_password(new_password)
        if validation_error:
            raise ValueError(validation_error)
        target.password_hash = self._hash_password(new_password)
        target.must_change_password = True
        target.password_last_changed_at = utc_now_iso()
        self.store.update_user(target)
        self.store.log_event(
            event_type="password_reset_admin",
            admin_user_id=admin.user_id,
            target_user_id=target_user_id,
        )
        return True

    def generate_reset_token(self, admin: User, target_user_id: str, minutes_valid: int = 60) -> Optional[PasswordResetToken]:
        if not self.is_admin(admin):
            self.store.log_event(
                event_type="admin_action_denied",
                admin_user_id=admin.user_id,
                target_user_id=target_user_id,
                detail="generate_reset_token",
            )
            return None
        if not self.store.get_user_by_id(target_user_id):
            return None
        token_value = self._generate_token_value()
        expires_at = (datetime.utcnow() + timedelta(minutes=minutes_valid)).replace(microsecond=0).isoformat() + "Z"
        token = PasswordResetToken(
            token=token_value,
            user_id=target_user_id,
            expires_at=expires_at,
            admin_user_id=admin.user_id,
        )
        self.store.save_token(token)
        self.store.log_event(
            event_type="reset_token_created",
            admin_user_id=admin.user_id,
            target_user_id=target_user_id,
            detail=f"expires_at={expires_at}",
        )
        return token

    def reset_password_with_token(self, username: str, token_value: str, new_password: str) -> bool:
        user = self.store.get_user_by_username(username)
        if not user:
            return False
        token = self.store.get_token(token_value)
        if not token or token.user_id != user.user_id or token.used:
            return False
        try:
            expires_at = datetime.fromisoformat(token.expires_at.replace("Z", "+00:00"))
        except ValueError:
            return False
        if datetime.utcnow() > expires_at:
            return False
        validation_error = self._validate_password(new_password)
        if validation_error:
            raise ValueError(validation_error)
        user.password_hash = self._hash_password(new_password)
        user.must_change_password = False
        user.password_last_changed_at = utc_now_iso()
        self.store.update_user(user)
        token.used = True
        token.used_at = utc_now_iso()
        self.store.save_token(token)
        self.store.log_event(
            event_type="password_reset_token_used",
            user_id=user.user_id,
            admin_user_id=token.admin_user_id,
            target_user_id=user.user_id,
        )
        return True
