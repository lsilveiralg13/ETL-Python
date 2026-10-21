from __future__ import annotations

import os
import secrets
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

GITHUB_AUTHORIZE_URL = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL = "https://github.com/login/oauth/access_token"
GITHUB_USER_URL = "https://api.github.com/user"
STATE_TTL_SECONDS = 600
SESSION_TTL_SECONDS = 12 * 60 * 60
MAX_PENDING_STATES = 1024
MAX_ACTIVE_SESSIONS = 16


@dataclass
class OAuthState:
    expires_at: float


@dataclass
class CopilotSession:
    login: str
    access_token: str
    expires_at: float
    refresh_token: str | None
    access_expires_at: float | None


class GitHubCopilotAuth:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._states: dict[str, OAuthState] = {}
        self._sessions: dict[str, CopilotSession] = {}

    @staticmethod
    def is_configured() -> bool:
        return all(
            os.environ.get(name, "").strip()
            for name in (
                "GITHUB_CLIENT_ID",
                "GITHUB_CLIENT_SECRET",
                "VETRA_GITHUB_REDIRECT_URI",
                "VETRA_GITHUB_ALLOWED_USER",
            )
        )

    def create_authorization_url(self) -> str:
        if not self.is_configured():
            raise RuntimeError("O login GitHub da Vetra não está configurado.")
        state = secrets.token_urlsafe(32)
        now = time.monotonic()
        with self._lock:
            self._states = {
                key: value
                for key, value in self._states.items()
                if value.expires_at > now
            }
            if len(self._states) >= MAX_PENDING_STATES:
                raise RuntimeError("Há muitas tentativas de login em andamento.")
            self._states[state] = OAuthState(now + STATE_TTL_SECONDS)
        return GITHUB_AUTHORIZE_URL + "?" + urlencode(
            {
                "client_id": os.environ["GITHUB_CLIENT_ID"].strip(),
                "redirect_uri": os.environ["VETRA_GITHUB_REDIRECT_URI"].strip(),
                "scope": "read:user",
                "state": state,
            }
        )

    def complete_login(self, code: str, state: str) -> tuple[str, str]:
        if not self.is_configured():
            raise RuntimeError("O login GitHub da Vetra não está configurado.")
        if not code or not state:
            raise ValueError("A resposta de autenticação do GitHub está incompleta.")

        now = time.monotonic()
        with self._lock:
            oauth_state = self._states.pop(state, None)
        if oauth_state is None or oauth_state.expires_at <= now:
            raise ValueError("O estado de autenticação expirou ou já foi utilizado.")

        token_response = requests.post(
            GITHUB_TOKEN_URL,
            headers={"Accept": "application/json"},
            data={
                "client_id": os.environ["GITHUB_CLIENT_ID"].strip(),
                "client_secret": os.environ["GITHUB_CLIENT_SECRET"].strip(),
                "code": code,
                "redirect_uri": os.environ["VETRA_GITHUB_REDIRECT_URI"].strip(),
            },
            timeout=(5, 15),
        )
        token_response.raise_for_status()
        token_data = token_response.json()
        access_token = str(token_data.get("access_token") or "")
        if not access_token or token_data.get("error"):
            raise ValueError("O GitHub não autorizou o acesso do Copilot.")

        user_response = requests.get(
            GITHUB_USER_URL,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {access_token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=(5, 15),
        )
        user_response.raise_for_status()
        login = str(user_response.json().get("login") or "")
        allowed_user = os.environ["VETRA_GITHUB_ALLOWED_USER"].strip()
        if not login or login.casefold() != allowed_user.casefold():
            raise PermissionError("Esta conta GitHub não tem acesso à Vetra.")

        expires_in = token_data.get("expires_in")
        access_expires_at = (
            now + max(0, int(expires_in) - 60)
            if expires_in is not None
            else None
        )
        session_id = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions = {
                key: value
                for key, value in self._sessions.items()
                if value.expires_at > now
            }
            while len(self._sessions) >= MAX_ACTIVE_SESSIONS:
                self._sessions.pop(next(iter(self._sessions)))
            self._sessions[session_id] = CopilotSession(
                login=login,
                access_token=access_token,
                expires_at=now + SESSION_TTL_SECONDS,
                refresh_token=token_data.get("refresh_token"),
                access_expires_at=access_expires_at,
            )
        return session_id, login

    def get_access_token(self, session_id: str) -> tuple[str, str] | None:
        now = time.monotonic()
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                return None
            if session.expires_at <= now:
                del self._sessions[session_id]
                return None
            refresh_required = (
                session.access_expires_at is not None
                and session.access_expires_at <= now
            )
            if not refresh_required:
                return session.access_token, session.login
            refresh_token = session.refresh_token

        if not refresh_token:
            self.logout(session_id)
            return None

        response = requests.post(
            GITHUB_TOKEN_URL,
            headers={"Accept": "application/json"},
            data={
                "client_id": os.environ["GITHUB_CLIENT_ID"].strip(),
                "client_secret": os.environ["GITHUB_CLIENT_SECRET"].strip(),
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=(5, 15),
        )
        response.raise_for_status()
        token_data = response.json()
        access_token = str(token_data.get("access_token") or "")
        if not access_token or token_data.get("error"):
            self.logout(session_id)
            return None

        expires_in = token_data.get("expires_in")
        with self._lock:
            current = self._sessions.get(session_id)
            if current is None or current.expires_at <= time.monotonic():
                return None
            current.access_token = access_token
            current.refresh_token = token_data.get("refresh_token") or refresh_token
            current.access_expires_at = (
                time.monotonic() + max(0, int(expires_in) - 60)
                if expires_in is not None
                else None
            )
            return current.access_token, current.login

    def logout(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)
