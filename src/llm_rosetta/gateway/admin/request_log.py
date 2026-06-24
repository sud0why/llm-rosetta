"""Request log for the gateway admin panel.

Delegates to SQLite persistence when available, falls back to an
in-memory ring buffer otherwise.
"""

from __future__ import annotations

import contextvars
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .persistence import PersistenceManager

# Per-request detailed log data — set by proxy handler, read by _record_telemetry.
request_detail_var: contextvars.ContextVar[dict[str, Any] | None] = (
    contextvars.ContextVar("request_detail", default=None)
)

# Deferred log fields for streaming requests (logged after the stream ends).
pending_stream_log_var: contextvars.ContextVar[dict[str, Any] | None] = (
    contextvars.ContextVar("pending_stream_log", default=None)
)


@dataclass(frozen=True)
class RequestLogEntry:
    """A single logged proxy request."""

    id: str
    timestamp: str  # ISO 8601
    model: str
    source_provider: str
    target_provider: str
    is_stream: bool
    status_code: int
    duration_ms: float
    error_detail: str | None = None
    api_key_label: str | None = None
    target_provider_name: str | None = None
    client_ip: str | None = None
    request_path: str | None = None
    request_method: str | None = None
    # Detailed request/response bodies and headers (for debug view)
    request_body: dict[str, Any] | None = None
    request_headers: dict[str, str] | None = None
    response_body: dict[str, Any] | None = None
    response_headers: dict[str, str] | None = None
    upstream_request_body: dict[str, Any] | None = None
    upstream_response_body: dict[str, Any] | None = None
    upstream_request_headers: dict[str, str] | None = None
    upstream_response_headers: dict[str, str] | None = None
    upstream_url: str | None = None

    @classmethod
    def create(
        cls,
        *,
        model: str,
        source_provider: str,
        target_provider: str,
        is_stream: bool,
        status_code: int,
        duration_ms: float,
        error_detail: str | None = None,
        api_key_label: str | None = None,
        target_provider_name: str | None = None,
        client_ip: str | None = None,
        request_path: str | None = None,
        request_method: str | None = None,
        request_body: dict[str, Any] | None = None,
        request_headers: dict[str, str] | None = None,
        response_body: dict[str, Any] | None = None,
        response_headers: dict[str, str] | None = None,
        upstream_request_body: dict[str, Any] | None = None,
        upstream_response_body: dict[str, Any] | None = None,
        upstream_request_headers: dict[str, str] | None = None,
        upstream_response_headers: dict[str, str] | None = None,
        upstream_url: str | None = None,
    ) -> RequestLogEntry:
        """Factory with auto-generated id and timestamp."""
        return cls(
            id=uuid.uuid4().hex,
            timestamp=datetime.now(timezone.utc).isoformat(),
            model=model,
            source_provider=source_provider,
            target_provider=target_provider,
            is_stream=is_stream,
            status_code=status_code,
            duration_ms=round(duration_ms, 2),
            error_detail=error_detail,
            api_key_label=api_key_label,
            target_provider_name=target_provider_name,
            client_ip=client_ip,
            request_path=request_path,
            request_method=request_method,
            request_body=request_body,
            request_headers=request_headers,
            response_body=response_body,
            response_headers=response_headers,
            upstream_request_body=upstream_request_body,
            upstream_response_body=upstream_response_body,
            upstream_request_headers=upstream_request_headers,
            upstream_response_headers=upstream_response_headers,
            upstream_url=upstream_url,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dict."""
        d: dict[str, Any] = {
            "id": self.id,
            "timestamp": self.timestamp,
            "model": self.model,
            "source_provider": self.source_provider,
            "target_provider": self.target_provider,
            "is_stream": self.is_stream,
            "status_code": self.status_code,
            "duration_ms": self.duration_ms,
        }
        if self.error_detail is not None:
            d["error_detail"] = self.error_detail
        if self.api_key_label is not None:
            d["api_key_label"] = self.api_key_label
        if self.target_provider_name is not None:
            d["target_provider_name"] = self.target_provider_name
        if self.client_ip is not None:
            d["client_ip"] = self.client_ip
        if self.request_path is not None:
            d["request_path"] = self.request_path
        if self.request_method is not None:
            d["request_method"] = self.request_method
        # Detailed request/response (optional, only included when present)
        if self.request_body is not None:
            d["request_body"] = self.request_body
        if self.request_headers is not None:
            d["request_headers"] = self.request_headers
        if self.response_body is not None:
            d["response_body"] = self.response_body
        if self.response_headers is not None:
            d["response_headers"] = self.response_headers
        if self.upstream_request_body is not None:
            d["upstream_request_body"] = self.upstream_request_body
        if self.upstream_response_body is not None:
            d["upstream_response_body"] = self.upstream_response_body
        if self.upstream_request_headers is not None:
            d["upstream_request_headers"] = self.upstream_request_headers
        if self.upstream_response_headers is not None:
            d["upstream_response_headers"] = self.upstream_response_headers
        if self.upstream_url is not None:
            d["upstream_url"] = self.upstream_url
        return d


def finalize_stream_request_log() -> None:
    """Flush a deferred streaming request log entry, if one is pending."""
    pending = pending_stream_log_var.get()
    if pending is None:
        return
    request_log = pending.get("request_log")
    if request_log is None:
        pending_stream_log_var.set(None)
        return

    detail = request_detail_var.get()
    request_log.add(
        RequestLogEntry.create(
            model=pending["model"],
            source_provider=pending["source_provider"],
            target_provider=pending["target_provider"],
            target_provider_name=pending.get("target_provider_name"),
            is_stream=True,
            status_code=pending["status_code"],
            duration_ms=pending["duration_ms"],
            error_detail=pending.get("error_detail"),
            api_key_label=pending.get("api_key_label"),
            client_ip=pending.get("client_ip"),
            request_path=pending.get("request_path"),
            request_method=pending.get("request_method"),
            request_body=detail.get("request_body") if detail else None,
            request_headers=detail.get("request_headers") if detail else None,
            response_body=detail.get("response_body") if detail else None,
            response_headers=detail.get("response_headers") if detail else None,
            upstream_request_body=detail.get("upstream_request_body")
            if detail
            else None,
            upstream_response_body=detail.get("upstream_response_body")
            if detail
            else None,
            upstream_request_headers=detail.get("upstream_request_headers")
            if detail
            else None,
            upstream_response_headers=detail.get("upstream_response_headers")
            if detail
            else None,
            upstream_url=detail.get("upstream_url") if detail else None,
        )
    )
    pending_stream_log_var.set(None)
    request_detail_var.set(None)


class RequestLog:
    """Proxy request log with optional SQLite persistence.

    When *persistence* is provided, all operations delegate to SQLite.
    Otherwise falls back to an in-memory :class:`collections.deque`
    ring buffer (used when no config path is available).
    """

    def __init__(
        self,
        persistence: PersistenceManager | None = None,
        max_entries: int = 500,
    ) -> None:
        self._persistence = persistence
        # Fallback in-memory storage (only used when persistence is None)
        self._entries: deque[RequestLogEntry] = deque(maxlen=max_entries)
        self._pending: list[RequestLogEntry] = []

    def add(self, entry: RequestLogEntry) -> None:
        """Record a proxy request."""
        if self._persistence is not None:
            self._persistence.insert_log_entries([entry.to_dict()])
        else:
            self._entries.append(entry)
            self._pending.append(entry)

    def get_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        model: str | None = None,
        provider: str | None = None,
        provider_type: str | None = None,
        status: str | None = None,
        api_key_label: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return filtered entries (newest-first) and total count.

        Args:
            provider: Provider display name (e.g. ``"Gemini"``).
            provider_type: Resolved API type for *provider* (e.g.
                ``"google"``).  When supplied the filter also matches
                legacy entries whose ``target_provider`` stores the API
                type but have no ``target_provider_name`` backfill.
            api_key_label: Filter by API key label (exact match).
        """
        if self._persistence is not None:
            return self._persistence.query_log_entries(
                limit=limit,
                offset=offset,
                model=model,
                provider=provider,
                provider_type=provider_type,
                status=status,
                api_key_label=api_key_label,
            )

        # Fallback: in-memory filtering
        filtered: list[RequestLogEntry] = list(reversed(self._entries))
        if model:
            filtered = [e for e in filtered if e.model == model]
        if provider:
            filtered = [
                e
                for e in filtered
                if e.target_provider_name == provider
                or e.target_provider == provider
                or (
                    provider_type
                    and e.target_provider_name is None
                    and e.target_provider == provider_type
                )
            ]
        if status == "ok":
            filtered = [e for e in filtered if e.status_code < 400]
        elif status == "error":
            filtered = [e for e in filtered if e.status_code >= 400]
        if api_key_label:
            filtered = [e for e in filtered if e.api_key_label == api_key_label]
        total = len(filtered)
        page = filtered[offset : offset + limit]
        return [e.to_dict() for e in page], total

    def get_entry(self, entry_id: str) -> dict[str, Any] | None:
        """Return a single entry by id, or ``None``."""
        if self._persistence is not None:
            return self._persistence.get_log_entry(entry_id)
        for e in self._entries:
            if e.id == entry_id:
                return e.to_dict()
        return None

    def get_api_key_labels(self) -> list[str]:
        """Return distinct API key labels seen in request logs."""
        if self._persistence is not None:
            return self._persistence.get_api_key_labels()
        return sorted({e.api_key_label for e in self._entries if e.api_key_label})

    def load_entries(self, entries: list[dict[str, Any]]) -> None:
        """Bulk-load entries (in-memory fallback only)."""
        for d in entries:
            try:
                entry = RequestLogEntry(**d)
                self._entries.append(entry)
            except (TypeError, KeyError):
                continue

    def pending_entries(self) -> list[dict[str, Any]]:
        """Return and clear entries added since last call.

        Only meaningful in fallback mode; returns ``[]`` when using
        SQLite persistence (entries are written immediately).
        """
        if self._persistence is not None:
            return []
        entries = [e.to_dict() for e in self._pending]
        self._pending.clear()
        return entries

    def clear(self) -> None:
        """Remove all entries."""
        if self._persistence is not None:
            self._persistence.clear_log()
        else:
            self._entries.clear()

    def __len__(self) -> int:
        if self._persistence is not None:
            return self._persistence.count_log_entries()
        return len(self._entries)
