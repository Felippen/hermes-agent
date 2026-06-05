"""Hermes Zen Engine v1.

The v1 engine is intentionally session-local and deterministic. It subclasses
the built-in compressor so existing compression behavior remains intact, then
adds a request-copy working brief through ContextEngine.compile_turn_context().
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from agent.context_compressor import ContextCompressor


_NOTE_KINDS = {
    "constraint",
    "evidence",
    "failed_path",
    "file_fact",
    "user_guidance",
    "plan_anchor",
    "open_item",
}

_FILE_RE = re.compile(r"(?<![\w/.-])(?:[\w.-]+/)+[\w.-]+\.[A-Za-z0-9_]+|(?<![\w.-])[\w.-]+\.(?:py|ts|tsx|js|jsx|md|json|yaml|yml|toml|sh|swift|sql)")
_FAILURE_RE = re.compile(r"\b(error|failed|failure|exception|traceback|non-zero|not found|denied|timeout|timed out)\b", re.I)
_EVIDENCE_RE = re.compile(r"\b(passed|validated|verified|succeeded|complete|completed|wrote|updated|created|inspected|confirmed)\b", re.I)
_CONSTRAINT_RE = re.compile(r"\b(must|shall|required|do not|don't|never|only|preserve|avoid|without|explicitly)\b", re.I)
_PLAN_RE = re.compile(r"\b(plan|next step|approach|decision|choose|chosen|implement|verify)\b", re.I)
_OPEN_RE = re.compile(r"\b(todo|open question|blocked|remaining|unclear|unknown|need to|next)\b", re.I)


@dataclass(frozen=True)
class ZenSourcePointer:
    pointer_id: str
    message_index: int
    role: str
    content_sha12: str


@dataclass(frozen=True)
class ZenNote:
    kind: str
    summary: str
    source: ZenSourcePointer
    confidence: str = "observed"


class ZenContextEngine(ContextCompressor):
    """Opt-in, in-memory Zen v1 context engine."""

    max_notes: int = 80
    max_brief_notes: int = 12

    @property
    def name(self) -> str:
        return "zen"

    def __init__(self, model: str = "", *args: Any, **kwargs: Any) -> None:
        super().__init__(model=model, *args, **kwargs)
        self._zen_session_id = ""
        self._zen_notes: list[ZenNote] = []
        self._zen_seen: set[tuple[str, str, str]] = set()
        self._zen_source_pointers: dict[str, ZenSourcePointer] = {}
        self._zen_last_brief_chars = 0

    def on_session_start(self, session_id: str, **kwargs: Any) -> None:
        self._zen_session_id = session_id or ""

    def on_session_reset(self) -> None:
        super().on_session_reset()
        self._clear_zen_state()

    def on_session_end(self, session_id: str, messages: List[Dict[str, Any]]) -> None:
        self._clear_zen_state()

    def compile_turn_context(
        self,
        *,
        session_id: str,
        user_message: str,
        conversation_history: List[Dict[str, Any]],
        current_turn_user_idx: int,
        model: str = "",
        platform: str = "",
        system_prompt_chars: int = 0,
    ) -> str | None:
        if session_id and session_id != self._zen_session_id:
            self._zen_session_id = session_id
        self._ingest_messages(conversation_history, current_turn_user_idx)
        brief = self._assemble_working_brief(user_message=user_message)
        self._zen_last_brief_chars = len(brief)
        return brief or None

    def get_tool_schemas(self) -> List[Dict[str, Any]]:
        return []

    def handle_tool_call(self, name: str, args: Dict[str, Any], **kwargs: Any) -> str:
        import json

        return json.dumps({"error": "Hermes Zen v1 exposes no model-callable tools"})

    def get_status(self) -> Dict[str, Any]:
        status = super().get_status()
        status.update(
            {
                "zen_notes": len(self._zen_notes),
                "zen_source_pointers": len(self._zen_source_pointers),
                "zen_last_brief_chars": self._zen_last_brief_chars,
            }
        )
        return status

    @property
    def zen_notes(self) -> tuple[ZenNote, ...]:
        return tuple(self._zen_notes)

    @property
    def zen_source_pointers(self) -> dict[str, ZenSourcePointer]:
        return dict(self._zen_source_pointers)

    def _clear_zen_state(self) -> None:
        self._zen_notes.clear()
        self._zen_seen.clear()
        self._zen_source_pointers.clear()
        self._zen_last_brief_chars = 0

    def _ingest_messages(self, messages: List[Dict[str, Any]], current_turn_user_idx: int) -> None:
        start = max(0, len(messages) - 40)
        for idx in range(start, len(messages)):
            msg = messages[idx]
            role = str(msg.get("role", ""))
            content = _content_to_text(msg.get("content", ""))
            if not content.strip():
                continue
            pointer = self._source_pointer(idx, role, content)
            for kind, summary in self._extract_notes(role, content, idx, current_turn_user_idx):
                self._append_note(kind, summary, pointer)
        if len(self._zen_notes) > self.max_notes:
            self._zen_notes = self._zen_notes[-self.max_notes :]
            self._zen_seen = {(note.kind, note.summary, note.source.pointer_id) for note in self._zen_notes}
            self._zen_source_pointers = {note.source.pointer_id: note.source for note in self._zen_notes}

    def _source_pointer(self, idx: int, role: str, content: str) -> ZenSourcePointer:
        digest = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()[:12]
        pointer = ZenSourcePointer(
            pointer_id=f"turn:{idx}:{role}:{digest}",
            message_index=idx,
            role=role,
            content_sha12=digest,
        )
        self._zen_source_pointers[pointer.pointer_id] = pointer
        return pointer

    def _extract_notes(
        self,
        role: str,
        content: str,
        idx: int,
        current_turn_user_idx: int,
    ) -> Iterable[tuple[str, str]]:
        compact = _compact(content)
        if role == "user":
            if _CONSTRAINT_RE.search(compact):
                yield "constraint", _sentence(compact)
            if idx == current_turn_user_idx:
                yield "open_item", _sentence(compact)
            elif _OPEN_RE.search(compact):
                yield "user_guidance", _sentence(compact)
        elif role == "assistant":
            if _PLAN_RE.search(compact):
                yield "plan_anchor", _sentence(compact)
            if _EVIDENCE_RE.search(compact):
                yield "evidence", _sentence(compact)
        elif role == "tool":
            if _FAILURE_RE.search(compact):
                yield "failed_path", _sentence(compact)
            else:
                yield "evidence", _sentence(compact)

        for path in _paths(compact):
            yield "file_fact", f"{path}: referenced in {role} turn"

    def _append_note(self, kind: str, summary: str, source: ZenSourcePointer) -> None:
        if kind not in _NOTE_KINDS:
            return
        summary = summary.strip()
        if not summary:
            return
        key = (kind, summary, source.pointer_id)
        if key in self._zen_seen:
            return
        self._zen_seen.add(key)
        self._zen_notes.append(ZenNote(kind=kind, summary=summary, source=source))

    def _assemble_working_brief(self, *, user_message: str) -> str:
        if not self._zen_notes:
            return ""
        ordered = sorted(
            self._zen_notes,
            key=lambda note: (_kind_rank(note.kind), note.source.message_index),
        )
        selected = _latest_by_kind_then_rank(ordered, self.max_brief_notes)
        if not selected:
            return ""
        lines = ["Hermes Zen working brief (session-local, source-backed):"]
        for note in selected:
            lines.append(
                f"- {note.kind}: {note.summary} [source: {note.source.pointer_id}]"
            )
        if _has_conflicting_constraints(selected):
            lines.append("- uncertainty: constraints may conflict; verify against the latest user turn before acting")
        return "\n".join(lines)


def register(ctx: Any) -> None:
    ctx.register_context_engine(ZenContextEngine())


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text") or item.get("content") or ""
                if text:
                    parts.append(str(text))
            elif item is not None:
                parts.append(str(item))
        return "\n".join(parts)
    if content is None:
        return ""
    return str(content)


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _sentence(text: str, limit: int = 220) -> str:
    text = _compact(text)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _paths(text: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for match in _FILE_RE.finditer(text):
        path = match.group(0)
        if path not in seen:
            seen.add(path)
            result.append(path)
    return result[:4]


def _kind_rank(kind: str) -> int:
    ranks = {
        "constraint": 0,
        "user_guidance": 1,
        "open_item": 2,
        "plan_anchor": 3,
        "evidence": 4,
        "failed_path": 5,
        "file_fact": 6,
    }
    return ranks.get(kind, 99)


def _latest_by_kind_then_rank(notes: list[ZenNote], limit: int) -> list[ZenNote]:
    latest: dict[tuple[str, str], ZenNote] = {}
    for note in notes:
        latest[(note.kind, note.summary)] = note
    deduped = sorted(latest.values(), key=lambda note: (_kind_rank(note.kind), -note.source.message_index))
    return deduped[:limit]


def _has_conflicting_constraints(notes: list[ZenNote]) -> bool:
    constraints = [note.summary.lower() for note in notes if note.kind == "constraint"]
    if len(constraints) < 2:
        return False
    has_do = any("do " in item or "must " in item for item in constraints)
    has_do_not = any("do not" in item or "don't" in item or "never" in item for item in constraints)
    return has_do and has_do_not


__all__ = ["ZenContextEngine", "ZenNote", "ZenSourcePointer", "register"]
