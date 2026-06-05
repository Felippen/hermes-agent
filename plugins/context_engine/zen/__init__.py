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

_LARGE_OBSERVATION_CHARS = 1200
_MASKED_SUMMARY_CHARS = 280
_STALE_MESSAGE_AGE = 12
_OBSERVATION_WINDOW = 40


@dataclass(frozen=True)
class ZenSourcePointer:
    pointer_id: str
    message_index: int
    role: str
    content_sha12: str


@dataclass(frozen=True)
class ZenMaskingMetadata:
    observation_class: str
    payload_fingerprint: str
    original_chars: int
    summary_chars: int
    source_pointer_ids: tuple[str, ...]
    occurrence_count: int = 1
    stale: bool = False


@dataclass(frozen=True)
class ZenNote:
    kind: str
    summary: str
    source: ZenSourcePointer
    confidence: str = "observed"
    masking: ZenMaskingMetadata | None = None


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
        self._zen_observation_fingerprints: dict[str, int] = {}
        self._zen_observation_sources: dict[str, tuple[str, ...]] = {}
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
        self._zen_observation_fingerprints.clear()
        self._zen_observation_sources.clear()
        self._zen_last_brief_chars = 0

    def _ingest_messages(self, messages: List[Dict[str, Any]], current_turn_user_idx: int) -> None:
        start = max(0, len(messages) - _OBSERVATION_WINDOW)
        for idx in range(start, len(messages)):
            msg = messages[idx]
            role = str(msg.get("role", ""))
            content = _content_to_text(msg.get("content", ""))
            if not content.strip():
                continue
            pointer = self._source_pointer(idx, role, content)
            if role == "tool":
                try:
                    if self._ingest_tool_observation(content, idx, current_turn_user_idx, pointer):
                        continue
                except Exception:
                    continue
            for kind, summary in self._extract_notes(role, content, idx, current_turn_user_idx):
                self._append_note(kind, summary, pointer)
        if len(self._zen_notes) > self.max_notes:
            self._zen_notes = self._zen_notes[-self.max_notes :]
            self._zen_seen = {(note.kind, note.summary, note.source.pointer_id) for note in self._zen_notes}
            self._zen_source_pointers = {note.source.pointer_id: note.source for note in self._zen_notes}
            self._zen_observation_fingerprints = {
                note.masking.payload_fingerprint: pos
                for pos, note in enumerate(self._zen_notes)
                if note.masking is not None
            }
            self._zen_observation_sources = {
                note.masking.payload_fingerprint: note.masking.source_pointer_ids
                for note in self._zen_notes
                if note.masking is not None
            }

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

    def _ingest_tool_observation(
        self,
        content: str,
        idx: int,
        current_turn_user_idx: int,
        pointer: ZenSourcePointer,
    ) -> bool:
        classification = _classify_observation(
            content,
            idx=idx,
            current_turn_user_idx=current_turn_user_idx,
            seen_fingerprints=self._zen_observation_sources,
        )
        prior_sources = self._zen_observation_sources.get(classification.payload_fingerprint, ())
        if pointer.pointer_id not in prior_sources:
            self._zen_observation_sources[classification.payload_fingerprint] = prior_sources + (pointer.pointer_id,)
        if classification.observation_class == "ordinary":
            return False
        if classification.observation_class == "repeated":
            self._merge_repeated_observation(classification, pointer, content)
            return True

        kind = "failed_path" if classification.observation_class == "failed" else "evidence"
        if classification.observation_class == "path_bearing":
            kind = "file_fact"
        summary = _masked_observation_summary(content, classification)
        metadata = ZenMaskingMetadata(
            observation_class=classification.observation_class,
            payload_fingerprint=classification.payload_fingerprint,
            original_chars=len(content),
            summary_chars=len(summary),
            source_pointer_ids=(pointer.pointer_id,),
            stale=classification.stale,
        )
        note = self._append_note(kind, summary, pointer, masking=metadata)
        if note is not None:
            self._zen_observation_fingerprints[classification.payload_fingerprint] = len(self._zen_notes) - 1
        return True

    def _merge_repeated_observation(
        self,
        classification: "ZenObservationClassification",
        pointer: ZenSourcePointer,
        content: str,
    ) -> None:
        note_idx = self._zen_observation_fingerprints.get(classification.payload_fingerprint)
        pointer_ids = self._zen_observation_sources.get(classification.payload_fingerprint, (pointer.pointer_id,))
        if note_idx is None or note_idx >= len(self._zen_notes):
            summary = _repeat_summary(_masked_observation_summary(content, classification), len(pointer_ids))
            metadata = ZenMaskingMetadata(
                observation_class="repeated",
                payload_fingerprint=classification.payload_fingerprint,
                original_chars=classification.original_chars,
                summary_chars=len(summary),
                source_pointer_ids=pointer_ids,
                occurrence_count=len(pointer_ids),
                stale=classification.stale,
            )
            note = self._append_note("evidence", summary, pointer, masking=metadata)
            if note is not None:
                self._zen_observation_fingerprints[classification.payload_fingerprint] = len(self._zen_notes) - 1
            return
        note = self._zen_notes[note_idx]
        if note.masking is None:
            return
        summary = _repeat_summary(note.summary, len(pointer_ids))
        metadata = ZenMaskingMetadata(
            observation_class="repeated",
            payload_fingerprint=note.masking.payload_fingerprint,
            original_chars=max(note.masking.original_chars, classification.original_chars),
            summary_chars=len(summary),
            source_pointer_ids=pointer_ids,
            occurrence_count=len(pointer_ids),
            stale=note.masking.stale or classification.stale,
        )
        self._zen_notes[note_idx] = ZenNote(
            kind=note.kind,
            summary=summary,
            source=note.source,
            confidence="observed",
            masking=metadata,
        )

    def _append_note(
        self,
        kind: str,
        summary: str,
        source: ZenSourcePointer,
        *,
        masking: ZenMaskingMetadata | None = None,
    ) -> ZenNote | None:
        if kind not in _NOTE_KINDS:
            return None
        summary = summary.strip()
        if not summary:
            return None
        key = (kind, summary, source.pointer_id)
        if key in self._zen_seen:
            return None
        self._zen_seen.add(key)
        note = ZenNote(kind=kind, summary=summary, source=source, masking=masking)
        self._zen_notes.append(note)
        return note

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
            stale = " stale/uncertain" if note.masking and note.masking.stale else ""
            coverage = ""
            if note.masking and note.masking.occurrence_count > 1:
                coverage = f"; sources: {', '.join(note.masking.source_pointer_ids)}"
            lines.append(
                f"- {note.kind}{stale}: {note.summary} [source: {note.source.pointer_id}{coverage}]"
            )
        if _has_conflicting_constraints(selected):
            lines.append("- uncertainty: constraints may conflict; verify against the latest user turn before acting")
        return "\n".join(lines)


def register(ctx: Any) -> None:
    ctx.register_context_engine(ZenContextEngine())


@dataclass(frozen=True)
class ZenObservationClassification:
    observation_class: str
    payload_fingerprint: str
    original_chars: int
    stale: bool = False


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
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def _bounded_mask_summary(text: str, limit: int = _MASKED_SUMMARY_CHARS) -> str:
    return _sentence(text, limit)


def _paths(text: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for match in _FILE_RE.finditer(text):
        path = match.group(0)
        if path not in seen:
            seen.add(path)
            result.append(path)
    return result[:4]


def _observation_fingerprint(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip().lower()
    normalized = re.sub(r"\d+", "#", normalized)
    return hashlib.sha256(normalized.encode("utf-8", errors="replace")).hexdigest()[:16]


def _classify_observation(
    text: str,
    *,
    idx: int,
    current_turn_user_idx: int,
    seen_fingerprints: dict[str, tuple[str, ...]],
) -> ZenObservationClassification:
    compact = _compact(text)
    fingerprint = _observation_fingerprint(compact)
    stale = current_turn_user_idx - idx > _STALE_MESSAGE_AGE
    if _FAILURE_RE.search(compact):
        observation_class = "failed"
    elif fingerprint in seen_fingerprints:
        observation_class = "repeated"
    elif len(compact) > _LARGE_OBSERVATION_CHARS:
        observation_class = "large"
    elif _paths(compact):
        observation_class = "path_bearing"
    elif stale:
        observation_class = "stale"
    else:
        observation_class = "ordinary"
    return ZenObservationClassification(
        observation_class=observation_class,
        payload_fingerprint=fingerprint,
        original_chars=len(text),
        stale=stale,
    )


def _masked_observation_summary(text: str, classification: ZenObservationClassification) -> str:
    compact = _compact(text)
    if not compact and classification.observation_class == "repeated":
        return "Repeated tool observation"
    if classification.observation_class == "failed":
        return _bounded_mask_summary(f"Tool observation failed; avoid repeating the same path. Detail: {compact}")
    if classification.observation_class == "path_bearing":
        paths = ", ".join(_paths(compact))
        return _bounded_mask_summary(f"Tool observation referenced files: {paths}")
    if classification.observation_class == "stale":
        return _bounded_mask_summary(f"Stale tool observation retained for context only: {compact}")
    if classification.observation_class == "large":
        return _bounded_mask_summary(f"Large tool observation masked ({classification.original_chars} chars): {compact}")
    return _bounded_mask_summary(compact)


def _repeat_summary(summary: str, count: int) -> str:
    base = re.sub(r"^Repeated tool observation \(\d+x\):\s*", "", summary).strip()
    return _bounded_mask_summary(f"Repeated tool observation ({count}x): {base}")


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


__all__ = [
    "ZenContextEngine",
    "ZenMaskingMetadata",
    "ZenNote",
    "ZenObservationClassification",
    "ZenSourcePointer",
    "register",
]
