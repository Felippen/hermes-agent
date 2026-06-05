"""Hermes Zen Engine v1.

The v1 engine is intentionally session-local and deterministic. It subclasses
the built-in compressor so existing compression behavior remains intact, then
adds a request-copy working brief through ContextEngine.compile_turn_context().
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Protocol

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
_CONTINUE_RE = re.compile(r"\b(continue|resume|pick up|carry on|what'?s next|next)\b", re.I)
_VERIFY_RE = re.compile(r"\b(verify|verification|validate|validation|test|rollback|merge order|pr)\b", re.I)
_PR_RE = re.compile(r"(?:pull/\d+|PR\s*#?\d+|#\d+)", re.I)

_LARGE_OBSERVATION_CHARS = 1200
_MASKED_SUMMARY_CHARS = 280
_STALE_MESSAGE_AGE = 12
_OBSERVATION_WINDOW = 40
_MASKING_THRESHOLDS = {
    "relaxed": 2000,
    "standard": _LARGE_OBSERVATION_CHARS,
    "strict": 800,
}
_TRACE_VERBOSITIES = {"standard", "debug"}
_TRACE_ACTIONS = {
    "kept",
    "compressed",
    "masked",
    "dropped",
    "deferred",
    "selected",
    "fallback",
    "bypassed",
}
_NEED_STATES = {"detected", "advised", "resolved", "suppressed", "rejected"}
_NEED_RISK_LEVELS = {"low", "medium", "high"}
_NEED_POLICIES = {"advisory", "provider_allowed", "provider_blocked"}
_PROVIDER_OUTCOMES = {"denied", "empty", "stale", "unsupported", "rejected"}


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
class ZenDecisionTrace:
    action: str
    reason_code: str
    confidence: str
    input_source: str
    token_estimate: int
    safety_policy: str
    budget_impact: int
    redacted: bool = True

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason_code": self.reason_code,
            "confidence": self.confidence,
            "input_source": self.input_source,
            "token_estimate": self.token_estimate,
            "safety_policy": self.safety_policy,
            "budget_impact": self.budget_impact,
            "redacted": self.redacted,
        }


@dataclass
class ZenMetrics:
    context_chars_in: int = 0
    context_chars_out: int = 0
    masked_span_count: int = 0
    kept_count: int = 0
    compressed_count: int = 0
    masked_count: int = 0
    dropped_count: int = 0
    deferred_count: int = 0
    selected_count: int = 0
    trace_event_count: int = 0
    trace_complete: bool = True
    privacy_safe: bool = True
    context_need_count: int = 0
    context_need_advisory_count: int = 0
    provider_call_count: int = 0
    injected_slice_count: int = 0
    rejected_slice_count: int = 0
    provider_budget_used: int = 0
    provider_denied_count: int = 0
    provider_empty_count: int = 0
    provider_stale_count: int = 0

    @property
    def compression_ratio(self) -> float:
        if self.context_chars_in <= 0:
            return 0.0
        return round(self.context_chars_out / self.context_chars_in, 4)

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "context_chars_in": self.context_chars_in,
            "context_chars_out": self.context_chars_out,
            "masked_span_count": self.masked_span_count,
            "compression_ratio": self.compression_ratio,
            "kept_count": self.kept_count,
            "compressed_count": self.compressed_count,
            "masked_count": self.masked_count,
            "dropped_count": self.dropped_count,
            "deferred_count": self.deferred_count,
            "selected_count": self.selected_count,
            "trace_event_count": self.trace_event_count,
            "trace_complete": self.trace_complete,
            "privacy_safe": self.privacy_safe,
            "context_need_count": self.context_need_count,
            "context_need_advisory_count": self.context_need_advisory_count,
            "provider_call_count": self.provider_call_count,
            "injected_slice_count": self.injected_slice_count,
            "rejected_slice_count": self.rejected_slice_count,
            "provider_budget_used": self.provider_budget_used,
            "provider_denied_count": self.provider_denied_count,
            "provider_empty_count": self.provider_empty_count,
            "provider_stale_count": self.provider_stale_count,
        }


@dataclass(frozen=True)
class ZenContextNeed:
    need_id: str
    reason: str
    confidence: float
    triggering_source: str
    desired_context_type: str
    risk_level: str
    acquisition_policy: str
    trace_id: str
    token_budget: int
    lifecycle_state: str = "detected"

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "need_id": self.need_id,
            "reason": self.reason,
            "confidence": self.confidence,
            "triggering_source": self.triggering_source,
            "desired_context_type": self.desired_context_type,
            "risk_level": self.risk_level,
            "acquisition_policy": self.acquisition_policy,
            "trace_id": self.trace_id,
            "token_budget": self.token_budget,
            "lifecycle_state": self.lifecycle_state,
            "redacted": True,
        }


@dataclass(frozen=True)
class ZenContextSlice:
    need_id: str
    summary: str
    source_id: str
    token_estimate: int
    uncertain: bool = False

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "need_id": self.need_id,
            "summary": _sentence(self.summary, _MASKED_SUMMARY_CHARS),
            "source_id": self.source_id,
            "token_estimate": self.token_estimate,
            "uncertain": self.uncertain,
            "redacted": True,
        }


@dataclass(frozen=True)
class ZenProviderOutcome:
    status: str
    reason: str = ""
    summary: str = ""
    source_id: str = ""
    token_estimate: int = 0

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "status": self.status if self.status in _PROVIDER_OUTCOMES else "rejected",
            "reason": _sentence(self.reason, 120),
            "source_id": self.source_id,
            "token_estimate": max(0, int(self.token_estimate)),
            "redacted": True,
        }


class ZenContextNeedProvider(Protocol):
    def resolve_context_need(
        self,
        need: ZenContextNeed,
        budget: int,
    ) -> Iterable[ZenContextSlice | ZenProviderOutcome | Dict[str, Any] | str]:
        ...


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

    def __init__(
        self,
        model: str = "",
        *args: Any,
        zen_config: Dict[str, Any] | None = None,
        context_need_provider: ZenContextNeedProvider | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(model=model, *args, **kwargs)
        self._zen_session_id = ""
        self._zen_notes: list[ZenNote] = []
        self._zen_seen: set[tuple[str, str, str]] = set()
        self._zen_source_pointers: dict[str, ZenSourcePointer] = {}
        self._zen_observation_fingerprints: dict[str, int] = {}
        self._zen_observation_sources: dict[str, tuple[str, ...]] = {}
        self._zen_decision_traces: list[ZenDecisionTrace] = []
        self._zen_context_needs: list[ZenContextNeed] = []
        self._zen_context_slices: list[ZenContextSlice] = []
        self._zen_context_need_seen: set[str] = set()
        self._zen_context_need_provider = context_need_provider
        self._zen_metrics = ZenMetrics()
        self._zen_last_brief_chars = 0
        self._zen_config = _normalize_zen_config(zen_config)

    def on_session_start(self, session_id: str, **kwargs: Any) -> None:
        self._zen_session_id = session_id or ""
        if "zen_config" in kwargs:
            self.configure_zen(kwargs.get("zen_config"))

    def configure_zen(self, config: Dict[str, Any] | None) -> None:
        self._zen_config = _normalize_zen_config(config)

    def configure_context_need_provider(self, provider: ZenContextNeedProvider | None) -> None:
        self._zen_context_need_provider = provider

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
        self._zen_metrics.context_chars_in = _history_chars(conversation_history)
        if not self._zen_config["enabled"]:
            self._record_trace(
                action="bypassed",
                reason_code="zen_disabled",
                safety_policy="operator_control",
            )
            self._zen_last_brief_chars = 0
            self._zen_metrics.context_chars_out = 0
            return None
        if self._zen_config["force_compressor_fallback"]:
            self._record_trace(
                action="fallback",
                reason_code="compressor_fallback_forced",
                safety_policy="operator_control",
            )
            self._zen_last_brief_chars = 0
            self._zen_metrics.context_chars_out = 0
            return None
        if session_id and session_id in self._zen_config["bypass_session_ids"]:
            self._record_trace(
                action="bypassed",
                reason_code="session_bypassed",
                safety_policy="operator_control",
            )
            self._zen_last_brief_chars = 0
            self._zen_metrics.context_chars_out = 0
            return None
        self._ingest_messages(conversation_history, current_turn_user_idx)
        brief = self._assemble_working_brief(
            user_message=user_message,
            current_turn_user_idx=current_turn_user_idx,
        )
        self._zen_last_brief_chars = len(brief)
        self._zen_metrics.context_chars_out = len(brief)
        return brief or None

    def compress(
        self,
        messages: List[Dict[str, Any]],
        current_tokens: int = None,
        focus_topic: str = None,
    ) -> List[Dict[str, Any]]:
        before_chars = _history_chars(messages)
        result = super().compress(messages, current_tokens=current_tokens, focus_topic=focus_topic)
        after_chars = _history_chars(result)
        self._record_trace(
            action="compressed",
            reason_code="inherited_compressor_compress",
            token_estimate=current_tokens if current_tokens is not None else _estimate_tokens(str(before_chars)),
            budget_impact=after_chars - before_chars,
            safety_policy="compressor_owned",
        )
        return result

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
                "zen_traces": len(self._zen_decision_traces),
                "zen_trace_complete": self._zen_metrics.trace_complete,
                "zen_privacy_safe": self._zen_metrics.privacy_safe,
                "zen_metrics": self._zen_metrics.to_safe_dict(),
            }
        )
        return status

    @property
    def zen_notes(self) -> tuple[ZenNote, ...]:
        return tuple(self._zen_notes)

    @property
    def zen_source_pointers(self) -> dict[str, ZenSourcePointer]:
        return dict(self._zen_source_pointers)

    @property
    def zen_decision_traces(self) -> tuple[ZenDecisionTrace, ...]:
        return tuple(self._zen_decision_traces)

    @property
    def zen_metrics(self) -> dict[str, Any]:
        return self._zen_metrics.to_safe_dict()

    @property
    def zen_context_needs(self) -> tuple[ZenContextNeed, ...]:
        return tuple(self._zen_context_needs)

    @property
    def zen_context_slices(self) -> tuple[ZenContextSlice, ...]:
        return tuple(self._zen_context_slices)

    def _clear_zen_state(self) -> None:
        self._zen_notes.clear()
        self._zen_seen.clear()
        self._zen_source_pointers.clear()
        self._zen_observation_fingerprints.clear()
        self._zen_observation_sources.clear()
        self._zen_decision_traces.clear()
        self._zen_context_needs.clear()
        self._zen_context_slices.clear()
        self._zen_context_need_seen.clear()
        self._zen_metrics = ZenMetrics()
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
                    self._record_trace(
                        action="dropped",
                        reason_code="masking_error",
                        source=pointer,
                        token_estimate=_estimate_tokens(content),
                        safety_policy="request_copy_only",
                    )
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
            large_threshold=self._zen_large_observation_chars,
        )
        prior_sources = self._zen_observation_sources.get(classification.payload_fingerprint, ())
        if pointer.pointer_id not in prior_sources:
            self._zen_observation_sources[classification.payload_fingerprint] = prior_sources + (pointer.pointer_id,)
        if classification.observation_class == "ordinary":
            self._record_trace(
                action="deferred",
                reason_code="observation_ordinary",
                source=pointer,
                token_estimate=_estimate_tokens(content),
                safety_policy="source_backed",
            )
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
            self._zen_metrics.masked_span_count += 1
            self._record_trace(
                action="masked",
                reason_code=f"observation_{classification.observation_class}",
                source=pointer,
                token_estimate=_estimate_tokens(content),
                budget_impact=len(summary) - len(content),
                confidence="uncertain" if classification.stale else "observed",
                safety_policy="source_backed",
            )
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
                self._zen_metrics.masked_span_count += 1
                self._record_trace(
                    action="masked",
                    reason_code="observation_repeated",
                    source=pointer,
                    token_estimate=_estimate_tokens(content),
                    budget_impact=len(summary) - len(content),
                    confidence="observed",
                    safety_policy="source_backed",
                )
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
        self._record_trace(
            action="masked",
            reason_code="observation_repeated",
            source=pointer,
            token_estimate=_estimate_tokens(content),
            budget_impact=len(summary) - len(content),
            confidence="observed",
            safety_policy="source_backed",
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
            self._record_trace(
                action="dropped",
                reason_code="duplicate_note",
                source=source,
                token_estimate=_estimate_tokens(summary),
                safety_policy="privacy_safe_trace",
            )
            return None
        self._zen_seen.add(key)
        note = ZenNote(kind=kind, summary=summary, source=source, masking=masking)
        self._zen_notes.append(note)
        if masking is None:
            self._record_trace(
                action="kept",
                reason_code=f"{kind}_note_extracted",
                source=source,
                token_estimate=_estimate_tokens(summary),
                budget_impact=len(summary),
                confidence="observed",
                safety_policy="source_backed",
            )
        return note

    def _assemble_working_brief(self, *, user_message: str, current_turn_user_idx: int | None = None) -> str:
        ordered = sorted(
            self._zen_notes,
            key=lambda note: (_kind_rank(note.kind), note.source.message_index),
        )
        selected = _latest_by_kind_then_rank(ordered, self.max_brief_notes)
        fallback_source = _latest_source(selected)
        if fallback_source is None and user_message.strip():
            fallback_source = self._source_pointer(
                current_turn_user_idx if current_turn_user_idx is not None and current_turn_user_idx >= 0 else 0,
                "user",
                user_message,
            )
        needs = self._sense_context_needs(
            selected,
            user_message=user_message,
            current_turn_user_idx=current_turn_user_idx,
            fallback_source=fallback_source,
        )
        need_lines = self._context_need_brief_lines(needs)
        if not selected and not need_lines:
            return ""
        lines = ["Hermes Zen working brief (session-local, source-backed):"]
        for note in selected:
            self._record_trace(
                action="selected",
                reason_code="working_brief_selected",
                source=note.source,
                token_estimate=_estimate_tokens(note.summary),
                budget_impact=len(note.summary),
                confidence=note.confidence,
                safety_policy="request_copy_only",
            )
            stale = " stale/uncertain" if note.masking and note.masking.stale else ""
            coverage = ""
            if note.masking and note.masking.occurrence_count > 1:
                coverage = f"; sources: {', '.join(note.masking.source_pointer_ids)}"
            lines.append(
                f"- {note.kind}{stale}: {note.summary} [source: {note.source.pointer_id}{coverage}]"
            )
        if _has_conflicting_constraints(selected):
            lines.append("- uncertainty: constraints may conflict; verify against the latest user turn before acting")
        lines.extend(need_lines)
        return "\n".join(lines)

    def _sense_context_needs(
        self,
        selected: list[ZenNote],
        *,
        user_message: str,
        current_turn_user_idx: int | None = None,
        fallback_source: ZenSourcePointer | None = None,
    ) -> list[ZenContextNeed]:
        if not self._zen_config["proactive_sensing_enabled"]:
            self._record_trace(
                action="deferred",
                reason_code="proactive_sensing_disabled",
                safety_policy="operator_control",
            )
            return []
        needs: list[ZenContextNeed] = []
        prior_selected = [
            note for note in selected
            if current_turn_user_idx is None or note.source.message_index != current_turn_user_idx
        ]
        selected_text = " ".join(note.summary for note in prior_selected).lower()
        trigger = _latest_source(selected) or fallback_source

        repeated_failed = _repeated_failed_fingerprint(self._zen_notes)
        if repeated_failed and trigger is not None:
            needs.append(self._make_context_need(
                reason="repeated_failed_action",
                confidence=0.92,
                source=trigger,
                desired_context_type="failed_path",
                risk_level="medium",
                acquisition_policy="provider_allowed",
            ))

        if _CONTINUE_RE.search(user_message) and not any(
            note.kind in {"plan_anchor", "constraint", "user_guidance"} for note in prior_selected
        ):
            needs.append(self._make_context_need(
                reason="continue_without_prior_goal",
                confidence=0.78,
                source=trigger,
                desired_context_type="prior_goal",
                risk_level="medium",
                acquisition_policy="provider_allowed",
            ))

        if any(note.kind == "evidence" and note.source.pointer_id not in self._zen_source_pointers for note in selected):
            needs.append(self._make_context_need(
                reason="source_claim_missing_evidence",
                confidence=0.86,
                source=trigger,
                desired_context_type="source_evidence",
                risk_level="high",
                acquisition_policy="provider_blocked",
            ))

        missing_refs = [
            ref for ref in _artifact_references(user_message)
            if ref.lower() not in selected_text
        ]
        if missing_refs:
            needs.append(self._make_context_need(
                reason="missing_referenced_artifact",
                confidence=0.82,
                source=trigger,
                desired_context_type="referenced_artifact",
                risk_level="medium",
                acquisition_policy="provider_allowed",
            ))

        if _VERIFY_RE.search(user_message) and not any(
            term in selected_text
            for term in ("validate", "validated", "verification", "pytest", "rollback", "merge order", "openspec")
        ):
            needs.append(self._make_context_need(
                reason="missing_verification_context",
                confidence=0.88,
                source=trigger,
                desired_context_type="verification_context",
                risk_level="high",
                acquisition_policy="provider_blocked",
            ))
        return [need for need in needs if need.need_id in self._zen_context_need_seen]

    def _make_context_need(
        self,
        *,
        reason: str,
        confidence: float,
        source: ZenSourcePointer | None,
        desired_context_type: str,
        risk_level: str,
        acquisition_policy: str,
    ) -> ZenContextNeed:
        source_id = source.pointer_id if source is not None else ""
        base = f"{reason}:{source_id}:{desired_context_type}:{risk_level}"
        need_id = hashlib.sha256(base.encode("utf-8", errors="replace")).hexdigest()[:12]
        if need_id in self._zen_context_need_seen:
            return next(need for need in self._zen_context_needs if need.need_id == need_id)
        trace_id = f"context_need_{reason}"
        need = ZenContextNeed(
            need_id=need_id,
            reason=reason,
            confidence=round(max(0.0, min(1.0, confidence)), 2),
            triggering_source=source_id,
            desired_context_type=desired_context_type,
            risk_level=risk_level if risk_level in _NEED_RISK_LEVELS else "medium",
            acquisition_policy=acquisition_policy if acquisition_policy in _NEED_POLICIES else "advisory",
            trace_id=trace_id,
            token_budget=int(self._zen_config["context_need_token_budget"]),
            lifecycle_state="detected",
        )
        self._zen_context_needs.append(need)
        self._zen_context_need_seen.add(need_id)
        self._zen_metrics.context_need_count = len(self._zen_context_needs)
        self._record_trace(
            action="deferred",
            reason_code=trace_id,
            source=source,
            token_estimate=need.token_budget,
            safety_policy="context_need_detected",
            confidence="inferred",
        )
        return need

    def _context_need_brief_lines(self, needs: list[ZenContextNeed]) -> list[str]:
        lines: list[str] = []
        for need in needs[:4]:
            resolved = self._maybe_resolve_context_need(need)
            if resolved:
                for item in resolved:
                    label = "resolved uncertain" if item.uncertain else "resolved"
                    lines.append(
                        f"- needed_context {label}: {item.summary} [source: {item.source_id}; need: {need.need_id}]"
                    )
                continue
            advised = self._replace_context_need(need, lifecycle_state="advised")
            self._zen_metrics.context_need_advisory_count += 1
            self._record_trace(
                action="selected",
                reason_code=f"context_need_advisory_{need.reason}",
                token_estimate=need.token_budget,
                safety_policy="advisory_only",
                confidence="inferred",
            )
            lines.append(
                f"- needed_context advisory: {advised.desired_context_type} needed because {advised.reason} "
                f"[need: {advised.need_id}; risk: {advised.risk_level}; policy: {advised.acquisition_policy}]"
            )
        return lines

    def _maybe_resolve_context_need(self, need: ZenContextNeed) -> list[ZenContextSlice]:
        if not self._zen_config["context_need_provider_enabled"] or self._zen_context_need_provider is None:
            return []
        if need.confidence < float(self._zen_config["context_need_min_confidence"]):
            return []
        if need.risk_level == "high" or need.acquisition_policy != "provider_allowed":
            return []
        self._zen_metrics.provider_call_count += 1
        slices: list[ZenContextSlice] = []
        budget_used = 0
        saw_candidate = False
        try:
            provider_items = list(self._zen_context_need_provider.resolve_context_need(need, need.token_budget))
        except Exception as exc:
            self._record_provider_outcome(need, _provider_outcome_from_exception(exc))
            return []
        if not provider_items:
            self._record_provider_outcome(need, ZenProviderOutcome(status="empty", reason="provider_returned_no_slices"))
            return []
        for raw in provider_items:
            saw_candidate = True
            outcome = _coerce_provider_outcome(raw)
            if outcome is not None:
                self._record_provider_outcome(need, outcome)
                if outcome.status != "stale" or not outcome.summary:
                    continue
            item = _coerce_context_slice(need.need_id, raw)
            if not item or not item.source_id:
                self._zen_metrics.rejected_slice_count += 1
                self._replace_context_need(need, lifecycle_state="rejected")
                self._record_trace(
                    action="dropped",
                    reason_code=f"context_need_rejected_{need.reason}",
                    token_estimate=need.token_budget,
                    safety_policy="provider_slice_rejected",
                    confidence="inferred",
                )
                continue
            if item.token_estimate > need.token_budget - budget_used:
                self._zen_metrics.rejected_slice_count += 1
                self._replace_context_need(need, lifecycle_state="rejected")
                self._record_trace(
                    action="dropped",
                    reason_code=f"context_need_rejected_{need.reason}",
                    token_estimate=item.token_estimate,
                    safety_policy="provider_budget_rejected",
                    confidence="inferred",
                )
                continue
            slices.append(item)
            budget_used += item.token_estimate
        if not slices:
            if saw_candidate:
                self._replace_context_need(need, lifecycle_state="rejected")
            return []
        self._zen_metrics.provider_budget_used += budget_used
        self._zen_metrics.injected_slice_count += len(slices)
        self._zen_context_slices.extend(slices)
        self._replace_context_need(need, lifecycle_state="resolved")
        self._record_trace(
            action="selected",
            reason_code=f"context_need_resolved_{need.reason}",
            token_estimate=budget_used,
            budget_impact=budget_used,
            safety_policy="provider_bounded",
            confidence="observed",
        )
        return slices

    def _record_provider_outcome(self, need: ZenContextNeed, outcome: ZenProviderOutcome) -> None:
        status = outcome.status if outcome.status in _PROVIDER_OUTCOMES else "rejected"
        if status == "denied":
            self._zen_metrics.provider_denied_count += 1
            lifecycle = "advised"
        elif status == "empty":
            self._zen_metrics.provider_empty_count += 1
            lifecycle = "advised"
        elif status == "stale":
            self._zen_metrics.provider_stale_count += 1
            lifecycle = need.lifecycle_state
        elif status == "unsupported":
            lifecycle = "advised"
        else:
            self._zen_metrics.rejected_slice_count += 1
            lifecycle = "rejected"
        if lifecycle != need.lifecycle_state:
            self._replace_context_need(need, lifecycle_state=lifecycle)
        self._record_trace(
            action="deferred" if status in {"denied", "empty", "unsupported", "stale"} else "dropped",
            reason_code=f"context_need_provider_{status}_{need.reason}",
            token_estimate=max(0, int(outcome.token_estimate)),
            safety_policy=f"provider_{status}",
            confidence="uncertain" if status == "stale" else "inferred",
        )

    def _replace_context_need(self, need: ZenContextNeed, *, lifecycle_state: str) -> ZenContextNeed:
        if lifecycle_state not in _NEED_STATES:
            lifecycle_state = "rejected"
        updated = ZenContextNeed(
            need_id=need.need_id,
            reason=need.reason,
            confidence=need.confidence,
            triggering_source=need.triggering_source,
            desired_context_type=need.desired_context_type,
            risk_level=need.risk_level,
            acquisition_policy=need.acquisition_policy,
            trace_id=need.trace_id,
            token_budget=need.token_budget,
            lifecycle_state=lifecycle_state,
        )
        self._zen_context_needs = [
            updated if item.need_id == need.need_id else item
            for item in self._zen_context_needs
        ]
        return updated

    @property
    def _zen_large_observation_chars(self) -> int:
        return _MASKING_THRESHOLDS[self._zen_config["masking_strictness"]]

    def _record_trace(
        self,
        *,
        action: str,
        reason_code: str,
        source: ZenSourcePointer | None = None,
        token_estimate: int = 0,
        safety_policy: str = "privacy_safe_trace",
        budget_impact: int = 0,
        confidence: str = "observed",
    ) -> None:
        if action not in _TRACE_ACTIONS:
            action = "deferred"
            reason_code = "invalid_trace_action"
        trace = ZenDecisionTrace(
            action=action,
            reason_code=reason_code,
            confidence=confidence if confidence in {"observed", "inferred", "uncertain"} else "uncertain",
            input_source=source.pointer_id if source is not None else "",
            token_estimate=max(0, int(token_estimate)),
            safety_policy=safety_policy,
            budget_impact=int(budget_impact),
            redacted=True,
        )
        self._zen_decision_traces.append(trace)
        if action == "kept":
            self._zen_metrics.kept_count += 1
        elif action == "compressed":
            self._zen_metrics.compressed_count += 1
        elif action == "masked":
            self._zen_metrics.masked_count += 1
        elif action == "dropped":
            self._zen_metrics.dropped_count += 1
        elif action == "deferred":
            self._zen_metrics.deferred_count += 1
        elif action == "selected":
            self._zen_metrics.selected_count += 1
        self._zen_metrics.trace_event_count = len(self._zen_decision_traces)
        self._zen_metrics.trace_complete = all(_trace_complete(item) for item in self._zen_decision_traces)
        self._zen_metrics.privacy_safe = all(item.redacted and _trace_privacy_safe(item) for item in self._zen_decision_traces)


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


def _normalize_zen_config(config: Dict[str, Any] | None) -> dict[str, Any]:
    raw = config if isinstance(config, dict) else {}
    strictness = str(raw.get("masking_strictness", "standard") or "standard").lower()
    if strictness not in _MASKING_THRESHOLDS:
        strictness = "standard"
    verbosity = str(raw.get("trace_verbosity", "standard") or "standard").lower()
    if verbosity not in _TRACE_VERBOSITIES:
        verbosity = "standard"
    bypass_raw = raw.get("bypass_session_ids", ())
    if isinstance(bypass_raw, str):
        bypass_session_ids = frozenset({bypass_raw}) if bypass_raw else frozenset()
    else:
        try:
            bypass_session_ids = frozenset(str(item) for item in bypass_raw if str(item))
        except TypeError:
            bypass_session_ids = frozenset()
    return {
        "enabled": bool(raw.get("enabled", True)),
        "force_compressor_fallback": bool(raw.get("force_compressor_fallback", False)),
        "masking_strictness": strictness,
        "trace_verbosity": verbosity,
        "bypass_session_ids": bypass_session_ids,
        "proactive_sensing_enabled": bool(raw.get("proactive_sensing_enabled", True)),
        "context_need_provider_enabled": bool(raw.get("context_need_provider_enabled", False)),
        "context_need_token_budget": max(40, int(raw.get("context_need_token_budget", 240) or 240)),
        "context_need_min_confidence": float(raw.get("context_need_min_confidence", 0.7) or 0.7),
    }


def _history_chars(messages: List[Dict[str, Any]]) -> int:
    return sum(len(_content_to_text(msg.get("content", ""))) for msg in messages)


def _estimate_tokens(text: str) -> int:
    compact = _compact(text)
    if not compact:
        return 0
    return max(1, (len(compact) + 3) // 4)


def _latest_source(notes: list[ZenNote]) -> ZenSourcePointer | None:
    if not notes:
        return None
    return max(notes, key=lambda note: note.source.message_index).source


def _repeated_failed_fingerprint(notes: list[ZenNote]) -> str:
    seen: set[str] = set()
    for note in notes:
        if note.kind != "failed_path":
            continue
        fingerprint = note.masking.payload_fingerprint if note.masking else _observation_fingerprint(note.summary)
        if fingerprint in seen:
            return fingerprint
        seen.add(fingerprint)
    return ""


def _artifact_references(text: str) -> list[str]:
    refs = _paths(text)
    for match in _PR_RE.finditer(text):
        refs.append(match.group(0))
    return refs[:6]


def _coerce_provider_outcome(raw: ZenContextSlice | ZenProviderOutcome | Dict[str, Any] | str) -> ZenProviderOutcome | None:
    if isinstance(raw, ZenProviderOutcome):
        return raw
    if not isinstance(raw, dict):
        return None
    status = str(raw.get("status", "") or raw.get("outcome", "")).strip().lower()
    if status not in _PROVIDER_OUTCOMES:
        return None
    return ZenProviderOutcome(
        status=status,
        reason=str(raw.get("reason", "") or raw.get("error", "")),
        summary=str(raw.get("summary", "")),
        source_id=str(raw.get("source_id", "") or raw.get("source", "")),
        token_estimate=int(raw.get("token_estimate", 0) or 0),
    )


def _provider_outcome_from_exception(exc: Exception) -> ZenProviderOutcome:
    code = str(getattr(exc, "code", "") or "").strip().lower()
    if code in {"forbidden", "missing_authz_policy", "unauthorized", "authz_denied", "permission_denied"}:
        return ZenProviderOutcome(status="denied", reason=code or "provider_denied")
    if code in {"not_found", "empty"}:
        return ZenProviderOutcome(status="empty", reason=code)
    if code in {"stale", "stale_pointer", "stale_result"}:
        return ZenProviderOutcome(status="stale", reason=code)
    return ZenProviderOutcome(status="rejected", reason=code or exc.__class__.__name__)


def _coerce_context_slice(
    need_id: str,
    raw: ZenContextSlice | ZenProviderOutcome | Dict[str, Any] | str,
) -> ZenContextSlice | None:
    if isinstance(raw, ZenContextSlice):
        return raw if raw.need_id == need_id else ZenContextSlice(
            need_id=need_id,
            summary=raw.summary,
            source_id=raw.source_id,
            token_estimate=raw.token_estimate,
            uncertain=raw.uncertain,
        )
    if isinstance(raw, ZenProviderOutcome):
        if raw.status != "stale":
            return None
        summary = _sentence(raw.summary, _MASKED_SUMMARY_CHARS)
        source_id = raw.source_id
        token_estimate = raw.token_estimate or _estimate_tokens(summary)
        uncertain = True
    if isinstance(raw, dict):
        summary = _sentence(str(raw.get("summary", "")), _MASKED_SUMMARY_CHARS)
        source_id = str(raw.get("source_id", "") or raw.get("source", ""))
        token_estimate = int(raw.get("token_estimate", _estimate_tokens(summary)) or 0)
        status = str(raw.get("status", "") or raw.get("outcome", "")).strip().lower()
        uncertain = bool(raw.get("uncertain") or raw.get("stale") or status == "stale")
    else:
        if not isinstance(raw, ZenProviderOutcome):
            summary = _sentence(str(raw), _MASKED_SUMMARY_CHARS)
            source_id = ""
            token_estimate = _estimate_tokens(summary)
            uncertain = False
    if not summary:
        return None
    return ZenContextSlice(
        need_id=need_id,
        summary=summary,
        source_id=source_id,
        token_estimate=max(1, token_estimate),
        uncertain=uncertain,
    )


def _trace_complete(trace: ZenDecisionTrace) -> bool:
    return bool(
        trace.action
        and trace.reason_code
        and trace.confidence
        and trace.safety_policy
        and trace.action in _TRACE_ACTIONS
    )


def _trace_privacy_safe(trace: ZenDecisionTrace) -> bool:
    raw_markers = ("\n", "traceback", "large output", "detail line", "secret", "api_key")
    fields = (trace.reason_code, trace.input_source, trace.safety_policy)
    return not any(marker in field.lower() for field in fields for marker in raw_markers)


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
    large_threshold: int = _LARGE_OBSERVATION_CHARS,
) -> ZenObservationClassification:
    compact = _compact(text)
    fingerprint = _observation_fingerprint(compact)
    stale = current_turn_user_idx - idx > _STALE_MESSAGE_AGE
    if _FAILURE_RE.search(compact):
        observation_class = "failed"
    elif fingerprint in seen_fingerprints:
        observation_class = "repeated"
    elif len(compact) > large_threshold:
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
    "ZenContextNeed",
    "ZenContextNeedProvider",
    "ZenContextSlice",
    "ZenDecisionTrace",
    "ZenMaskingMetadata",
    "ZenMetrics",
    "ZenNote",
    "ZenObservationClassification",
    "ZenProviderOutcome",
    "ZenSourcePointer",
    "register",
]
