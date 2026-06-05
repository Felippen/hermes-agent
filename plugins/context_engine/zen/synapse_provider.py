"""Read-only provider adapter from Zen context needs to Synapse retrieval."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from plugins.context_engine.zen import ZenContextNeed, ZenContextSlice, ZenProviderOutcome


class SynapseRetrievalClient(Protocol):
    def retrieve_document(self, request: Any) -> Any:
        ...


@dataclass(frozen=True)
class SynapseProviderRoute:
    method_name: str
    domain: str
    query: str


class SynapseContextNeedProvider:
    """Resolve eligible Zen context needs through an injected Synapse client."""

    def __init__(
        self,
        *,
        client: SynapseRetrievalClient,
        agent_profile: Any,
        limit: int = 2,
        prefer_memory: bool = True,
    ) -> None:
        self.client = client
        self.agent_profile = agent_profile
        self.limit = max(1, min(5, int(limit or 2)))
        self.prefer_memory = bool(prefer_memory)

    def resolve_context_need(self, need: ZenContextNeed, budget: int) -> list[ZenContextSlice | ZenProviderOutcome]:
        route = self._route_for_need(need)
        if route is None:
            return [ZenProviderOutcome(status="unsupported", reason=need.reason)]
        request = {
            "query": route.query,
            "agent_profile": self.agent_profile,
            "domain": route.domain,
            "limit": self.limit,
            "token_budget": max(1, int(budget)),
        }
        try:
            response = getattr(self.client, route.method_name)(request)
        except Exception as exc:
            return [_outcome_from_exception(exc)]
        return self._coerce_response(need, response, budget)

    def _route_for_need(self, need: ZenContextNeed) -> SynapseProviderRoute | None:
        reason = need.reason
        if reason == "missing_referenced_artifact":
            return SynapseProviderRoute(
                method_name="retrieve_document",
                domain="documents",
                query=_bounded_query("referenced artifact context", need),
            )
        if reason in {"continue_without_prior_goal", "repeated_failed_action"}:
            if self.prefer_memory and hasattr(self.client, "retrieve_memory"):
                return SynapseProviderRoute(
                    method_name="retrieve_memory",
                    domain="memory",
                    query=_bounded_query("prior task intent and decisions", need),
                )
            return SynapseProviderRoute(
                method_name="retrieve_document",
                domain="documents",
                query=_bounded_query("prior task intent and decisions", need),
            )
        return None

    def _coerce_response(
        self,
        need: ZenContextNeed,
        response: Any,
        budget: int,
    ) -> list[ZenContextSlice | ZenProviderOutcome]:
        status = _field(response, "status")
        if str(status).lower() in {"denied", "forbidden", "unauthorized"}:
            return [ZenProviderOutcome(status="denied", reason=str(status))]
        slices = list(_field(response, "slices") or [])
        if not slices:
            return [ZenProviderOutcome(status="empty", reason="no_synapse_slices")]

        results: list[ZenContextSlice | ZenProviderOutcome] = []
        remaining_budget = max(1, int(budget))
        for item in slices:
            summary = _slice_summary(item)
            source_id = _slice_source_id(item)
            token_estimate = min(_estimate_tokens(summary), remaining_budget + 1)
            stale = bool(_field(item, "stale") or _field(item, "uncertain") or _field(item, "is_stale"))
            if not source_id:
                results.append(ZenProviderOutcome(status="rejected", reason="missing_source"))
                continue
            if token_estimate > remaining_budget:
                results.append(ZenProviderOutcome(status="rejected", reason="budget_exceeded", source_id=source_id, token_estimate=token_estimate))
                continue
            if stale:
                results.append(
                    ZenProviderOutcome(
                        status="stale",
                        reason="synapse_stale",
                        summary=summary,
                        source_id=source_id,
                        token_estimate=token_estimate,
                    )
                )
                remaining_budget -= token_estimate
                continue
            results.append(
                ZenContextSlice(
                    need_id=need.need_id,
                    summary=summary,
                    source_id=source_id,
                    token_estimate=token_estimate,
                    uncertain=False,
                )
            )
            remaining_budget -= token_estimate
        if not results:
            return [ZenProviderOutcome(status="empty", reason="no_usable_synapse_slices")]
        return results


def _bounded_query(prefix: str, need: ZenContextNeed) -> str:
    parts = [
        prefix,
        f"reason:{need.reason}",
        f"type:{need.desired_context_type}",
        f"source:{need.triggering_source}" if need.triggering_source else "",
    ]
    return _sentence(" ".join(part for part in parts if part), 240)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _slice_summary(item: Any) -> str:
    title = str(_field(item, "title", "") or "")
    fields = _field(item, "fields", {}) or {}
    if isinstance(fields, dict):
        field_text = " ".join(str(value) for value in fields.values() if value)
    else:
        field_text = str(fields)
    return _sentence(" ".join(part for part in (title, field_text) if part), 280)


def _slice_source_id(item: Any) -> str:
    source_id = str(_field(item, "source_id", "") or "")
    if source_id:
        return source_id
    pointers = _field(item, "pointers", []) or []
    if pointers:
        pointer = pointers[0]
        pointer_id = _field(pointer, "id", "")
        if pointer_id:
            return str(pointer_id)
    entity_id = _field(item, "entity_id", "")
    if entity_id:
        return f"synapse:{entity_id}"
    return ""


def _outcome_from_exception(exc: Exception) -> ZenProviderOutcome:
    code = str(getattr(exc, "code", "") or "").strip().lower()
    if code in {"forbidden", "missing_authz_policy", "unauthorized", "authz_denied", "permission_denied"}:
        return ZenProviderOutcome(status="denied", reason=code or "denied")
    if code in {"not_found", "empty"}:
        return ZenProviderOutcome(status="empty", reason=code)
    if code in {"stale", "stale_pointer", "stale_result"}:
        return ZenProviderOutcome(status="stale", reason=code)
    return ZenProviderOutcome(status="rejected", reason=code or exc.__class__.__name__)


def _estimate_tokens(text: str) -> int:
    text = _sentence(text, 280)
    if not text:
        return 0
    return max(1, (len(text) + 3) // 4)


def _sentence(text: str, limit: int) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


__all__ = ["SynapseContextNeedProvider", "SynapseProviderRoute", "SynapseRetrievalClient"]
