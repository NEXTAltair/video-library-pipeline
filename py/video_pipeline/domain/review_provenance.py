"""Review acceptance is not evidence of a human confirmation.

Legacy human flags remain readable, but new agent decisions always explicitly
clear them. Historical rows require externally supplied, row-scoped evidence.
"""
from __future__ import annotations


def acceptance_provenance(acceptance="agent_accepted", human_confirmation=None):
    if acceptance not in {"agent_accepted", "human_confirmed"}:
        raise ValueError("acceptance must be agent_accepted or human_confirmed")
    if acceptance == "human_confirmed":
        if not isinstance(human_confirmation, dict) or any(
            not isinstance(human_confirmation.get(key), str) or not human_confirmation[key].strip()
            for key in ("confirmed_by", "evidence")
        ):
            raise ValueError("human confirmation requires confirmed_by and evidence")
    elif human_confirmation:
        raise ValueError("agent acceptance cannot carry human confirmation")
    return {"review_acceptance": acceptance, "human_reviewed": acceptance == "human_confirmed",
            "human_confirmation": dict(human_confirmation) if human_confirmation else None}


def is_agent_accepted_metadata(source, md=None):
    return str(source or "").strip().lower() == "agent_accepted" or (
        isinstance(md, dict) and md.get("review_acceptance") == "agent_accepted")
