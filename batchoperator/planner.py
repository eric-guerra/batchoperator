"""Pure reconcile logic for ShardedBatch resources.

A ShardedBatch fans one container out over many shards (stores, regions, date partitions), runs
at most `parallelism` Kubernetes Jobs at a time, retries failed shards with backoff, and reports
progress in `.status`. Everything here is a function of (spec, status, observed jobs, now) so the
operator's behaviour is unit-testable without a cluster.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

PENDING, RUNNING, SUCCEEDED, FAILED, BACKOFF = "Pending", "Running", "Succeeded", "Failed", "Backoff"
TERMINAL = {SUCCEEDED, FAILED}


@dataclass(frozen=True)
class CreateJob:
    shard: str
    attempt: int
    job_name: str


@dataclass(frozen=True)
class DeleteJob:
    job_name: str
    reason: str


@dataclass
class Plan:
    actions: list[Any] = field(default_factory=list)
    status: dict = field(default_factory=dict)
    requeue_after: float | None = None
    events: list[tuple[str, str, str]] = field(default_factory=list)  # (type, reason, message)


class SpecError(ValueError):
    pass


def validate(spec: dict) -> dict:
    """Normalise spec and apply defaults; raise SpecError with a user-facing message."""
    if not spec.get("image"):
        raise SpecError("spec.image is required")
    shards = spec.get("shards")
    if isinstance(shards, int):
        if shards < 1 or shards > 10_000:
            raise SpecError("spec.shards must be 1..10000")
        shard_ids = [str(i) for i in range(shards)]
    elif isinstance(shards, list) and shards:
        shard_ids = [str(s) for s in shards]
        if len(set(shard_ids)) != len(shard_ids):
            raise SpecError("spec.shards contains duplicates")
    else:
        raise SpecError("spec.shards must be a positive integer or a non-empty list")
    parallelism = int(spec.get("parallelism", 5))
    if parallelism < 1:
        raise SpecError("spec.parallelism must be >= 1")
    return {
        "image": spec["image"],
        "command": list(spec.get("command", [])),
        "env": dict(spec.get("env", {})),
        "shards": shard_ids,
        "parallelism": parallelism,
        "maxRetries": int(spec.get("maxRetries", 2)),
        "backoffSeconds": float(spec.get("backoffSeconds", 30)),
        "failurePolicy": spec.get("failurePolicy", "Continue"),  # Continue | FailFast
        "suspend": bool(spec.get("suspend", False)),
        "resources": spec.get("resources", {"requests": {"cpu": "250m", "memory": "256Mi"}}),
        "activeDeadlineSeconds": int(spec.get("activeDeadlineSeconds", 3600)),
    }


def job_name(batch: str, shard: str, attempt: int) -> str:
    """DNS-1123 label (≤63 chars), deterministic so a re-run of reconcile never double-creates."""
    safe = re.sub(r"[^a-z0-9-]", "-", f"{batch}-{shard}".lower()).strip("-")
    suffix = f"-a{attempt}"
    if len(safe) + len(suffix) > 63:
        digest = hashlib.sha1(f"{batch}/{shard}".encode()).hexdigest()[:8]
        safe = safe[: 63 - len(suffix) - 9].rstrip("-") + "-" + digest
    return safe + suffix


def reconcile(name: str, raw_spec: dict, status: dict | None, jobs: dict[str, str], now: float) -> Plan:
    """
    jobs: observed Job name -> one of Running / Succeeded / Failed (owned by this batch).
    status: previous .status (may be None on first reconcile).
    """
    spec = validate(raw_spec)
    prev = (status or {}).get("shards", {})
    shards: dict[str, dict] = {}
    plan = Plan()

    # 1. Fold observed Job states into per-shard state.
    for sid in spec["shards"]:
        s = dict(prev.get(sid) or {"state": PENDING, "attempts": 0})
        jn = s.get("jobName")
        observed = jobs.get(jn) if jn else None
        if s["state"] == RUNNING:
            if observed == SUCCEEDED:
                s["state"] = SUCCEEDED
                s["finishedAt"] = now
                plan.events.append(("Normal", "ShardSucceeded", f"shard {sid} succeeded (attempt {s['attempts']})"))
            elif observed == FAILED or observed is None:
                # A Job that vanished (deleted by hand, node lost before status recorded) counts as a failure.
                why = "job failed" if observed == FAILED else "job disappeared"
                if s["attempts"] <= spec["maxRetries"]:
                    s["state"] = BACKOFF
                    s["retryAt"] = now + spec["backoffSeconds"] * (2 ** (s["attempts"] - 1))
                    plan.events.append(("Warning", "ShardRetry", f"shard {sid} {why}; retry {s['attempts']}/{spec['maxRetries']}"))
                else:
                    s["state"] = FAILED
                    s["finishedAt"] = now
                    plan.events.append(("Warning", "ShardFailed", f"shard {sid} {why}; giving up after {s['attempts']} attempts"))
                s["lastError"] = why
        if s["state"] == BACKOFF and now >= s.get("retryAt", 0):
            s["state"] = PENDING
        shards[sid] = s

    # 2. Shards removed from the spec: stop their jobs.
    for sid, s in prev.items():
        if sid not in shards and s.get("state") == RUNNING and s.get("jobName"):
            plan.actions.append(DeleteJob(s["jobName"], f"shard {sid} removed from spec"))

    fail_fast = spec["failurePolicy"] == "FailFast" and any(s["state"] == FAILED for s in shards.values())
    if fail_fast:
        for sid, s in shards.items():
            if s["state"] == RUNNING:
                plan.actions.append(DeleteJob(s["jobName"], "failFast: another shard failed"))
                s["state"] = FAILED
                s["lastError"] = "cancelled (failFast)"
            elif s["state"] in (PENDING, BACKOFF):
                s["state"] = FAILED
                s["lastError"] = "not started (failFast)"

    # 3. Start pending shards within the parallelism budget, in spec order.
    if not spec["suspend"] and not fail_fast:
        running = sum(1 for s in shards.values() if s["state"] == RUNNING)
        for sid in spec["shards"]:
            if running >= spec["parallelism"]:
                break
            s = shards[sid]
            if s["state"] != PENDING:
                continue
            s["attempts"] += 1
            s["jobName"] = job_name(name, sid, s["attempts"])
            s["state"] = RUNNING
            s["startedAt"] = now
            plan.actions.append(CreateJob(sid, s["attempts"], s["jobName"]))
            running += 1

    # 4. Status summary + when to look again.
    counts = {k: 0 for k in (PENDING, RUNNING, BACKOFF, SUCCEEDED, FAILED)}
    for s in shards.values():
        counts[s["state"]] += 1
    total = len(shards)
    done = counts[SUCCEEDED] + counts[FAILED]
    if done == total:
        phase = "Failed" if counts[FAILED] else "Succeeded"
    elif spec["suspend"]:
        phase = "Suspended"
    else:
        phase = "Running" if counts[RUNNING] or counts[BACKOFF] or counts[PENDING] else "Pending"
    if phase in ("Succeeded", "Failed") and (status or {}).get("phase") != phase:
        plan.events.append(("Normal" if phase == "Succeeded" else "Warning", f"Batch{phase}",
                            f"{counts[SUCCEEDED]}/{total} shards succeeded"))

    retry_times = [s["retryAt"] for s in shards.values() if s["state"] == BACKOFF]
    if retry_times:
        plan.requeue_after = max(1.0, min(retry_times) - now)

    plan.status = {
        "phase": phase,
        "shards": shards,
        "counts": {k.lower(): v for k, v in counts.items()},
        "progress": f"{done}/{total}",
        "observedShards": total,
    }
    return plan


def job_manifest(batch_name: str, namespace: str, uid: str, spec: dict, action: CreateJob) -> dict:
    """The Job for one shard attempt. backoffLimit=0: retries are ours, with our backoff and accounting."""
    spec = validate(spec)
    env = [{"name": k, "value": str(v)} for k, v in sorted(spec["env"].items())]
    env += [
        {"name": "SHARD_ID", "value": action.shard},
        {"name": "SHARD_ATTEMPT", "value": str(action.attempt)},
        {"name": "SHARD_COUNT", "value": str(len(spec["shards"]))},
    ]
    labels = {"app.kubernetes.io/managed-by": "batchoperator", "batch.ericguerra.dev/name": batch_name,
              "batch.ericguerra.dev/shard": re.sub(r"[^A-Za-z0-9_.-]", "_", action.shard)[:63]}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": action.job_name,
            "namespace": namespace,
            "labels": labels,
            "ownerReferences": [{"apiVersion": "batch.ericguerra.dev/v1", "kind": "ShardedBatch", "name": batch_name,
                                 "uid": uid, "controller": True, "blockOwnerDeletion": True}],
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": spec["activeDeadlineSeconds"],
            "ttlSecondsAfterFinished": 86400,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [{
                        "name": "worker",
                        "image": spec["image"],
                        "command": spec["command"] or None,
                        "env": env,
                        "resources": spec["resources"],
                        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                            "capabilities": {"drop": ["ALL"]}},
                    }],
                },
            },
        },
    }
