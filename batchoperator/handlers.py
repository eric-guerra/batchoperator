"""kopf wiring: thin adapters between Kubernetes events and the pure planner."""
from __future__ import annotations

import time

import kopf
import kubernetes
from kubernetes.client.rest import ApiException

from .planner import CreateJob, DeleteJob, SpecError, job_manifest, reconcile

GROUP, VERSION, PLURAL = "batch.ericguerra.dev", "v1", "shardedbatches"
LABEL = "batch.ericguerra.dev/name"


@kopf.on.startup()
def configure(settings: kopf.OperatorSettings, **_):
    settings.persistence.finalizer = "batch.ericguerra.dev/finalizer"
    settings.posting.level = "INFO"
    settings.watching.server_timeout = 270
    # Leader election via kopf's peering so only one replica reconciles at a time.
    settings.peering.name = "batchoperator"
    settings.peering.mandatory = True
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()


def observed_jobs(namespace: str, name: str) -> dict[str, str]:
    batch = kubernetes.client.BatchV1Api()
    out = {}
    for j in batch.list_namespaced_job(namespace, label_selector=f"{LABEL}={name}").items:
        st = j.status
        if st.succeeded:
            out[j.metadata.name] = "Succeeded"
        elif st.failed:
            out[j.metadata.name] = "Failed"
        else:
            out[j.metadata.name] = "Running"
    return out


def apply(plan, *, name, namespace, uid, spec, logger, body):
    batch = kubernetes.client.BatchV1Api()
    for a in plan.actions:
        if isinstance(a, CreateJob):
            try:
                batch.create_namespaced_job(namespace, job_manifest(name, namespace, uid, spec, a))
                logger.info(f"created {a.job_name}")
            except ApiException as e:
                if e.status != 409:  # already exists: a previous reconcile got this far before crashing
                    raise kopf.TemporaryError(f"creating {a.job_name}: {e.reason}", delay=10)
        elif isinstance(a, DeleteJob):
            try:
                batch.delete_namespaced_job(a.job_name, namespace, propagation_policy="Background")
                logger.info(f"deleted {a.job_name}: {a.reason}")
            except ApiException as e:
                if e.status != 404:
                    raise kopf.TemporaryError(f"deleting {a.job_name}: {e.reason}", delay=10)
    for etype, reason, message in plan.events:
        kopf.event(body, type=etype, reason=reason, message=message)


def _reconcile(spec, status, name, namespace, uid, logger, body, patch):
    try:
        plan = reconcile(name, dict(spec), dict(status or {}).get("batch"), observed_jobs(namespace, name), time.time())
    except SpecError as e:
        patch.status["batch"] = {"phase": "Invalid", "message": str(e)}
        raise kopf.PermanentError(str(e))
    apply(plan, name=name, namespace=namespace, uid=uid, spec=dict(spec), logger=logger, body=body)
    patch.status["batch"] = plan.status
    if plan.requeue_after:
        raise kopf.TemporaryError("waiting for shard backoff", delay=plan.requeue_after)


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL, field="spec")
@kopf.on.resume(GROUP, VERSION, PLURAL)
def on_change(spec, status, name, namespace, uid, logger, body, patch, **_):
    _reconcile(spec, status, name, namespace, uid, logger, body, patch)


@kopf.on.event("batch", "v1", "jobs", labels={LABEL: kopf.PRESENT})
def on_job_event(body, event, logger, **_):
    """A child Job changed: poke the owner so it reconciles promptly (instead of waiting for a timer)."""
    owner = body["metadata"]["labels"][LABEL]
    ns = body["metadata"]["namespace"]
    if event["type"] in ("MODIFIED", "DELETED"):
        api = kubernetes.client.CustomObjectsApi()
        try:
            api.patch_namespaced_custom_object(GROUP, VERSION, ns, PLURAL, owner,
                                               {"metadata": {"annotations": {"batch.ericguerra.dev/poke": str(time.time())}}})
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"could not poke {owner}: {e.reason}")


@kopf.on.field(GROUP, VERSION, PLURAL, field="metadata.annotations")
def on_poke(spec, status, name, namespace, uid, logger, body, patch, **_):
    phase = ((status or {}).get("batch") or {}).get("phase")
    if phase not in ("Succeeded", "Failed", "Invalid"):
        _reconcile(spec, status, name, namespace, uid, logger, body, patch)


@kopf.timer(GROUP, VERSION, PLURAL, interval=60, idle=30)
def safety_net(spec, status, name, namespace, uid, logger, body, patch, **_):
    """Missed watch events happen (API server restarts); the timer guarantees eventual progress."""
    phase = ((status or {}).get("batch") or {}).get("phase")
    if phase not in ("Succeeded", "Failed", "Invalid"):
        _reconcile(spec, status, name, namespace, uid, logger, body, patch)
