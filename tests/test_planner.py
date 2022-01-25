import pytest

from batchoperator.planner import (
    BACKOFF, FAILED, PENDING, RUNNING, SUCCEEDED, CreateJob, DeleteJob, SpecError, job_manifest, job_name, reconcile,
)

SPEC = {"image": "registry/store-sync:1.8.0", "shards": ["S001", "S002", "S003", "S004"], "parallelism": 2,
        "maxRetries": 1, "backoffSeconds": 10}


def creates(plan):
    return [a.shard for a in plan.actions if isinstance(a, CreateJob)]


def run(spec, status, jobs, now):
    return reconcile("store-sync", spec, status, jobs, now)


def test_first_reconcile_respects_parallelism():
    p = run(SPEC, None, {}, 0)
    assert creates(p) == ["S001", "S002"]
    assert p.status["phase"] == "Running"
    assert p.status["counts"] == {"pending": 2, "running": 2, "backoff": 0, "succeeded": 0, "failed": 0}


def test_reconcile_is_idempotent_while_jobs_run():
    p1 = run(SPEC, None, {}, 0)
    jobs = {a.job_name: "Running" for a in p1.actions}
    p2 = run(SPEC, p1.status, jobs, 5)
    assert p2.actions == []
    assert p2.status["shards"] == p1.status["shards"]


def test_success_frees_a_slot_for_the_next_shard():
    p1 = run(SPEC, None, {}, 0)
    names = {a.shard: a.job_name for a in p1.actions}
    p2 = run(SPEC, p1.status, {names["S001"]: "Succeeded", names["S002"]: "Running"}, 5)
    assert creates(p2) == ["S003"]
    assert p2.status["shards"]["S001"]["state"] == SUCCEEDED
    assert ("Normal", "ShardSucceeded", "shard S001 succeeded (attempt 1)") in p2.events


def test_failure_backs_off_then_retries_with_new_job_name():
    p1 = run(SPEC, None, {}, 0)
    names = {a.shard: a.job_name for a in p1.actions}
    p2 = run(SPEC, p1.status, {names["S001"]: "Failed", names["S002"]: "Running"}, 100)
    s = p2.status["shards"]["S001"]
    assert s["state"] == BACKOFF and s["retryAt"] == 110
    assert creates(p2) == ["S003"]  # slot reused while S001 waits
    assert p2.requeue_after == 10

    jobs = {names["S002"]: "Running", p2.actions[0].job_name: "Running"}
    p3 = run(SPEC, p2.status, jobs, 105)
    assert creates(p3) == []  # still in backoff, and no free slot anyway
    jobs[names["S002"]] = "Succeeded"
    p4 = run(SPEC, p3.status, jobs, 111)
    assert creates(p4) == ["S001"]
    assert p4.status["shards"]["S001"]["jobName"].endswith("-a2")


def test_retries_exhausted_marks_shard_failed_and_batch_failed():
    spec = {**SPEC, "shards": ["S001"], "maxRetries": 0}
    p1 = run(spec, None, {}, 0)
    p2 = run(spec, p1.status, {p1.actions[0].job_name: "Failed"}, 1)
    assert p2.status["shards"]["S001"]["state"] == FAILED
    assert p2.status["phase"] == "Failed"
    assert any(e[1] == "BatchFailed" for e in p2.events)
    # the terminal event fires once
    assert not any(e[1] == "BatchFailed" for e in run(spec, p2.status, {}, 2).events)


def test_vanished_job_counts_as_a_failed_attempt():
    p1 = run({**SPEC, "shards": ["S001"]}, None, {}, 0)
    p2 = run({**SPEC, "shards": ["S001"]}, p1.status, {}, 1)
    assert p2.status["shards"]["S001"]["lastError"] == "job disappeared"


def test_fail_fast_cancels_running_and_pending():
    spec = {**SPEC, "maxRetries": 0, "failurePolicy": "FailFast"}
    p1 = run(spec, None, {}, 0)
    names = {a.shard: a.job_name for a in p1.actions}
    p2 = run(spec, p1.status, {names["S001"]: "Failed", names["S002"]: "Running"}, 1)
    assert [a for a in p2.actions if isinstance(a, DeleteJob)] == [DeleteJob(names["S002"], "failFast: another shard failed")]
    assert creates(p2) == []
    assert p2.status["phase"] == "Failed"
    assert {s["state"] for s in p2.status["shards"].values()} == {FAILED}


def test_suspend_stops_new_work_but_keeps_running_jobs():
    p1 = run(SPEC, None, {}, 0)
    jobs = {a.job_name: "Running" for a in p1.actions}
    p2 = run({**SPEC, "suspend": True}, p1.status, jobs, 1)
    assert p2.actions == []
    assert p2.status["phase"] == "Suspended"


def test_shard_removed_from_spec_deletes_its_job():
    p1 = run(SPEC, None, {}, 0)
    jobs = {a.job_name: "Running" for a in p1.actions}
    p2 = run({**SPEC, "shards": ["S002", "S003", "S004"]}, p1.status, jobs, 1)
    assert DeleteJob(p1.actions[0].job_name, "shard S001 removed from spec") in p2.actions
    assert "S001" not in p2.status["shards"]


def test_completes_all_shards():
    spec = {**SPEC, "parallelism": 10}
    p = run(spec, None, {}, 0)
    p = run(spec, p.status, {a.job_name: "Succeeded" for a in p.actions}, 1)
    assert p.status["phase"] == "Succeeded"
    assert p.status["progress"] == "4/4"


@pytest.mark.parametrize("bad,msg", [
    ({"shards": 3}, "image"),
    ({"image": "x"}, "shards"),
    ({"image": "x", "shards": 0}, "1..10000"),
    ({"image": "x", "shards": ["a", "a"]}, "duplicates"),
    ({"image": "x", "shards": 2, "parallelism": 0}, "parallelism"),
])
def test_spec_validation(bad, msg):
    with pytest.raises(SpecError, match=msg):
        run(bad, None, {}, 0)


def test_integer_shards():
    assert creates(run({"image": "x", "shards": 3, "parallelism": 5}, None, {}, 0)) == ["0", "1", "2"]


def test_job_names_are_dns_safe_and_bounded():
    assert job_name("store-sync", "S001", 1) == "store-sync-s001-a1"
    assert job_name("Nightly_Sync", "us/east#1", 3) == "nightly-sync-us-east-1-a3"
    long = job_name("x" * 50, "region-" + "y" * 40, 12)
    assert len(long) <= 63 and long.endswith("-a12")
    assert long != job_name("x" * 50, "region-" + "y" * 39 + "z", 12), "hash keeps long names unique"


def test_job_manifest_is_locked_down_and_owned():
    m = job_manifest("store-sync", "batch", "uid-1", SPEC, CreateJob("S002", 2, "store-sync-s002-a2"))
    assert m["spec"]["backoffLimit"] == 0
    assert m["metadata"]["ownerReferences"][0]["uid"] == "uid-1"
    c = m["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in c["env"]}
    assert env["SHARD_ID"] == "S002" and env["SHARD_ATTEMPT"] == "2" and env["SHARD_COUNT"] == "4"
    assert c["securityContext"]["allowPrivilegeEscalation"] is False
    assert m["spec"]["template"]["spec"]["restartPolicy"] == "Never"
