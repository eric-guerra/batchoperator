# BatchOperator

Portfolio reference implementation of a 2021 project, built to show a Kubernetes operator that fans batch work out over shards. A typical batch is a nightly price sync for 2,000 stores, or a reprocess job over 90 date partitions. Each shard runs as its own Job, with bounded parallelism and per-shard retries.

**Stack:** Python 3.10, kopf, the official Kubernetes client, Helm 3, and a CRD (`ShardedBatch`, group `batch.ericguerra.dev/v1`).

## Usage
```yaml
apiVersion: batch.ericguerra.dev/v1
kind: ShardedBatch
metadata: { name: nightly-price-sync, namespace: batch }
spec:
  image: registry.example.com/price-sync:2.3.1
  shards: [S001, S002, S003, S004]   # or an integer: shards: 90
  parallelism: 3
  maxRetries: 2
  backoffSeconds: 60                 # doubles per attempt
  failurePolicy: Continue            # or FailFast
```
```
$ kubectl get sb -n batch
NAME                 PHASE     PROGRESS   FAILED   AGE
nightly-price-sync   Running   5/8        0        4m
```
Each worker container receives `SHARD_ID`, `SHARD_ATTEMPT` and `SHARD_COUNT`.

## Design
- **Pure planner** (`batchoperator/planner.py`): `reconcile(spec, status, observed_jobs, now)` returns a plan of actions, a new status and events. It does no I/O, so all the behaviour below is unit-tested without a cluster:
  - Parallelism, filled in spec order.
  - Backoff and retry with new deterministic Job names (`<batch>-<shard>-a<attempt>`, DNS-safe and hashed when longer than 63 characters).
  - A Job that vanishes counts as a failed attempt.
  - `FailFast` cancels the running Jobs and the pending shards.
  - `suspend` stops new work without killing running Jobs.
  - Removing a shard from the spec deletes its Job.
  - The terminal event fires exactly once.
- **Safe, repeatable reconciles:**
  - Job names are deterministic and a 409 on create is treated as success, so a crash midway through a reconcile never creates the same Job twice.
  - Status lives on the CR, so the operator is stateless.
- **Retries are owned by the operator:** Jobs get `backoffLimit: 0` and the operator applies its own retry policy and accounting, which built-in Job retries can't do per shard.
- **kopf wiring** (`handlers.py`):
  - `create`, `update` and `resume` handlers.
  - Job events "poke" the owning CR for fast reaction.
  - A 60-second timer acts as a safety net for missed watch events.
  - `TemporaryError(delay=...)` handles backoff wake-ups.
  - Peering gives leader election across 2 replicas.
- **Locked down:**
  - Worker Jobs run non-root with a read-only root filesystem, all capabilities dropped and the RuntimeDefault seccomp profile.
  - The operator's RBAC is limited to its CRD, Jobs, events and kopf peering. It has no Secrets or `pods/exec` access.

## Install
```bash
kubectl apply -f https://github.com/nolar/kopf/raw/main/peering.yaml   # kopf peering CRDs
helm install batchoperator chart/batchoperator -n batch-system --create-namespace
kubectl apply -f examples/price-sync.yaml
```

## Test
```bash
pip install pytest && pytest -q   # 18 planner tests
```
