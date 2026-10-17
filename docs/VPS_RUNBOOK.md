# VPS Runbook — Phase 1 demo

Step-by-step for running `make demo-phase1` on the Hostinger KVM 2 instance and capturing the output needed to close the Phase 1 MEASURED entry in [`BENCHMARKS.md`](BENCHMARKS.md).

Target host: **srv1809153.hstgr.cloud (200.97.161.186)**. 2 vCPU AMD EPYC 9354P, 7.8 GiB RAM, no swap, Ubuntu 24.04.

## 0. SSH in

```bash
ssh root@200.97.161.186
```

## 1. One-time prerequisites (skip on later runs)

### 1a. Install Docker + Compose + Make + Git

```bash
apt-get update
apt-get install -y ca-certificates curl gnupg make git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
docker --version
docker compose version
```

### 1b. Record host info (for BENCHMARKS.md header)

```bash
mkdir -p ~/fkp-benchmarks/raw
{
  echo "=== uname ==="
  uname -a
  echo
  echo "=== CPU ==="
  lscpu | grep -E "Model name|CPU\(s\)|Thread|Socket|MHz"
  echo
  echo "=== Memory ==="
  free -h
  echo
  echo "=== Disk ==="
  df -h /
  echo
  echo "=== Docker ==="
  docker --version
  docker compose version
} | tee ~/fkp-benchmarks/raw/host-info.txt
```

## 2. Clone the repo

```bash
cd ~
git clone <your-repo-url> festival-kafka-poc    # or scp from local
cd festival-kafka-poc
```

If `scp` from your Mac is easier:

```bash
# on your Mac
rsync -avz --exclude 'target' --exclude 'node_modules' --exclude '.git' \
    /Users/Krishna.Vulchi/Documents/GitHub/festival-kafka-poc/ \
    root@200.97.161.186:/root/festival-kafka-poc/
```

## 3. First build (slow — ~15 min cold)

```bash
cd ~/festival-kafka-poc
make bootstrap          # confirms docker + compose present
make build              # builds 7 service images; cache primed afterwards
```

Subsequent `make up` reuses layer cache and is fast.

## 4. Phase 1 demo — run 1 (sanity run @ 100 rps, 60 s)

Confirms the saga completes end-to-end before cranking the load.

```bash
export RUN_LABEL="run1-100rps-60s-$(date +%Y%m%d-%H%M%S)"
mkdir -p ~/fkp-benchmarks/raw/$RUN_LABEL
make demo-phase1 LOAD_RPS=100 LOAD_DURATION_SEC=60 2>&1 \
    | tee ~/fkp-benchmarks/raw/$RUN_LABEL/transcript.txt
```

The transcript file is the single source of truth for this run. It contains:

- Load generator Summary (achieved RPS, p50/p95/p99, error rate)
- `saga_counts` snapshot
- Orders status distribution
- Payments and shipments distributions
- Consumer group lag summary

Also capture memory + CPU during the run (optional but very useful):

```bash
# in a second ssh session, during the load window:
docker stats --no-stream \
    | tee ~/fkp-benchmarks/raw/$RUN_LABEL/docker-stats.txt
```

### Expected saga-completion arithmetic

Given `payment.gateway.failure-rate = 0.01` (default):

- `shipment.created ≈ 0.99 × order.created` (payment success path)
- `order.cancelled ≈ 0.01 × order.created` (payment failure path)
- `inventory.failed ≈ 0` (demo SKUs have 100k stock each; load can't exhaust)
- `orders` table should sum to approximately: `COMPLETED` (~99%) + `CANCELLED` (~1%) + a tiny `CREATED` tail if load ended before all sagas drained.

If the ratios are wildly off, something is wrong — do NOT crank the RPS before investigating. Common causes:

- Consumer group lag non-zero after `drain` → increase `LAG_DRAIN_SECS` and re-check `make saga-stats`.
- One service crashed → `make ps` to spot an `Exited` container; `make logs SERVICE=<name>`.

## 5. Phase 1 demo — run 2 and 3 (crank)

Only run these if run 1 looks sane.

```bash
make down                        # clean state so each run starts fresh

export RUN_LABEL="run2-500rps-60s-$(date +%Y%m%d-%H%M%S)"
mkdir -p ~/fkp-benchmarks/raw/$RUN_LABEL
make demo-phase1 LOAD_RPS=500 LOAD_DURATION_SEC=60 2>&1 \
    | tee ~/fkp-benchmarks/raw/$RUN_LABEL/transcript.txt
```

Then:

```bash
make down
export RUN_LABEL="run3-1000rps-60s-$(date +%Y%m%d-%H%M%S)"
mkdir -p ~/fkp-benchmarks/raw/$RUN_LABEL
make demo-phase1 LOAD_RPS=1000 LOAD_DURATION_SEC=60 2>&1 \
    | tee ~/fkp-benchmarks/raw/$RUN_LABEL/transcript.txt
```

If run 2 (500 rps) already saturates (saga completion drops below 90%, or lag grows during the window), **do not run at 1000 rps** — record the saturation point and stop. Phase 1's purpose is a baseline; we scale in later phases.

## 6. Capture back to your Mac

```bash
# on your Mac:
rsync -avz root@200.97.161.186:/root/fkp-benchmarks/raw/ \
    /Users/Krishna.Vulchi/Documents/GitHub/festival-kafka-poc/docs/benchmarks/raw/
```

Then paste the content of each `transcript.txt` back into the chat and I will fill in `docs/BENCHMARKS.md` with proper MEASURED entries and close the Phase 1 DECISIONS wrap-up.

## 7. Teardown

```bash
make down               # removes containers + volumes
docker system prune -f  # optional: reclaim disk from built images
```

## Checklist before pasting back

- [ ] `host-info.txt` captured once (step 1b)
- [ ] Run 1 (100 rps) transcript captured
- [ ] `docker stats` snapshot during the load window
- [ ] Run 2 (500 rps) transcript (if run 1 passed)
- [ ] Run 3 (1000 rps) transcript (only if run 2 passed)
- [ ] Any noteworthy anomalies / crashes / OOM-kills noted

## Common issues

**Build is extremely slow / seems stuck.** The first `make build` pulls Maven dependencies (~500 MB) and 6+ Docker base images (~1.5 GB). Expect 10–20 min on a 2 vCPU machine. Watch `docker events` in another session if unsure.

**`make wait-healthy` times out.** Some service didn't come up. Run `make ps` to spot the laggard, then `make logs SERVICE=<name>` to see why. Common cause: Postgres took longer than expected to accept connections — healthchecks have `start_period: 30s`; if the VPS is under IO pressure, bump `HEALTH_WAIT_SECS` and retry.

**OOM-kill during run.** Expected if you push past the memory budget. Look at `dmesg | grep -i oom` and `docker events`. Reduce `LOAD_RPS` and record the ceiling honestly.

**Consumer group lag doesn't drain.** `make saga-stats` keeps showing non-zero lag. Either a consumer crashed (check `make ps`), or `LAG_DRAIN_SECS` is too short. Increase it to 60 and re-run `make saga-stats` manually; don't shut down the stack mid-drain.
