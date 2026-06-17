import asyncio
import aiohttp
import argparse
import csv
import json
import os
import time
import redis
import statistics
import psutil
from datetime import datetime
from dataclasses import dataclass, asdict, field
from typing import Optional
from pathlib import Path
import logging
import aiofiles

API_BASE = os.getenv("API_BASE", "http://localhost:3000/analysis")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
IMAGES_DIR = Path(os.getenv("IMAGES_DIR", "images"))
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

POLL_INTERVAL = 0.3
POLL_TIMEOUT  = 30.0

# Data model
@dataclass
class JobResult:
    scenario:           str
    image_file:         str
    user_id:            str
    expected_verdict:   str
    actual_verdict:     str
    risk_score:         float
    correct:            bool   
    abuse_correct:      bool 
    upload_index:       int 

    # timing
    upload_latency:     float
    total_latency:      float
    worker_latency:     float
    clip_latency:       float
    redis_latency:      float
    db_latency:         float
    decision_latency:   float

    error:              Optional[str] = None
    iteration:          int = 0
    config:             str = "full"  

# HTTP helpers
async def upload_image(session: aiohttp.ClientSession,
                       image_path: Path,
                       user_id: str) -> tuple[Optional[str], float]:
    t0 = time.perf_counter()
    try:
        async with aiofiles.open(image_path, "rb") as f:    # non-blocking
            file_bytes = await f.read()

        data = aiohttp.FormData()
        data.add_field("image", file_bytes,
                        filename=image_path.name,
                        content_type="image/jpeg")
        data.add_field("userId", user_id)

        async with session.post(API_BASE, data=data) as resp:
            upload_latency = time.perf_counter() - t0
            body = await resp.json()
            print(f"[debug] upload response: {resp.status} {body}")
            return body.get("jobId"), upload_latency

    except Exception as e:
        print(f"[debug] upload exception: {e}")
        return None, time.perf_counter() - t0



logger = logging.getLogger(__name__)

# Exceptions worth retrying vs ones that should abort immediately
_RETRYABLE = (
    aiohttp.ClientConnectionError,
    aiohttp.ServerDisconnectedError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
)

_FATAL = (
    aiohttp.ClientResponseError,   # 4xx — job not found, bad request, etc.
)


async def poll_result(session: aiohttp.ClientSession,
                      job_id: str) -> tuple[Optional[dict], float, Optional[str]]:

    t0 = time.perf_counter()
    deadline = t0 + POLL_TIMEOUT
    last_error: Optional[str] = None
    attempts = 0

    while time.perf_counter() < deadline:
        attempts += 1
        try:
            async with session.get(f"{API_BASE}/{job_id}") as resp:
                resp.raise_for_status()            
                body = await resp.json()
                status = body.get("status")

                if status == "completed":
                    return body, time.perf_counter() - t0, None

                if status == "failed":             
                    reason = body.get("error", "job failed on server")
                    logger.warning("job %s failed server-side: %s", job_id, reason)
                    return None, time.perf_counter() - t0, reason

                if attempts % 10 == 0:
                    logger.debug("job %s still %s after %d polls",
                                 job_id, status, attempts)

        except _FATAL as e:
            reason = f"fatal HTTP error: {e}"
            logger.error("job %s — %s, aborting poll", job_id, reason)
            return None, time.perf_counter() - t0, reason

        except _RETRYABLE as e:
            last_error = f"{type(e).__name__}: {e}"
            logger.debug("job %s — retryable error on attempt %d: %s",
                         job_id, attempts, last_error)

        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            logger.warning("job %s — unexpected error on attempt %d: %s",
                           job_id, attempts, last_error)

        await asyncio.sleep(POLL_INTERVAL)

    reason = f"poll timeout after {attempts} attempts" + (
        f"; last error: {last_error}" if last_error else ""
    )
    logger.error("job %s — %s", job_id, reason)
    return None, time.perf_counter() - t0, reason


async def run_job(session: aiohttp.ClientSession,
                  scenario: str,
                  image_path: Path,
                  user_id: str,
                  expected: str,
                  iteration: int = 0,
                  upload_index: int = 1,
                  config: str = "full") -> JobResult:

    job_id, upload_latency = await upload_image(session, image_path, user_id)

    def _error_result(msg):
        return JobResult(
            scenario=scenario, image_file=image_path.name,
            user_id=user_id, expected_verdict=expected,
            actual_verdict="error", risk_score=0.0,
            correct=False, abuse_correct=False,
            upload_index=upload_index,
            upload_latency=upload_latency, total_latency=0.0,
            worker_latency=0.0, clip_latency=0.0,
            redis_latency=0.0, db_latency=0.0, decision_latency=0.0,
            error=msg, iteration=iteration, config=config
        )

    if job_id is None:
        return _error_result("upload failed")

    result_body, poll_duration, poll_error = await poll_result(session, job_id)
    if result_body is None:
        return _error_result(poll_error or "poll timeout")

    timing = result_body.get("timing", {})
    actual = result_body.get("verdict", "unknown")
    score = float(result_body.get("riskScore", 0.0))

    predicted_abuse = actual in ("flagged", "suspicious")
    expected_abuse = expected in ("flagged", "suspicious")

    return JobResult(
        scenario=scenario,
        image_file=image_path.name,
        user_id=user_id,
        expected_verdict=expected,
        actual_verdict=actual,
        risk_score=score,
        correct=(actual == expected),
        abuse_correct=(predicted_abuse == expected_abuse),
        upload_index=upload_index,
        upload_latency=upload_latency,
        total_latency=upload_latency + poll_duration,
        worker_latency=float(timing.get("worker", 0.0)),
        clip_latency=float(timing.get("clip", 0.0)),
        redis_latency=float(timing.get("redis", 0.0)),
        db_latency=float(timing.get("db", 0.0)),
        decision_latency=float(timing.get("decision", 0.0)),
        iteration=iteration,
        config=config,
    )

# Redis flush
def flush_redis():
    r = redis.from_url(REDIS_URL)
    r.flushdb()
    print("[redis] flushed — residual state cleared for repeatability")

# Scenarios
SCENARIOS = {
    "coordinated": [
        ("attack.jpg", "user_A", "clean"),
        ("attack.jpg", "user_B", "suspicious"),
        ("attack.jpg", "user_C", "flagged"),
    ],
    "burst": [
        ("attack.jpg", "user_burst", "clean"),
        ("attack.jpg", "user_burst", "suspicious"),
        ("attack.jpg", "user_burst", "flagged"),
    ],
    "near_duplicate": [
        ("attack.jpg", "user_X", "clean"),
        ("near_1.jpg", "user_Y", "suspicious"),
        ("attack.jpg", "user_Z", "suspicious"),
    ],
    "clean_baseline": [
        ("cat.jpg", "user_clean_1", "clean"),
        ("mountain.jpg", "user_clean_2", "clean"),
        ("city.jpg", "user_clean_3", "clean"),
    ],
}

FP_ITERATIONS = 20
SCALABILITY_IMAGE = "clean_1.jpg"
SCALABILITY_DEFAULT = [1, 5, 10, 25, 50]

# Signal ablation configurations.
# Each maps to env vars worker reads to disable signals.
# Set these via the API or a control endpoint, or pass as worker env at startup.
ABLATION_CONFIGS = [
    {"name": "harm_only", "ENABLE_PHASH": "false", "ENABLE_BURST": "false", "ENABLE_SEMANTIC": "false"},
    {"name": "harm+phash", "ENABLE_PHASH": "true", "ENABLE_BURST": "false", "ENABLE_SEMANTIC": "false"},
    {"name": "harm+burst", "ENABLE_PHASH": "false", "ENABLE_BURST": "true", "ENABLE_SEMANTIC": "false"},
    {"name": "full", "ENABLE_PHASH": "true", "ENABLE_BURST": "true", "ENABLE_SEMANTIC": "true"},
]

# Helpers
def _percentile(data: list[float], p: int) -> float:
    if not data:
        return 0.0
    
    s = sorted(data)

    k = (len(s) - 1) * p / 100
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _stats(data: list[float]) -> dict:
    if not data:
        return {"mean": 0, "median": 0, "p95": 0, "min": 0, "max": 0, "stdev": 0}
    return {
        "mean": round(statistics.mean(data), 4),
        "median": round(statistics.median(data), 4),
        "p95": round(_percentile(data, 95), 4),
        "min": round(min(data), 4),
        "max": round(max(data), 4),
        "stdev": round(statistics.stdev(data) if len(data) > 1 else 0.0, 4),
    }


def _confusion(results: list[JobResult]) -> dict:
    #Binary confusion matrix: abuse (flagged|suspicious) vs clean.
    tp = fp = fn = tn = 0
    error_count = 0

    for r in results:
        if r.error:
            error_count += 1
            continue
        pa = r.actual_verdict in ("flagged", "suspicious")
        ea = r.expected_verdict in ("flagged", "suspicious")
        if ea and pa:  tp += 1
        if not ea and pa: fp += 1
        if ea  and not pa: fn += 1
        if not ea and not pa: tn += 1

    evaluated = tp + fp + fn + tn
    total = evaluated + error_count

    prec = tp / (tp + fp) if (tp + fp) else 0
    rec = tp / (tp + fn) if (tp + fn) else 0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
    acc  = (tp + tn) / evaluated if evaluated else 0
    effective_acc = (tp + tn) / total if total else 0
    fpr = fp / (fp + tn) if (fp + tn) else 0

    return {
        "TP": tp, "FP": fp, "FN": fn, "TN": tn,
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
        "accuracy": round(acc, 4),
        "effective_acc": round(effective_acc, 4),
        "fpr": round(fpr, 4),
        "evaluated": evaluated,
        "errors": error_count,
        "total": total,
    }

# Experiment 1 - Detection Accuracy
async def run_accuracy_experiment(
    session: aiohttp.ClientSession,
    iterations: int = 5
) -> list[JobResult]:
    print("\n1.Detection Accuracy")
    all_results = []

    # Track per-scenario first-flagged upload index
    detection_triggers: dict[str, list[int]] = {s: [] for s in SCENARIOS}

    for i in range(iterations):
        print(f"Iteration {i+1}/{iterations}")
        flush_redis()

        for scenario_name, jobs in SCENARIOS.items():
            flush_redis()
            flagged_at = None
            for idx, (image_file, user_id, expected) in enumerate(jobs, start=1):
                result = await run_job(
                    session, scenario_name,
                    IMAGES_DIR / image_file, user_id, expected,
                    iteration=i+1, upload_index=idx
                )
                all_results.append(result)

                print(f"{scenario_name}[{idx}] {expected} {result.actual_verdict} {result.risk_score:.3f}")

                if flagged_at is None and result.actual_verdict == "flagged":
                    flagged_at = idx

            if flagged_at:
                detection_triggers[scenario_name].append(flagged_at)

    print("\nDetection trigger position (avg upload # until flagged):")
    for scenario_name, triggers in detection_triggers.items():
        if triggers:
            avg = statistics.mean(triggers)
            print(f"{scenario_name:20s} flagged at upload #{avg:.1f}")

    return all_results

# Experiment 2 — False Positive Rate
async def run_false_positive_experiment(
    session: aiohttp.ClientSession,
    iterations: int = FP_ITERATIONS
) -> list[JobResult]:
    print(f"\n2.False Positive Analysis ({iterations} clean uploads)")
    results = []

    for i in range(iterations):
        flush_redis()  # isolate each upload completely
        result = await run_job(
            session, "fp_clean",
            IMAGES_DIR / "clean_1.jpg",
            f"fp_user_{i}", "clean",
            iteration=i+1, upload_index=1
        )
        results.append(result)
        label = "TN" if result.actual_verdict == "clean" else "FP"
        print(f"[{label}] upload {i+1:2d} | "
                f"verdict={result.actual_verdict} score={result.risk_score:.3f}")

    fp_count = sum(1 for r in results if r.actual_verdict != "clean")
    fpr = fp_count / len(results) * 100
    print(f"\nFalse positives: {fp_count}/{len(results)} FPR: {fpr:.1f}%")
    return results

# Experiment 3 - Risk Score Distribution
def analyze_risk_scores(results: list[JobResult]):
    #Print per-scenario risk score statistics.
    print("\n3. Risk Score Distribution")
    grouped: dict[str, list[float]] = {}
    for r in results:
        if not r.error:
            grouped.setdefault(r.scenario, []).append(r.risk_score)

    print(f"{'Scenario':<22} {'Mean':>6} {'Median':>7} {'StdDev':>7} "
          f"{'Min':>6} {'Max':>6}")
    print(f"{'-'*22} {'-'*6} {'-'*7} {'-'*7} {'-'*6} {'-'*6}")

    for scenario, scores in sorted(grouped.items()):
        s = _stats(scores)
        print(f"{scenario:<22} {s['mean']:>6.3f} {s['median']:>7.3f} "
              f"{s['stdev']:>7.3f} {s['min']:>6.3f} {s['max']:>6.3f}")

# Experiment 4 - Latency Analysis
async def run_latency_experiment(
    session: aiohttp.ClientSession, 
    iterations: int = 10,
    isolated: bool = True,       # True - flush before every job (cold-path timing)
                                 # False - flush between iterations only (stateful timing)
) -> list[JobResult]:
    mode = "isolated (cold-path)" if isolated else "stateful (warm-path)"
    print(f"\n4. Latency Analysis ({iterations} iterations per scenario, {mode})")
    all_results = []

    for scenario_name, jobs in SCENARIOS.items():
        flush_redis()
        for i in range(iterations):
            if not isolated:
                flush_redis()   # stateful mode: reset once per iteration

            for idx, (image_file, user_id, expected) in enumerate(jobs, start=1):
                if isolated:
                    flush_redis()   # isolated mode: reset before every single job

                result = await run_job(
                    session, scenario_name,
                    IMAGES_DIR / image_file, user_id, expected,
                    iteration=i+1, upload_index=idx
                )
                all_results.append(result)

    # Reporting — split by upload position so warm/cold aren't averaged together
    print(f"\n{'Scenario':<22} {'Upload#':>7} {'Mean':>7} {'Median':>7} "
          f"{'P95':>7} {'Min':>7} {'Max':>7} {'N':>4}")
    print(f"{'-'*22} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*7} {'-'*4}")

    for scenario_name, jobs in SCENARIOS.items():
        for idx in range(1, len(jobs) + 1):
            latencies = [
                r.total_latency for r in all_results
                if r.scenario == scenario_name
                and r.upload_index == idx
                and not r.error
            ]
            if latencies:
                s = _stats(latencies)
                print(f"{scenario_name:<22} {idx:>7d} "
                      f"{s['mean']:>7.3f} {s['median']:>7.3f} "
                      f"{s['p95']:>7.3f} {s['min']:>7.3f} "
                      f"{s['max']:>7.3f} {len(latencies):>4d}")

    return all_results

# Experiment 5 — Scalability & Throughput
async def run_scalability_experiment(
    session: aiohttp.ClientSession,
    concurrency_levels: list[int] = SCALABILITY_DEFAULT,
    iterations: int = 5
) -> list[dict]:
    print("\n5.Scalability & Throughput")
    scalability_rows = []
    image_path = IMAGES_DIR / SCALABILITY_IMAGE

    for n in concurrency_levels:
        round_latencies = []
        round_errors = 0
        total_wall = 0.0
        cpu_samples = []
        mem_delta_samples = []

        for i in range(iterations):
            flush_redis()

            # Collect CPU/RAM before the round
            cpu_before = psutil.cpu_percent(interval=None)
            mem_before = psutil.virtual_memory().used / (1024 ** 2)

            t0 = time.perf_counter()
            tasks = [
                run_job(session, "scalability", image_path,
                        f"scale_u{i}_{j}", "clean",
                        iteration=i+1, upload_index=1)
                for j in range(n)
            ]
            results = await asyncio.gather(*tasks)
            wall_time = time.perf_counter() - t0
            total_wall += wall_time

            cpu_after = psutil.cpu_percent(interval=None)
            mem_after = psutil.virtual_memory().used / (1024 ** 2)

            cpu_samples.append((cpu_before + cpu_after) / 2)
            mem_delta_samples.append(mem_after - mem_before)

            for r in results:
                if r.error:
                    round_errors += 1
                else:
                    round_latencies.append(r.total_latency)

            total_jobs = n * iterations
            completed = total_jobs - round_errors
            throughput = completed / total_wall if total_wall > 0 else 0
            success_rate = completed / total_jobs * 100
            lat_stats = _stats(round_latencies)

            avg_cpu = round(statistics.mean(cpu_samples), 1) if cpu_samples else 0.0
            avg_mem_delta = round(statistics.mean(mem_delta_samples), 1) if mem_delta_samples else 0.0

            row = {
                "concurrency": n,
                "avg_latency": lat_stats["mean"],
                "median_latency": lat_stats["median"],
                "p95_latency": lat_stats["p95"],
                "throughput_jobs_sec": round(throughput, 2),
                "success_rate_pct": round(success_rate, 1),
                "total_jobs": total_jobs,
                "errors": round_errors,
                "avg_cpu_pct": avg_cpu,
                "avg_mem_delta_mb": avg_mem_delta,
            }
            scalability_rows.append(row)

            print(f"n={n:3d} | avg={lat_stats['mean']:.3f}s  "
                  f"p95={lat_stats['p95']:.3f}s  "
                  f"throughput={throughput:.2f} jobs/s  "
                  f"success={success_rate:.1f}%  "
                  f"cpu={avg_cpu:.1f}%  mem_delta={avg_mem_delta:+.1f}MB")

    return scalability_rows

# Experiment 6 - Module Overhead Breakdown
async def run_overhead_experiment(
    session: aiohttp.ClientSession,
    iterations: int = 10
) -> list[JobResult]:
    print(f"\n6.Redis/DB Overhead Breakdown ({iterations} iterations)")
    all_results = []

    for i in range(iterations):
        flush_redis()
        for idx, (image_file, user_id, expected) in enumerate(
            SCENARIOS["coordinated"], start=1
        ):
            result = await run_job(
                session, "overhead",
                IMAGES_DIR / image_file, user_id, expected,
                iteration=i+1, upload_index=idx
            )
            all_results.append(result)

    valid = [r for r in all_results if not r.error]
    if valid:
        modules = {
            "CLIP inference": [r.clip_latency for r in valid],
            "Redis lookup": [r.redis_latency for r in valid],
            "DB write": [r.db_latency for r in valid],
            "Decision engine": [r.decision_latency for r in valid],
            "Total worker": [r.worker_latency for r in valid],
        }
        total_mean = _stats(modules["Total worker"])["mean"] or 1

        print(f"\n{'Module':<18} {'Mean':>7} {'P95':>7} {'% of worker':>12}")
        print(f"{'-'*18} {'-'*7} {'-'*7} {'-'*12}")
        for module, vals in modules.items():
            s = _stats(vals)
            pct = s["mean"] / total_mean * 100 if module != "Total worker" else 100
            print(f"{module:<18} {s['mean']:>7.3f} {s['p95']:>7.3f} {pct:>11.1f}%")

    return all_results

# new
async def _verify_worker_config(session: aiohttp.ClientSession,
                                expected: dict) -> tuple[bool, str]:
    """Call worker /config and verify feature flags match expected."""
    config_endpoint = API_BASE.replace("/analysis", "/config")
    try:
        async with session.get(config_endpoint) as resp:
            resp.raise_for_status()
            actual = await resp.json()

        mismatches = []
        for key in ("ENABLE_PHASH", "ENABLE_BURST", "ENABLE_SEMANTIC"):
            exp_val = expected.get(key, "").lower()
            act_val = str(actual.get(key, "")).lower()
            if exp_val != act_val:
                mismatches.append(f"{key}: expected {exp_val}, got {act_val}")

        if mismatches:
            return False, "; ".join(mismatches)
        return True, ""

    except Exception as e:
        return False, f"could not reach /config: {e}"

# Experiment 7 - Signal Contribution (Ablation)
# Requires ENABLE_PHASH / ENABLE_BURST / ENABLE_SEMANTIC env flags in worker.
# You must restart the worker between configs, or expose a /config endpoint.
async def run_ablation_experiment(
    session: aiohttp.ClientSession,
    iterations: int = 5
) -> list[JobResult]:
    print("\n7.Signal Contribution (Ablation Study)")
    all_results = []

    # In practice you'd restart the worker between configs.
    # This loop runs sequentially and prompts you to do so.
    for cfg in ABLATION_CONFIGS:
        config_name = cfg["name"]

        while True:
            input(f"\nSet worker config '{config_name}' {cfg}\n"
                  f"Then press Enter to verify and continue...")

            ok, reason = await _verify_worker_config(session, cfg)
            if ok:
                print(f"Config verified: {config_name}")
                break
            else:
                print(f"Config mismatch — {reason}")
                print("Please restart the worker with the correct flags and try again.")
                # loops back to input()

            for i in range(iterations):
                flush_redis()
                for scenario_name, jobs in SCENARIOS.items():
                    for idx, (image_file, user_id, expected) in enumerate(jobs, start=1):
                        result = await run_job(
                            session, scenario_name,
                            IMAGES_DIR / image_file, user_id, expected,
                            iteration=i+1, upload_index=idx, config=config_name
                        )
                        all_results.append(result)

        # Print metrics for this config
        config_results = [r for r in all_results if r.config == config_name]
        m = _confusion(config_results)
        print(f"\nConfig: {config_name}")
        print(f"Accuracy={m['accuracy']:.3f}  Effective={m['effective_acc']:.3f}  "
              f"Precision={m['precision']:.3f}  Recall={m['recall']:.3f}  "
              f"F1={m['f1']:.3f}  FPR={m['fpr']:.3f}  "
              f"Errors={m['errors']}/{m['total']}")

     # Summary table
    print(f"\n{'Config':<18} {'Acc':>6} {'Eff':>6} {'Prec':>6} "
          f"{'Rec':>6} {'F1':>6} {'FPR':>6} {'Err':>5}")
    print(f"{'-'*18} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*5}")
    for cfg in ABLATION_CONFIGS:
        name = cfg["name"]
        results = [r for r in all_results if r.config == name]
        if results:
            m = _confusion(results)
            print(f"{name:<18} {m['accuracy']:>6.3f} {m['effective_acc']:>6.3f} "
                  f"{m['precision']:>6.3f} {m['recall']:>6.3f} "
                  f"{m['f1']:>6.3f} {m['fpr']:>6.3f} "
                  f"{m['errors']:>5}")

    return all_results

# Severity accuracy (exact match, not just binary)
def analyze_severity_accuracy(results: list[JobResult]):
    print("\nSeverity Classification")

    # Per-scenario exact match rate
    print(f"{'Scenario':<22} {'Exact':>7} {'Binary':>7} {'N':>5}")
    print(f"{'-'*22} {'-'*7} {'-'*7} {'-'*5}")

    for scenario_name in list(SCENARIOS.keys()) + ["fp_clean"]:
        subset = [r for r in results
                  if r.scenario == scenario_name and not r.error]
        if not subset:
            continue
        exact = sum(1 for r in subset if r.correct) / len(subset)
        binary = sum(1 for r in subset if r.abuse_correct) / len(subset)
        print(f"{scenario_name:<22} {exact:>7.3f} {binary:>7.3f} {len(subset):>5}")

    # Overall severity confusion table
    pairs: dict[tuple[str, str], int] = {}
    for r in results:
        if not r.error:
            key = (r.expected_verdict, r.actual_verdict)
            pairs[key] = pairs.get(key, 0) + 1

    print("\nSeverity confusion (expected - actual):")
    for (exp, act), count in sorted(pairs.items()):
        print(f"{exp} -> {act} {count}")


# CSV / JSON output
def save_results(all_job_results: list[JobResult],
                 scalability_rows: list[dict],
                 metrics: dict,
                 tag: str = "phase9"):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = RESULTS_DIR / f"run_{tag}_{ts}"

    if all_job_results:
        jobs_path = f"{prefix}_jobs.csv"
        with open(jobs_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=asdict(all_job_results[0]).keys())
            writer.writeheader()
            for r in all_job_results:
                writer.writerow(asdict(r))
        print(f"\nSaved job results - {jobs_path}")

    if scalability_rows:
        scale_path = f"{prefix}_scalability.csv"
        with open(scale_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=scalability_rows[0].keys())
            writer.writeheader()
            writer.writerows(scalability_rows)
        print(f"Saved scalability - {scale_path}")

    metrics_path = f"{prefix}_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"Saved metrics - {metrics_path}")



# Main
async def main():
    parser = argparse.ArgumentParser(description="Phase 9 evaluation framework")
    parser.add_argument(
        "--scenarios", nargs="+",
        choices=["all", "accuracy", "fp", "latency", "scalability", "overhead", "ablation"],
        default=["all"]
    )
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--fp-iterations", type=int, default=FP_ITERATIONS)
    parser.add_argument("--concurrency", type=int, nargs="+", default=SCALABILITY_DEFAULT)
    parser.add_argument("--latency-isolated", action=argparse.BooleanOptionalAction,
                        default=True,
                        help="Flush Redis before each job (cold-path). "
                             "Use --no-latency-isolated for stateful timing.")
    args = parser.parse_args()

    run_all = "all" in args.scenarios
    all_job_results = []
    scalability_rows = []
    latency_results = []


    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:

        if run_all or "accuracy" in args.scenarios:
            results = await run_accuracy_experiment(session, iterations=args.iterations)
            all_job_results.extend(results)

        if run_all or "fp" in args.scenarios:
            results = await run_false_positive_experiment(
                session,
                iterations=args.iterations)
            all_job_results.extend(results)

        if run_all or "latency" in args.scenarios:
            results = await run_latency_experiment(
                session,
                iterations=args.iterations, 
                isolated=args.latency_isolated)
            # all_job_results.extend(results)
            latency_results = results

        if run_all or "scalability" in args.scenarios:
            scalability_rows = await run_scalability_experiment(
                session,
                concurrency_levels=args.concurrency,
                iterations=args.iterations
            )

        if run_all or "overhead" in args.scenarios:
            results = await run_overhead_experiment(
                session,
                iterations=args.iterations)
            all_job_results.extend(results)

        if "ablation" in args.scenarios:
            results = await run_ablation_experiment(
                session,
                iterations=args.iterations)
            all_job_results.extend(results)

    # Post-processing on all collected results
    if all_job_results:
        analyze_risk_scores(all_job_results)
        analyze_severity_accuracy(all_job_results)

        metrics = _confusion(all_job_results)

        # Per-scenario metrics
        print("\nPer-Scenario Metrics")
        print(f"{'Scenario':<22} {'Acc':>6} {'Prec':>6} {'Rec':>6} "
              f"{'F1':>6} {'FPR':>6} {'N':>5}")
        print(f"{'-'*22} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*5}")
        per_scenario = {}
        for scenario_name in list(SCENARIOS.keys()) + ["fp_clean", "overhead"]:
            subset = [r for r in all_job_results
                      if r.scenario == scenario_name and not r.error]
            if not subset:
                continue
            m = _confusion(subset)
            per_scenario[scenario_name] = m
            print(f"{scenario_name:<22} {m['accuracy']:>6.3f} {m['precision']:>6.3f} "
                  f"{m['recall']:>6.3f} {m['f1']:>6.3f} {m['fpr']:>6.3f} "
                  f"{len(subset):>5}")

        # Error rate summary
        error_count = sum(1 for r in all_job_results if r.error)
        error_rate = error_count / len(all_job_results) * 100
        print(f"\nError rate: {error_count}/{len(all_job_results)} "
              f"({error_rate:.1f}%)")

        print("\nOverall Metrics")
        print(f"Accuracy : {metrics['accuracy']:.4f}")
        print(f"Precision : {metrics['precision']:.4f}")
        print(f"Recall : {metrics['recall']:.4f}")
        print(f"F1 : {metrics['f1']:.4f}")
        print(f"FPR : {metrics['fpr']:.4f}")
        print(f"TP={metrics['TP']} FP={metrics['FP']}  "
              f"FN={metrics['FN']} TN={metrics['TN']}")

        metrics["per_scenario"] = per_scenario
        metrics["error_rate"] = round(error_rate, 2)

        if latency_results:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            latency_path = RESULTS_DIR / f"run_phase9_{ts}_latency.csv"
            with open(latency_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=asdict(latency_results[0]).keys())
                writer.writeheader()
                for r in latency_results:
                    writer.writerow(asdict(r))
            print(f"Saved latency - {latency_path}")

        save_results(all_job_results, scalability_rows, metrics)


if __name__ == "__main__":
    asyncio.run(main())