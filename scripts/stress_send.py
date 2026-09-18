#!/usr/bin/env python3
"""
stress_send.py — استرس‌تست سبک و امن برای Arvan SMS API

اجرا از هر جا:
  python3 stress_send.py --base-url http://localhost:8080 --user-id 1 -n 500 -c 40 mix
  BASE_URL=http://localhost:8080 USER_ID=1 N=1000 CONC=50 python3 /path/to/stress_send.py mix

سقف امن: N حداکثر 50_000 مگر با --force
هیچ فایل per-request روی دیسک نوشته نمی‌شود (مگر --save-samples).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional


DEFAULT_MAX_N = 50_000


@dataclass
class Result:
    code: int
    ms: float
    err: str = ""


def http_json(method: str, url: str, body: Optional[dict] = None, timeout: float = 30.0) -> tuple[int, dict | list | str]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            code = resp.getcode()
            try:
                return code, json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                return code, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, raw
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


def delivery_for(mode: str, i: int) -> str:
    if mode == "express":
        return "express"
    if mode == "mix" and i % 3 == 0:
        return "express"
    return "normal"


def build_body(user_id: str, key: str, dtype: str, n_rec: int, i: int) -> dict:
    recs = []
    for r in range(n_rec):
        n = (i * 10 + r) % 100_000_000
        recs.append(f"09{n:08d}")
    return {
        "userId": str(user_id),
        "idempotencyKey": key,
        "type": dtype,
        "text": f"stress-{i}",
        "recipients": recs,
    }


def one_send(base: str, user_id: str, mode: str, i: int, n_rec: int, timeout: float, run_id: str) -> Result:
    if mode == "idempotency":
        key = "stress-idem-shared"
        dtype = "normal"
    else:
        key = f"stress-{mode}-{i}-{run_id}"
        dtype = delivery_for(mode, i)
    body = build_body(user_id, key, dtype, n_rec, i)
    t0 = time.perf_counter()
    code, _ = http_json("POST", f"{base}/messages/send/text", body, timeout=timeout)
    ms = (time.perf_counter() - t0) * 1000
    return Result(code=code, ms=ms)


def pct(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round((p / 100) * (len(sorted_vals) - 1))))
    return sorted_vals[idx]


def main() -> int:
    env_n = os.getenv("N")
    env_c = os.getenv("CONC")
    p = argparse.ArgumentParser(description="Arvan SMS stress test (safe defaults)")
    p.add_argument("mode", nargs="?", default=os.getenv("MODE", "mix"),
                   choices=["mix", "normal", "express", "idempotency"])
    p.add_argument("--base-url", default=os.getenv("BASE_URL", "http://localhost:8080"))
    p.add_argument("--user-id", default=os.getenv("USER_ID", "1"))
    p.add_argument("-n", "--requests", type=int, default=int(env_n) if env_n else 500)
    p.add_argument("-c", "--concurrency", type=int, default=int(env_c) if env_c else 40)
    p.add_argument("--topup", type=int, default=int(os.getenv("TOPUP", "0")))
    p.add_argument("--recipients", type=int, default=int(os.getenv("RECIPIENTS_PER_REQ", "1")))
    p.add_argument("--timeout", type=float, default=float(os.getenv("TIMEOUT_SEC", "30")))
    p.add_argument("--max-n", type=int, default=int(os.getenv("MAX_N", str(DEFAULT_MAX_N))),
                   help=f"سقف امن تعداد درخواست (پیش‌فرض {DEFAULT_MAX_N})")
    p.add_argument("--force", action="store_true",
                   help="اجازه N بزرگ‌تر از --max-n (خطرناک)")
    p.add_argument("--save-samples", type=int, default=0,
                   help="فقط N نمونهٔ اول پاسخ را در stdout چاپ نکن؛ در فایل ذخیره کن")
    args = p.parse_args()

    base = args.base_url.rstrip("/")
    n = args.requests
    conc = max(1, args.concurrency)

    if n <= 0:
        print("N باید > 0 باشد", file=sys.stderr)
        return 2
    if n > args.max_n and not args.force:
        print(
            f"رد شد: N={n} از سقف امن {args.max_n} بزرگ‌تر است.\n"
            f"  مثال معقول: -n 1000 -c 50\n"
            f"  اگر عمداً می‌خواهی: --force  یا  MAX_N=... --force\n"
            f"  (N=500 میلیون ماشین را با fork/دیسک می‌کشد)",
            file=sys.stderr,
        )
        return 2
    if conc > 200 and not args.force:
        print(f"رد شد: concurrency={conc} زیاد است (سقف ۲۰۰ بدون --force)", file=sys.stderr)
        return 2

    print(f"BASE_URL={base} USER_ID={args.user_id} N={n} CONC={conc} MODE={args.mode}")

    code, health = http_json("GET", f"{base}/health", timeout=5)
    if code != 200:
        print(f"API در دسترس نیست: {base}/health → {code} {health}", file=sys.stderr)
        return 1
    print("health ok")

    if args.topup > 0:
        c, _ = http_json("POST", f"{base}/users/{args.user_id}/add-balance", {"amount": args.topup})
        print(f"topup +{args.topup} → http {c}")

    _, user_before = http_json("GET", f"{base}/users/{args.user_id}")
    bal_before = (user_before or {}).get("data", {}).get("balance") if isinstance(user_before, dict) else "?"
    print(f"balance قبل: {bal_before}")

    run_id = str(int(time.time()))
    results: list[Result] = []
    t0 = time.perf_counter()
    print(f"شروع بار…")

    with ThreadPoolExecutor(max_workers=conc) as ex:
        futs = [
            ex.submit(one_send, base, args.user_id, args.mode, i, args.recipients, args.timeout, run_id)
            for i in range(1, n + 1)
        ]
        done = 0
        report_every = max(1, n // 20)
        for fut in as_completed(futs):
            results.append(fut.result())
            done += 1
            if done % report_every == 0 or done == n:
                print(f"  progress {done}/{n}", flush=True)

    elapsed = max(time.perf_counter() - t0, 1e-6)
    codes: dict[int, int] = {}
    lat: list[float] = []
    ok = 0
    for r in results:
        codes[r.code] = codes.get(r.code, 0) + 1
        lat.append(r.ms)
        if 200 <= r.code < 300:
            ok += 1
    lat.sort()

    print("── نتیجه HTTP ──")
    print(f"total={len(results)} ok_2xx={ok} rps={len(results)/elapsed:.1f} wall_sec={elapsed:.2f}")
    print("status_counts:", dict(sorted(codes.items())))
    if lat:
        print(
            f"latency_ms: min={lat[0]:.0f} p50={pct(lat,50):.0f} "
            f"p90={pct(lat,90):.0f} p99={pct(lat,99):.0f} "
            f"max={lat[-1]:.0f} avg={statistics.mean(lat):.1f}"
        )

    print("\n── metrics بعد ──")
    _, metrics = http_json("GET", f"{base}/metrics", timeout=5)
    print(json.dumps(metrics, ensure_ascii=False, indent=2) if isinstance(metrics, (dict, list)) else metrics)

    _, user_after = http_json("GET", f"{base}/users/{args.user_id}")
    bal_after = (user_after or {}).get("data", {}).get("balance") if isinstance(user_after, dict) else "?"
    print(f"\nbalance قبل={bal_before} بعد={bal_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
