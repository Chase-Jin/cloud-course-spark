#!/usr/bin/env python3
import argparse
import concurrent.futures
import time
import urllib.request


def fetch(url: str, timeout: float) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 500
    except Exception:
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Simple HTTP load generator for HPA testing.")
    parser.add_argument("--url", required=True)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    start = time.perf_counter()
    ok = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [pool.submit(fetch, args.url, args.timeout) for _ in range(args.requests)]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            ok += int(fut.result())
            if i % 500 == 0:
                print(f"completed={i}, ok={ok}")
    elapsed = time.perf_counter() - start
    print(f"requests={args.requests}, ok={ok}, elapsed={elapsed:.2f}s, qps={args.requests/elapsed:.2f}")


if __name__ == "__main__":
    main()
