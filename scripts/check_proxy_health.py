"""Validate configured provider proxies without exposing credentials.

This is an operational preflight tool. It does not discover public proxies and
does not modify the proxy configuration.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.proxy_pool import redact_proxy_url


load_dotenv()


HEALTH_TARGETS = {
    "yahoo": "https://query1.finance.yahoo.com/v8/finance/chart/RELIANCE.NS?range=1d&interval=1d",
    "nse": "https://www.nseindia.com/",
    "bse": "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w?scripcode=500325",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json,text/html,*/*",
}


@dataclass(slots=True)
class ProxyHealthResult:
    provider: str
    endpoint: str
    healthy: bool
    status_code: int | None
    latency_ms: int
    error: str | None


def check_endpoint(provider: str, entry: dict, timeout: int) -> ProxyHealthResult:
    proxy_url = str(entry.get("url") or "")
    redacted = redact_proxy_url(proxy_url)
    started = time.monotonic()
    if not proxy_url.startswith(("http://", "https://")):
        return ProxyHealthResult(
            provider=provider,
            endpoint=redacted,
            healthy=False,
            status_code=None,
            latency_ms=0,
            error="Only HTTP/HTTPS upstream proxies are enabled in the active transport",
        )

    try:
        response = requests.get(
            HEALTH_TARGETS[provider],
            headers=HEADERS,
            proxies={"http": proxy_url, "https": proxy_url},
            timeout=(min(timeout, 5), timeout),
        )
        latency_ms = round((time.monotonic() - started) * 1_000)
        healthy = response.status_code < 400
        return ProxyHealthResult(
            provider=provider,
            endpoint=redacted,
            healthy=healthy,
            status_code=response.status_code,
            latency_ms=latency_ms,
            error=None if healthy else f"HTTP {response.status_code}",
        )
    except requests.RequestException as exc:
        return ProxyHealthResult(
            provider=provider,
            endpoint=redacted,
            healthy=False,
            status_code=None,
            latency_ms=round((time.monotonic() - started) * 1_000),
            error=type(exc).__name__,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config/proxies.json"))
    parser.add_argument("--timeout", type=int, default=10)
    args = parser.parse_args()

    payload = json.loads(args.config.read_text(encoding="utf-8"))
    jobs = []
    for provider in HEALTH_TARGETS:
        provider_config = payload.get(provider, [])
        entries = (
            provider_config.get("endpoints", [])
            if isinstance(provider_config, dict)
            else provider_config
        )
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict) or entry.get("enabled", True) is False:
                continue
            url_env = str(entry.get("url_env") or "")
            proxy_url = entry.get("url") or (os.environ.get(url_env) if url_env else None)
            if proxy_url:
                jobs.append((provider, {**entry, "url": proxy_url}))
    if not jobs:
        print(json.dumps({"configured": 0, "healthy": 0, "results": []}, indent=2))
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(jobs), 20)) as executor:
        futures = [
            executor.submit(check_endpoint, provider, entry, args.timeout)
            for provider, entry in jobs
        ]
        results = [future.result() for future in futures]

    healthy = sum(result.healthy for result in results)
    print(
        json.dumps(
            {
                "configured": len(results),
                "healthy": healthy,
                "results": [asdict(result) for result in results],
            },
            indent=2,
        )
    )
    raise SystemExit(0 if healthy == len(results) else 1)


if __name__ == "__main__":
    main()
