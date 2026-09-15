import os
from datetime import datetime, date
from pathlib import Path

import aiohttp


NSE_BASE_URL = "https://www.nseindia.com"
NSE_HISTORICAL_URL = (
    "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
)
NSE_DOWNLOAD_URL = (
    "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
)

NSE_HEADERS = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "referer": "https://www.nseindia.com/report-detail/eq_security",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
}

async def fetch_nse_security_wise_historical_data(
    symbol: str,
    from_date: str,
    to_date: str,
    series: str = "EQ",
    data_type: str = "priceVolumeDeliverable",
):
    """
    Args:
        symbol: HDFCBANK
        from_date: dd-mm-yyyy
        to_date: dd-mm-yyyy
        series: EQ
        data_type: priceVolumeDeliverable

    Returns:
        JSON response
    """

    params = {
        "from": from_date,
        "to": to_date,
        "symbol": symbol,
        "type": data_type,
        "series": series,
    }

    async with aiohttp.ClientSession(headers=NSE_HEADERS) as session:

        print("okokok------", type(from_date), type(to_date), series, symbol)
        # Actual API
        async with session.get(
            NSE_HISTORICAL_URL,
            params=params,
            headers={
                **NSE_HEADERS,
                "path": (
                    "/api/historicalOR/generateSecurityWiseHistoricalData"
                    f"?from={from_date}"
                    f"&to={to_date}"
                    f"&symbol={symbol}"
                    f"&type={data_type}"
                    f"&series={series}"
                ),
            },
        ) as response:
            print(response.status)
            response.raise_for_status()
            print(await response.json())
            return await response.json()


async def download_nse_delivery_csv(
    symbol: str,
    from_date: str,
    to_date: str,
    series: str,
    save_path: str,
    data_type: str = "priceVolumeDeliverable",
):

    params = {
        "from": from_date,
        "to": to_date,
        "symbol": symbol,
        "type": data_type,
        "series": series,
        "csv": "true",
    }

    async with aiohttp.ClientSession(headers=NSE_HEADERS) as session:
        async with session.get(
                NSE_HISTORICAL_URL,
                params=params,
                headers={
                    **NSE_HEADERS,
                    "path": (
                            "/api/historicalOR/generateSecurityWiseHistoricalData"
                            f"?from={from_date}"
                            f"&to={to_date}"
                            f"&symbol={symbol}"
                            f"&type={data_type}"
                            f"&series={series}"
                            "csv=true"
                    ),
                },
        ) as response:

            response.raise_for_status()
            # save_path = f"{save_path}/{symbol}.csv"
            download_dir = Path(save_path)
            download_dir.mkdir(parents=True, exist_ok=True)
            file_path = download_dir / f"{symbol}.csv"
            print(file_path)
            with open(file_path, "wb") as f:
                f.write(await response.read())

            return file_path

async def main_nse_fetch_security_wise_historical_data(
    symbol: str,
    from_date: str,
    to_date: str,
    series:str
):
    MEDIA_DIR = os.path.abspath("media")
    os.makedirs(MEDIA_DIR, exist_ok=True)
    print(MEDIA_DIR)
    return await download_nse_delivery_csv(
        symbol=symbol,
        from_date=from_date,
        to_date=to_date, series=series, save_path=f"{MEDIA_DIR}/nse_gross_deliverables"
    )


async def fetch_sec_bhavdata_full_data(
    date_str: str | None = None,
    save_to_disk: bool = True,
) -> dict:
    """
    Fetches sec_bhavdata_full_{DDMMYYYY}.csv from NSE archives for the specified date
    (defaults to current day in Asia/Kolkata timezone).

    URL format:
    https://nsearchives.nseindia.com//products/content/sec_bhavdata_full_{DDMMYYYY}.csv

    Returns:
        dict mapping symbol (str) -> dict of cleaned row data
    """
    import csv
    import io
    from zoneinfo import ZoneInfo
    import requests

    if not date_str:
        now_ist = datetime.now(ZoneInfo("Asia/Kolkata"))
        date_str = now_ist.strftime("%d%m%Y")

    primary_url = f"https://nsearchives.nseindia.com//products/content/sec_bhavdata_full_{date_str}.csv"
    fallback_urls = [
        f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date_str}.csv",
    ]
    urls_to_try = [primary_url] + fallback_urls

    headers = {
        "authority": "nsearchives.nseindia.com",
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "referer": "https://www.nseindia.com/",
        "connection": "keep-alive",
    }

    csv_content = None

    # Try aiohttp first
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            try:
                async with session.get("https://www.nseindia.com", timeout=aiohttp.ClientTimeout(total=5)) as _:
                    pass
            except Exception:
                pass

            for url in urls_to_try:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            csv_content = await response.text(encoding="utf-8-sig", errors="ignore")
                            break
                except Exception as e:
                    print(f"Error fetching {url} with aiohttp: {e}")
                    continue
    except Exception as e:
        print(f"aiohttp session error: {e}")

    # Fallback to requests if aiohttp did not obtain content
    if not csv_content:
        for url in urls_to_try:
            try:
                req_headers = {
                    "User-Agent": headers["user-agent"],
                    "Accept": headers["accept"],
                    "Accept-Language": headers["accept-language"],
                    "Referer": headers["referer"],
                }
                session = requests.Session()
                session.get("https://www.nseindia.com", headers=req_headers, timeout=5)
                resp = session.get(url, headers=req_headers, timeout=30)
                if resp.status_code == 200:
                    resp.encoding = "utf-8-sig"
                    csv_content = resp.text
                    break
            except Exception as req_err:
                print(f"Error fetching {url} with requests: {req_err}")
                continue

    if not csv_content:
        print(f"Could not retrieve sec_bhavdata_full for date {date_str}.")
        return {}

    if save_to_disk:
        try:
            MEDIA_DIR = os.path.abspath("media")
            save_dir = Path(MEDIA_DIR) / "nse_gross_deliverables"
            save_dir.mkdir(parents=True, exist_ok=True)
            file_path = save_dir / f"sec_bhavdata_full_{date_str}.csv"
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(csv_content)
        except Exception as e:
            print(f"Failed to save CSV file to disk: {e}")

    # Parse CSV content into dictionary mapping symbol -> row data
    reader = csv.DictReader(io.StringIO(csv_content))
    bhavdata = {}
    for row in reader:
        cleaned_row = {
            (k.strip() if k else ""): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
            if k is not None
        }
        symbol = cleaned_row.get("SYMBOL")
        series = cleaned_row.get("SERIES")
        if not symbol:
            continue

        # Prioritize EQ series if multiple series exist for the symbol
        if symbol not in bhavdata or series == "EQ":
            bhavdata[symbol] = cleaned_row

    return bhavdata