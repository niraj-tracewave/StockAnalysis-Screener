import os
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