import aiohttp

NSE_METADATA_URL = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
)

NSE_SYMBOL_DATA_URL = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
)

NSE_HEADERS = {
    "authority": "www.nseindia.com",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
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

NSE_SYMBOL_DATA_HEADERS = {
    "authority": "www.nseindia.com",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
    "referer": "https://www.nseindia.com/get-quotes/equity?symbol=TCS",
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


async def fetch_nse_metadata(symbol: str, c_name: str):
    params = {
        "functionName": "getMetaData",
        "symbol": symbol,
    }

    async with aiohttp.ClientSession(headers=NSE_HEADERS) as session:
        async with session.get(
            NSE_METADATA_URL,
            params=params,
            headers={
                **NSE_HEADERS,
                "path": (
                    "/api/NextApi/apiClient/GetQuoteApi"
                    f"?functionName=getMetaData"
                    f"&symbol={symbol}"
                ),
                "referer": f"https://www.nseindia.com/get-quote/equity/{symbol}/{c_name}"
            },
        ) as response:
            response.raise_for_status()

            data = await response.json()
            return data


async def fetch_nse_symbol_data(
    symbol: str,
    market_type: str = "N",
    series: str = "EQ",
):
    params = {
        "functionName": "getSymbolData",
        "marketType": market_type,
        "series": series,
        "symbol": symbol,
    }

    async with aiohttp.ClientSession(headers=NSE_HEADERS) as session:
        async with session.get(
            NSE_SYMBOL_DATA_URL,
            params=params,
            headers={
                **NSE_SYMBOL_DATA_HEADERS,
                "path": (
                    "/api/NextApi/apiClient/GetQuoteApi"
                    f"?functionName=getSymbolData"
                    f"&marketType={market_type}"
                    f"&series={series}"
                    f"&symbol={symbol}"
                ),
            },
        ) as response:
            print(response.status)
            response.raise_for_status()

            data = await response.json()
            return data