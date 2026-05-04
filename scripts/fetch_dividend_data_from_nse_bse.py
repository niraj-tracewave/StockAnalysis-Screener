import aiohttp


NSE_CORPORATE_ACTION_URL = "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
headers = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

async def fetch_nse_corporate_actions(symbol, c_name):
    url = f"{NSE_CORPORATE_ACTION_URL}?functionName=getCorpAction&type=W&marketApiType=equities&symbol={symbol}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={**headers,
                                             "path": f"/api/NextApi/apiClient/GetQuoteApi?functionName=getCorpAction&symbol={symbol}&type=W&marketApiType=equities",
                                             "referer": f"https://www.nseindia.com/get-quote/equity/{symbol}/{c_name}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_nse_corporate_action_call(symbol, c_name):
    data = await fetch_nse_corporate_actions(symbol, c_name)
    return data

BSE_CORPORATE_ACTION_URL = "https://api.bseindia.com/BseIndiaAPI/api/DefaultData/w"

BSE_HEADERS = {
    "authority": "api.bseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "origin": "https://www.bseindia.com",
    "referer": "https://www.bseindia.com/",
}

async def fetch_bse_corporate_actions(
    scripcode: str,
    purpose_code: str = "P9",   # P9 = Dividend
    from_date: str = "",
    to_date: str = "",
    segment: str = "0",
) -> dict:
    params = {
        "scripcode": scripcode,
        "Fdate": from_date,
        "Purposecode": purpose_code,
        "TDate": to_date,
        "ddlcategorys": "E",
        "ddlindustrys": "",
        "segment": segment,
        "strSearch": "S",
    }
    path = (
        f"/BseIndiaAPI/api/DefaultData/w"
        f"?scripcode={scripcode}&Fdate={from_date}&Purposecode={purpose_code}"
        f"&TDate={to_date}&ddlcategorys=E&ddlindustrys=&segment={segment}&strSearch=S"
    )
    async with aiohttp.ClientSession() as session:
        async with session.get(
            BSE_CORPORATE_ACTION_URL,
            params=params,
            headers={**BSE_HEADERS, "path": path},
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_bse_corporate_action_call(
    scripcode: str,
    purpose_code: str = "P9",
    from_date: str = "",
    to_date: str = "",
) -> dict:
    data = await fetch_bse_corporate_actions(
        scripcode=scripcode,
        purpose_code=purpose_code,
        from_date=from_date,
        to_date=to_date,
    )
    return data