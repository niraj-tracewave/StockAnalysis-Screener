# url = 'https://charting.nseindia.com/v1/charts/symbolHistoricalData?token=11536&fromDate=0&toDate=1771321964&symbol=TCS-EQ&symbolType=Equity&chartType=D&timeInterval=1'

import aiohttp

url = "https://charting.nseindia.com/v1/charts/symbolHistoricalData"
headers = {
    "authority": "charting.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
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


async def fetch_data(token, symbol, symbol_type):
    original_url = f"{url}?token={token}&fromDate=0&toDate=1771321964&symbol={symbol}&symbolType={symbol_type}&chartType=D&timeInterval=1"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**headers, "path": f"/v1/charts/symbolHistoricalData?token={token}&fromDate=0&toDate=1771321964&symbol={symbol}&symbolType={symbol_type}&chartType=D&timeInterval=1",
                                                      "referer": f"https://charting.nseindia.com/?symbol={symbol}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data

async def main_fetch_volume_from_nse(token, symbol, symbol_type):
    data = await fetch_data(token, symbol, symbol_type)
    return data