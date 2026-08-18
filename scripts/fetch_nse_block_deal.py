import csv
import io
import aiohttp

nse_block_deals_url = (
    "https://www.nseindia.com/api/historicalOR/"
    "bulk-block-short-deals"
)
nse_home_url = "https://www.nseindia.com/"

async def fetch_data(block_deal_payload):
    headers = {
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,image/avif,image/webp,"
            "image/apng,*/*;q=0.8,"
            "application/signed-exchange;v=b3;q=0.7"
        ),
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Sec-Ch-Ua": (
            '"Chromium";v="140", '
            '"Not=A?Brand";v="24", '
            '"Google Chrome";v="140"'
        ),
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Linux"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
    }

    async with aiohttp.ClientSession(
        headers=headers
    ) as session:

        async with session.get(
            nse_block_deals_url,
            params=block_deal_payload
        ) as response:

            response.raise_for_status()

            csv_bytes = await response.read()
            csv_text = csv_bytes.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(csv_text))
            data = list(reader)
            return data

async def main_block_deals(block_deal_payload):
    data = await fetch_data(block_deal_payload)
    return data