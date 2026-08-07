import aiohttp

BSE_GROSS_DELIVERY_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/GrossDeliverHistsData_ng/w"
)

BSE_SECURITY_POSITION_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/SecurityPosition/w"
)

BSE_HEADERS = {
    "authority": "api.bseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
    "origin": "https://www.bseindia.com",
    "referer": "https://www.bseindia.com/",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Linux",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}


async def fetch_bse_gross_delivery_history(
    scrip_code: int,
    from_date: str,
    to_date: str,
    strdates: str = "",
):
    """
    Args:
        scrip_code: BSE Scrip Code (e.g. 500875)
        from_date: dd/mm/yyyy (e.g. 01/01/2020)
        to_date: dd/mm/yyyy (e.g. 03/08/2026)
        strdates: Optional (leave empty)

    Returns:
        JSON response from BSE
    """

    params = {
        "ScripCds": scrip_code,
        "dtFromDates": from_date,
        "dtToDates": to_date,
        "strdates": strdates,
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(
            BSE_GROSS_DELIVERY_URL,
            params=params,
            headers={
                **BSE_HEADERS,
                "path": (
                    f"/BseIndiaAPI/api/GrossDeliverHistsData_ng/w"
                    f"?ScripCds={scrip_code}"
                    f"&dtFromDates={from_date}"
                    f"&dtToDates={to_date}"
                    f"&strdates={strdates}"
                ),
            },
        ) as response:
            response.raise_for_status()
            return await response.json()


async def main_bse_fetch_gross_delivery_history(
    scrip_code: int,
    from_date: str,
    to_date: str,
):
    return await fetch_bse_gross_delivery_history(
        scrip_code=scrip_code,
        from_date=from_date,
        to_date=to_date,
    )







BSE_SECURITY_POSITION_HEADERS = {
    "authority": "api.bseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, zstd",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.bseindia.com",
    "referer": "https://www.bseindia.com/",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/140.0.0.0 Safari/537.36"
    ),
}


async def fetch_bse_security_position(scrip_code: str):
    """
    Example:
        await fetch_bse_security_position("532540")

    Response:
    {
        "TradeDate": "05 Aug 2026 |11:18",
        "QtyTraded": "42,638",
        "DeliverableQty": "23,609",
        "PcDQ_TQ": "55.37"
    }
    """
    params = {
        "quotetype": "EQ",
        "scripcode": scrip_code,
    }

    async with aiohttp.ClientSession(headers=BSE_HEADERS) as session:
        async with session.get(
            BSE_SECURITY_POSITION_URL,
            params=params,
            headers={
                **BSE_HEADERS,
                "path": (
                    "/BseIndiaAPI/api/SecurityPosition/w"
                    f"?quotetype=EQ&scripcode={scrip_code}"
                ),
            },
        ) as response:
            response.raise_for_status()

            return await response.json()