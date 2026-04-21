# import asyncio
# from urllib.parse import urlparse
#
# import aiohttp
# from collections import defaultdict
#
# BASE_URL = "https://www.nseindia.com/api"
#
# HEADERS = {
#     "authority": "www.nseindia.com",
#     "accept": "*/*",
#     "method": "GET",
#     "scheme": "https",
#     "accept-language": "en-US,en;q=0.9",
#     "priority": "u=1, i",
#     "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
#     "sec-ch-ua-mobile": "?0",
#     "sec-ch-ua-platform": "\"Linux\"",
#     "sec-fetch-dest": "empty",
#     "sec-fetch-mode": "cors",
#     "sec-fetch-site": "same-origin",
#     "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
# }
#
#
# # ---------------------------
# # FETCH FUNCTION
# # ---------------------------
# async def fetch(session, url):
#     parsed = urlparse(url)
#
#     path = parsed.path.lstrip("/") + "?" + parsed.query
#
#     print(path, parsed.query)
#     async with session.get(url, headers={**HEADERS, "path":path, "referer": f"https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern?{parsed.query}"}) as response:
#         return await response.json()
#
#
# # ---------------------------
# # NSE API CALLS
# # ---------------------------
# async def fetch_all(ndsId):
#     async with aiohttp.ClientSession() as session:
#
#         # Required APIs (from your screenshots)
#         urls = {
#             # "summary": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&section=summary",
#             "promoter": f"{BASE_URL}/corporate-share-holdings-equities?ndsId={ndsId}&index=promoter",
#             # "public": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&index=public",
#             # "non_public": f"{BASE_URL}/corporate-share-holdings-equities?symbol={symbol}&index=non-public"
#         }
#
#         tasks = {k: fetch(session, v) for k, v in urls.items()}
#         results = await asyncio.gather(*tasks.values())
#
#         return dict(zip(tasks.keys(), results))
#
#
# async def main_nse_fetch_shareholding_data_using_api(ndsId):
#     data = await fetch_all(ndsId)
#     return data


from datetime import datetime
import urllib.parse
import requests

async def main_nse_fetch_shareholding_data_using_api(id, symbol, name, rec_id, row, inner_active_tab="equities"):

    # Parse date
    as_on_date = datetime.strptime(row.get("date"), "%d-%b-%Y")

    # Define ranges
    start_2011 = datetime.strptime("01-JAN-2011", "%d-%b-%Y")
    end_2015 = datetime.strptime("31-DEC-2015", "%d-%b-%Y")

    start_2009 = datetime.strptime("01-JAN-2009", "%d-%b-%Y")
    end_2010 = datetime.strptime("31-DEC-2010", "%d-%b-%Y")

    start_2006 = datetime.strptime("01-JAN-2006", "%d-%b-%Y")
    end_2008 = datetime.strptime("31-DEC-2008", "%d-%b-%Y")

    encoded_symbol = urllib.parse.quote(symbol)

    # 🔹 Date range conditions
    if start_2011 <= as_on_date <= end_2015:
        archive_url = f"https://nsearchives.nseindia.com/corporates/ShareholdingInformation.html?ndsid={rec_id}&symbol={encoded_symbol}"
        return {"type": "redirect", "url": archive_url}

    elif start_2009 <= as_on_date <= end_2010:
        archive_url = f"https://nsearchives.nseindia.com/corporates/ShareholdingInformation2009_10.html?ndsid={rec_id}&symbol={encoded_symbol}"
        return {"type": "redirect", "url": archive_url}

    elif start_2006 <= as_on_date <= end_2008:
        archive_url = f"https://nsearchives.nseindia.com/corporates/ShareholdingInformation2006_08.html?ndsid={rec_id}&symbol={encoded_symbol}"
        return {"type": "redirect", "url": archive_url}

    else:

        result = {
            "company_name": name,
            "symbol": symbol,
            "date": row.get("date"),
            "remarks": row.get("remarksWeb") if row.get("remarksWeb") and row.get("remarksWeb").lower() != "n" else "No Remarks.",
            "revision_remark": row.get("revisionRemark") or "No Remarks.",
            "revision_date": row.get("revisionDate") or "",
            "data": {}
        }
        HEADERS = {
            "authority": "www.nseindia.com",
            "accept": "*/*",
            "method": "GET",
            "scheme": "https",
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

        base_url = f"https://www.nseindia.com/api/corporate-share-holdings-{inner_active_tab}"

        def fetch_api(params):
            try:
                base1_url = f"{base_url}?ndsId={params.get("ndsId")}&index={params.get('index')}"
                response = requests.get(base1_url, headers=HEADERS, timeout=10)
                if response.status_code == 200:
                    return response.json()
            except Exception as e:
                print("API Error:", e)
            return []

        # # Declaration
        # result["data"]["declaration"] = fetch_api({
        #     "ndsId": rec_id,
        #     "index": "declaration"
        # })
        #
        # # Summary
        # result["data"]["summary"] = fetch_api({
        #     "ndsId": rec_id,
        #     "index": "summary"
        # })

        # Promoter
        result["data"]["promoter"] = fetch_api({
            "ndsId": rec_id,
            "index": "promoter"
        })

        # Public Shareholder
        result["data"]["public_shareholder"] = fetch_api({
            "ndsId": rec_id,
            "index": "public-shareholder"
        })
        #
        # # Non-public shareholder
        # result["data"]["non_public_shareholder"] = fetch_api({
        #     "ndsId": rec_id,
        #     "index": "non-public-shareholder"
        # })
        #
        # # Beneficial owners
        # result["data"]["beneficial_owners"] = fetch_api({
        #     "ndsId": rec_id,
        #     "index": "beneficial-owners"
        # })
        #
        # # Foreign ownership limits
        # fol_data = fetch_api({
        #     "ndsId": rec_id,
        #     "index": "foreign-ownership-limits"
        # })

        # formatted_fol = []
        # if isinstance(fol_data, list):
        #     for d in fol_data:
        #         formatted_fol.append({
        #             "category": d.get("shp_category"),
        #             "board": d.get("shp_board"),
        #             "limit": d.get("shp_limit")
        #         })

        # result["data"]["foreign_ownership_limits"] = formatted_fol

        return {"type": "data", "result": result}

import aiohttp

integrated_filing_url = "https://www.nseindia.com/api/corporate-share-holdings-master"

shareholder_list_headers = {
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "*/*",
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
async def fetch_data(symbol, index):
    original_url = f"{integrated_filing_url}?index={index}&symbol={symbol}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**shareholder_list_headers, "path": f"/api/corporate-share-holdings-master?index={index}&symbol={symbol}",
                                                      "referer": f"https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern?symbol={symbol}&tabIndex={index}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_nse_fetch_shareholding_list(symbol, index):
    data = await fetch_data(symbol, index)
    return data
