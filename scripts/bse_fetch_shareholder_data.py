import aiohttp
from bs4 import BeautifulSoup


BSE_SHP_URL = "https://api.bseindia.com/BseIndiaAPI/api/SHPQNewFormat/w"

BSE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36",
    "origin": "https://www.bseindia.com",
    "referer": "https://www.bseindia.com/",
    "sec-fetch-site": "same-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}
async def fetch_bse_shareholding_list(scripcode):
    url = f"{BSE_SHP_URL}?scripcode={scripcode}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={**BSE_HEADERS, "path": f"/BseIndiaAPI/api/SHPQNewFormat/w?scripcode={scripcode}"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_bse_fetch_shareholding_list(scripcode):
    data = await fetch_bse_shareholding_list(scripcode)
    return data


HEADERS = {
  "authority": "api.bseindia.com",
  "method": "GET",
  "scheme": "https",
  "accept": "application/json, text/plain, */*",
  "accept-language": "en-US,en;q=0.9",
  "priority": "u=1, i",
  "Origin": "https://www.bseindia.com",
    "referer": "https://www.bseindia.com/",
  "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Linux\"",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
  "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

async def parse_bse_promoter_table(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.bseindia.com/{url}", headers={**HEADERS, "path": url}) as resp:
            resp.raise_for_status()
            html = await resp.text()

    soup = BeautifulSoup(html, "html.parser")
    div = soup.find("div", id="tdData")
    if not div:
        return {"promoter": [], "summary": {}}

    table = div.find("table")
    if not table:
        return {"promoter": [], "summary": {}}

    results = []
    summary = {}

    for row in table.find_all("tr"):
        cols = row.find_all("td")

        if not cols:
            continue

        texts = [c.get_text(strip=True) for c in cols]

        if len(texts) < 6:
            continue

        name = texts[0]

        entity_type = texts[1]


        if "A=A1+A2" in name:

            if entity_type:
                summary = {
                    "label": name,
                    "shareholding_percent": texts[6]
                }
            elif cols[0].find(["b", "strong"]):
                summary = {
                    "label": name,
                    "shareholding_percent": texts[6]
                }
            continue

        if any(x in name for x in ["Sub Total", "A1", "A2"]):
            continue


        if entity_type:
            if "Promoter" not in entity_type:
                continue
        else:
            is_bold = cols[0].find(["b", "strong"])
            if is_bold:
                continue

        if entity_type:
            shareholding_percent = texts[6]
        else:
            shareholding_percent = texts[6]

        results.append({
            "name": name,
            "shareholding_percent": shareholding_percent
        })

    return {
        "promoter": results,
        "summary": summary
    }

async def new_parse_bse_promoter_table(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.bseindia.com/BseIndiaAPI/api/{url}", headers={**HEADERS, "path": f"/BseIndiaAPI/api/{url}"}) as resp:
            resp.raise_for_status()
            data = await resp.json()

    if not data:
        return {"promoter": [], "summary": {}}

    table = data.get("Table1")
    if not table:
        return {"promoter": [], "summary": {}}

    results = []
    summary = {}
    for row in table:
        flag = row.get("Flag")

        if not flag:
            continue

        name = row.get("Fld_ShareHolderName")
        if not name:
            name = row.get("Fld_SubCategory")
        if not name:
            name = row.get("Fld_Level")
        entity_type = row.get("FLd_ShareholderType")

        if name and "A=A1+A2" in name:
            summary = {
                "label": name,
                "shareholding_percent": row.get("Fld_TotalPercentageOf_A_B_C2")
            }
            continue

        if any(x in name for x in ["Sub Total", "A1", "A2"]):
            continue

        if entity_type:
            if "Promoter" not in entity_type:
                continue
        else:
            continue

        if entity_type:
            shareholding_percent = row.get("Fld_TotalPercentageOf_A_B_C2")
        else:
            shareholding_percent = row.get("Fld_TotalPercentageOf_A_B_C2")

        results.append({
            "name": name,
            "shareholding_percent": shareholding_percent
        })
    return {
        "promoter": results,
        "summary": summary
    }

async def parse_bse_public_shareholder_table(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://www.bseindia.com/{url}", headers={**HEADERS, "path": url}) as resp:
            resp.raise_for_status()
            html = await resp.text()

    soup = BeautifulSoup(html, "html.parser")
    div = soup.find("div", id="tdData")
    if not div:
        return {"public": []}

    outer_table = div.find("table")
    if not outer_table:
        return {"promoter": []}

    trs = outer_table.find_all("tr", recursive=False)

    if len(trs) < 3:
        return {"promoter": []}

    third_tr = trs[2]

    table = third_tr.find("table")

    if not table:
        return {"promoter": []}

    results = []

    skips_list = {"B=B1+B2+B3+B4", "Institutions"}
    for row in table.find_all("tr")[5:]:
        cols = row.find_all("td")

        if not cols:
            continue

        texts = [c.get_text(strip=True) for c in cols]
        if len(texts) < 6:
            continue

        name = texts[1]

        if name in skips_list:
            continue

        is_bold = bool(cols[1].find(["b", "strong"]))

        shareholding_percent = texts[7]

        results.append({
            "name": name,
            "shareholding_percent": shareholding_percent,
            "is_bold": is_bold
        })

    return {
        "public": results,
    }

async def new_parse_bse_public_shareholder_table(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.bseindia.com/BseIndiaAPI/api/{url}", headers={**HEADERS, "path": f"/BseIndiaAPI/api/{url}"}) as resp:
            resp.raise_for_status()
            data = await resp.json()
    # print(data)
    if not data:
        return {"public": []}

    table = data.get("Table1")
    if not table:
        return {"public": []}

    if not table:
        return {"public": []}

    results = []

    skips_list = {"B=B1+B2+B3+B4", "Institutions"}
    # print(table)
    for row in table:
        flag = row.get("Flag")

        if not flag:
            continue

        name = row.get("Fld_ShareHolderName")
        if not name:
            name = row.get("Fld_Level")
        if not name:
            name = row.get("Fld_SubCategory")


        if name in skips_list:
            continue

        is_bold = True if flag == 1 else False

        shareholding_percent = row.get("Fld_TotalPercentageOf_A_B_C2")

        results.append({
            "name": name,
            "shareholding_percent": shareholding_percent,
            "is_bold": is_bold
        })

    return {
        "public": results,
    }


import aiohttp

integrated_filing_url = "https://api.bseindia.com/BseIndiaAPI/api/CorporatesSHPSecuritybeta/w"

shareholder_list_headers = {
    "authority": "api.bseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "origin": "https://www.bseindia.com",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=1, i",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}
async def fetch_data(qtrid, scripcode):
    original_url = f"{integrated_filing_url}?scripcode={scripcode}&qtrid={qtrid}"
    async with aiohttp.ClientSession() as session:
        async with session.get(original_url, headers={**shareholder_list_headers, "path": f"/BseIndiaAPI/api/CorporatesSHPSecuritybeta/w?scripcode={scripcode}&qtrid={qtrid}",
                                                      "referer": f"https://www.bseindia.com/"}) as response:
            response.raise_for_status()
            data = await response.json()
            return data


async def main_bse_cshp_fetch_shareholding_list(qtrid, scripcode):
    data = await fetch_data(qtrid, scripcode)
    return data