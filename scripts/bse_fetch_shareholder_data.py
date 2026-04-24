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
  "authority": "www.bseindia.com",
  "method": "GET",
  "scheme": "https",
  "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "accept-language": "en-US,en;q=0.9",
  "priority": "u=0, i",
  "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "\"Linux\"",
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "none",
  "sec-fetch-user": "?1",
  "upgrade-insecure-requests": "1",
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
