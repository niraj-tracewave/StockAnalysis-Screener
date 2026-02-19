import aiohttp
from lxml import etree

async def fetch_shareholding_statements(url: str):
    main_url = f"https://www.bseindia.com/{url}"
    headers = {
            "authority": "www.bseindia.com",
            "method": "GET",
            "path": f"/{url}",
            "scheme": "https",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Linux\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(main_url, timeout=15) as resp:
            html = await resp.text()


    root = etree.HTML(html)

    header_tds = root.xpath(
        "//div[@id='tdData']//td[@class='innertable_header1']"
    )
    print(header_tds)

    share_pct_col_index = None

    for idx, td in enumerate(header_tds, start=1):
        header_text = " ".join(td.xpath(".//text()")).strip()
        print(header_text)
        if "Shareholding as a % of total no. of shares (calculated as per SCRR, 1957)As a % of (A+B+C2)" in header_text:
            share_pct_col_index = idx
            break

    print("Detected Share % column index:", share_pct_col_index)


    rows = root.xpath("//div[@id='tdData']//tr[count(td[@class='TTRow_left'])=2]")
    data = []

    for row in rows:
        name = row.xpath("td[@class='TTRow_left'][1]/text()")
        name_val = name[0].strip() if name and name[0].strip() else None

        if not name_val:
            continue

        right_col_index = share_pct_col_index - 2

        # share_pct = row.xpath("td[@class='TTRow_right'][4]/text()")
        share_pct = row.xpath(
            f"td[@class='TTRow_right'][{right_col_index}]/text()"
        )

        share_pct_val = float(share_pct[0].strip()) if share_pct and share_pct[0].strip() else None

        data.append({
            "name": name_val,
            "shareholding_percent": share_pct_val
        })

    return data


# ---------------- RUNNER ----------------
async def main_fetch_stock_share_holder_pattern_urls(url):
    result = await fetch_shareholding_statements(url)
    return result