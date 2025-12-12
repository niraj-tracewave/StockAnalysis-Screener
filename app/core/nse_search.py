import requests

url = "https://www.nseindia.com/api/search/autocomplete"
headers ={
    "authority": "www.nseindia.com",
    "method": "GET",
    "scheme": "https",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
    "cookie": "_ga=GA1.1.161678497.1761738475; AKA_A2=A; ak_bmsc=FE2EA5BBC7D659E4325CE516F97D2FD7~000000000000000000000000000000~YAAQjc8uF5z7Y8WaAQAAvHYrEh7M/47NwS25wcjQ9/2sTi2tePK67ZM4thHy76Xu/F6RRKD+yb+akusRGPbRxPR2sFYkJdjCnfhKb28ONJEnggyCBAnTYyR5zfH8CAYaIAh0ZIjWD2fBJ1zeNuqWnmyoIglc/x7T0zKuIv8tZJv4f/bh2TpDxUOMdZppsSezW9Ez6plMtDAgdxv+iwY7p+dmgbUXWTCl4mo3u/Fe1O/OuCU88S5lR6Ud0HUf5dHKgzN53SCkd9V/gqJLfZXmG6gzxfMhgxCJAUySn2Ssx9EYmJ0Pt6Jg1DtEptoEaSRzVgfmHkRDaSVwwxSjwZ4bd1MR8V6k08pzNTtksH9T91Htn9YhBYNP5/y7yNDmHS3BsERabxdrKmH720sLgMwKFr2aju2A6Vnu4XMXuPgeBPgFotHf9Q98r3XBb07MDDUVc4b9uqWoskWQTp2LAmgcRQ==; bm_sz=940B12365942AE6CC01876135A0EF37D~YAAQjc8uF5RjZMWaAQAAh4YtEh7AtVksCeFjHvIMgv/NZ6VJJcA+nFj/O9EPF2FsP8FbggJvgSqltpu3VttP0tzmr4WaTYsnFN/k1qBOINRaS1nUuiPuQkgAUliJ2UEiG+qL4dIYvb6CeOtMJX5YT4DmRcOpLgrmDsR1wiFXzJmuBwrE+x1OWMGdMCRIZLsCtxZeWz6WLNiNQrvBiraEdJ2aKWo4mgLjBOj/xnMVYLoDoldUllIO/c8Uqd1v7f42csl3FbHjxohrQf4dwsmH+dV2dC/wABMD+Q89QPzC86lAzoippa6frojCuwib9Rugn2fYPUt2Sa81RFMlWBPes6NwuaWBWwY67uTHW5iAyUIQdS3HpzY/Ewbjvnae8t33s6Ee0hZ+GEdFn1LTP/O+TPP9hVVdNxI=~3162416~3158072; _ga_87M7PJ3R97=GS2.1.s1765536396$o34$g1$t1765536533$j59$l0$h0; RT=\"z=1&dm=nseindia.com&si=2f290a67-b3e6-47cf-83ff-d40fdc6065f2&ss=mj2qs8hj&sl=0&se=8c&tt=0&bcn=%2F%2F684d0d48.akstat.io%2F\"; bm_sv=CC0237008CF4937E76A1D59E691192E0~YAAQjc8uF4NlZMWaAQAAUZAtEh5e/IWZUYwoNXmMjz0tzriCHpUJenFgpY80BGtviRAtN16/VvmzTIl9ojPHlPg5nwR/85lWYzFrxCMgW10aAkOBSme3MK5W1YVLa+h6PQubkkTfs9tLTnEId5l2Kr83wrnEzFm40iDobV4XxfLNO8MV57jIqlZ1Ylu+rkKMqbyR71KDoqZEVskpkpXYajI+MkiKjP+Sz29dO/Ym3LZ25m5CNTylTNZiWeKy3PN9bQM1~1",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Linux\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
  }


async def fetch_nse_data(search):
    response = requests.get(f"{url}?q={search}", headers={**headers, "path": f"/api/search/autocomplete?q={search}"})
    company_list = []
    if response.status_code == 200:
        symbols_data = response.json().get("symbols")
        for symbol_data in symbols_data:
            company_list.append({"symbols": symbol_data.get("symbol"), "company_name": symbol_data.get("symbol_info"), "url": symbol_data.get("url")})
        return company_list
    return company_list
