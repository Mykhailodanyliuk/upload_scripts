import asyncio
import time

import aiohttp
import requests
import json
from fake_headers import Headers
from bs4 import BeautifulSoup
import lxml

headers = {
    "cookie": "ak_bmsc=54E57F757C5F64B5710C2B1976588E8C~000000000000000000000000000000~YAAQjTxRaO6YCUqDAQAAYcP0XhHsilMrKKXmBWab%2BiyLQC5qw6RT9t8IXLMFX9RTFEWXACJ2pakIPXC5XQ3hH%2FFSUVYmBANnsbZOZfrNGmmpJbE5BMoln33FwaIskpkQIZxTqlqSRZVNn9ckJ5UpiC7DyUGKNQzP0gBjc%2FPRVVA08D7LkTjABInr8FYlKDOkWkRo5CA27ycQBWzAgIKGTkbJ7br%2FGPi67YS6kw43rPai5y%2FN7zE5fEvrcXUDj8GDg6HFN%2FQwiTSXhVeN8MJzXTsabt4By7fPS1%2BJl8CqF1BsjImcpxCZcK%2BTvcjGCJzCQ0rTaWZM0gmLSkZHvo6mjzlrViGeuRwn0Xat8W1gdk%2FeZxPqsSJm%2BZzFIYfuMsOSrJr%2BVgTN8M8Kb%2FW612v4Cuwb9SU%2FO%2BpmohwVGml9101PHA%3D%3D",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "uk-UA,uk;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cookie": "_ga=GA1.2.484631131.1655475251; ak_bmsc=35D0CFF2E96CE830ED9F1982C5D70802~000000000000000000000000000000~YAAQTuXKF/ADzFSDAQAA1030XhGvc6ZNSV/LXFU60CzbaFHkvy2LbOEfI+xd/MbSqPXiqYq6IFXGDwDO/0rQFHY2W5Z1z5S2elxFy6Bts2rtdJBxHVTnJ8I3XibPqD6TkHJt1OxCslcu7XoQalRfueFF69qVLr1GhlU0dx8HKTeuoxvQ6it4A4AypgrFFrDX62cY+I1YgNC7HwXefqZmdBn6X2HQ2z6Op6C11nwQiXh2RifJ3XFzB3yTkeZ68ZzZNcKbm4//d9mEj/Wx1mqwNkv7GQ88eI5D3vnMOddCJx7fZcXJNRvNtGMEL7MOChIjPL9DF559yf2HIeeFThaO9HCVoS73PhE8qGxMZ+3XHOV3Y5II13hsrKAFgUTp+TPZF60ALGWYNA==; bm_sv=A67F71F4AB25B92BB6F9136D077EBD99~YAAQTuXKF4AFzFSDAQAALHL0XhHcmFsZ9EwwtSf4C5hbGz0l3zKxZiByILoBNBMV8/1trzZmm24k2lPiQurKpXsYbMQRL+olwoduFQCwqtZLxZ3uutj2Q/VaG2f0p/fB+vxsDkJgbi3V6bO2PYoy2syR60LN7v7bbrDkVazLTnb0Sk2BnCtVwuu/VGpXvPPMmbHERwkgg/KLxaW6egRbUbIZsVFINseX+iwUlTpRknua1SVNRWYaC7ry8pqI~1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "TE": "trailers"
}

header = Headers(
        browser="chrome",  # Generate only Chrome UA
        os="win",  # Generate ony Windows platform
        headers=True  # generate misc headers
    )

async def get_page(session, url):
    while True:
        try:
            async with session.get(url, headers=headers) as r:
                print(r.status)
                # print(r.text())
                if r.status == 200:
                    return await r.text()
        except asyncio.exceptions.TimeoutError:
            print('asyncio.exceptions.TimeoutError')


async def get_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(get_page(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results


async def get_all_data_urls(urls, limit=2):
    timeout = aiohttp.ClientTimeout(total=600)
    connector = aiohttp.TCPConnector(limit=limit)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        data = await get_all(session, urls)
        return data


def get_json_from_request(url):
    return json.loads((requests.get(url, headers=headers)).text)


