#!/usr/bin/python3
import json
import time
import urllib.request

import bs4


GOV_URL = "https://www.gov.uk/guidance/access-fuel-price-data"


class DownloadError(Exception):
    pass


def download(url):
    # The Tesco server just hangs unless we give it the right headers.
    # Tut, tut.
    url = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.5",
        "Accept-Encoding": "identity",
    })
    delay = 1
    for _ in range(4):
        try:
            with urllib.request.urlopen(url) as response:
                return response.read().decode("utf-8")
        except (urllib.error.HTTPError, urllib.error.URLError):
            pass
        print("               ... retrying in {delay} s")
        time.sleep(delay)
        delay *= 2
    raise DownloadError()


def download_all():
    retailers = []
    with urllib.request.urlopen(GOV_URL) as response:
        soup = bs4.BeautifulSoup(response, "html.parser")
        table, = soup.body.select("#participating-retailers + table")
        for row in table.tbody.find_all("tr"):
            name, url = [td.text for td in row.find_all("td")]
            print(f"Downloading: {name}")
            try:
                raw = download(url)
            except DownloadError:
                print("               ... skipped.")
            else:
                data = json.loads(raw)
                retailers.append({"name": name, "data": data})
    print("Writing")
    with open("all.json", "w") as f:
        json.dump({"retailers": retailers}, f, indent=2)


if __name__ == "__main__":
    download_all()
