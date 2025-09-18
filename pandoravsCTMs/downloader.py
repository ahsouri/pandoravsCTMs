import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://data.hetzner.pandonia-global-network.org/"
LOCAL_DIR = "PGN_rnvs3_L2_files"  # all files will go here
SLEEP_BETWEEN_REQUESTS = 1  # polite delay

os.makedirs(LOCAL_DIR, exist_ok=True)

def sanitize(s: str) -> str:
    return s.replace('/', '_').replace('\\', '_').replace(' ', '_').replace('.', '_')

def get_links(url):
    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    hrefs = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and not href.startswith('?') and not href.startswith('/'):
            hrefs.append(href)
    return hrefs

stations = [h for h in get_links(BASE_URL) if h.endswith('/')]

for station in stations:
    station_url = urljoin(BASE_URL, station)
    instruments = [h for h in get_links(station_url) if h.endswith('/')]

    for instrument in instruments:
        instrument_url = urljoin(station_url, instrument)
        l2_url = urljoin(instrument_url, "L2/")
        try:
            files = get_links(l2_url)
        except Exception as e:
            print("Skipping", l2_url, "because", e)
            continue

        for f in files:
            if "rnvs3" in f and f.endswith(".txt"):
                file_url = urljoin(l2_url, f)
                safe_name = f"{sanitize(station)}_{sanitize(instrument)}_{sanitize(f)}"
                local_file = os.path.join(LOCAL_DIR, safe_name)
                if not os.path.exists(local_file):
                    print("Downloading", file_url)
                    resp = requests.get(file_url)
                    resp.raise_for_status()
                    with open(local_file, "wb") as out:
                        out.write(resp.content)
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
