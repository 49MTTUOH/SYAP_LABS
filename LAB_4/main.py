import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import sys
from fake_useragent import UserAgent

sys.stdout.reconfigure(line_buffering=True)

collected_data = {
    "Name": [],
    "IMO": [],
    "MMSI": [],
    "Type": []
}
error_log = []
processed_ok = 0

ua = UserAgent()

client = requests.Session()
client.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept-Language': 'en-US,ru;q=0.9,en-US;q=0.8',
    'Accept': 'text/html,application/xhtml+xml;q=0.9,*/*;q=0.8',
    'Connection': 'keep-alive',
})

try:
    client.get("https://www.vesselfinder.com/", timeout=10)
    time.sleep(1)
except:
    pass

def extract_mmsi_from_script(html_soup):
    for tag in html_soup.find_all("script"):
        if not tag.string:
            continue
        found = re.search(r"var\s+MMSI\s*=\s*(\d+)", tag.string)
        if found:
            collected_data["MMSI"].append(found.group(1))
            return True
    collected_data["MMSI"].append("")
    return False

def handle_single_result(page_soup, target_url):
    vessel_name_elem = page_soup.find("div", class_="slna")
    vessel_type_elem = page_soup.find("div", class_="slty")
    vessel_link_elem = page_soup.find("a", class_="ship-link")

    if not all([vessel_name_elem, vessel_type_elem, vessel_link_elem]):
        return False

    ship_name = vessel_name_elem.get_text(strip=True)
    ship_class = vessel_type_elem.get_text(strip=True)
    link_href = vessel_link_elem.get("href", "")

    imo_match = re.search(r"/details/(\d+)", link_href)
    if not imo_match:
        return False

    imo_id = imo_match.group(1)
    detail_page_url = f"https://www.vesselfinder.com/ru/vessels/details/{imo_id}"

    try:
        detail_resp = client.get(detail_page_url, timeout=30)
        if detail_resp.status_code != 200:
            print(f"Details not available: {detail_resp.status_code}", flush=True)
            collected_data["MMSI"].append("")
            return False
    except Exception as ex:
        print(f"Error loading parts: {ex}", flush=True)
        collected_data["MMSI"].append("")
        return False

    extract_mmsi_from_script(BeautifulSoup(detail_resp.text, "html.parser"))

    collected_data["Name"].append(ship_name)
    collected_data["IMO"].append(imo_id)
    collected_data["Type"].append(ship_class)
    return True

def analyze_search_page(content, original_url):
    if content.find("div", class_="no-result-row"):
        return True

    total_info = content.find("div", class_="pagination-totals")
    if not total_info:
        return True

    count_match = re.search(r"\d+", total_info.get_text())
    if not count_match:
        return True

    total_vessels = int(count_match.group())
    if total_vessels != 1:
        return True

    return handle_single_result(content, original_url)

def fetch_and_parse(url):
    global processed_ok
    try:
        resp = client.get(url.strip(), timeout=45)
        if resp.status_code != 200:
            error_log.append((url, resp.status_code))
            print(f"Page cant be download: {resp.status_code}", flush=True)
            return False

        parser = BeautifulSoup(resp.text, "html.parser")
        if analyze_search_page(parser, url):
            processed_ok += 1
            return True
        return False

    except Exception as err:
        error_log.append((url, str(err)))
        print(f"Fail: {type(err).__name__} — {err}", flush=True)
        return False

input_sheet = pd.read_excel("Links.xlsx")
counter = 1

for raw_url in input_sheet["Ссылка"]:
    clean_url = str(raw_url).strip()
    if not clean_url or clean_url.lower() == "nan":
        continue
    status = "Success" if fetch_and_parse(clean_url) else "Fail"
    print(f"{counter}: {status}", flush=True)
    counter += 1
    if counter == 500:
        output_frame = pd.DataFrame(collected_data, columns=["Name", "IMO", "MMSI", "Type"])
        output_frame.to_excel("result.xlsx", index=False)

output_frame = pd.DataFrame(collected_data, columns=["Name", "IMO", "MMSI", "Type"])
output_frame.to_excel("result.xlsx", index=False)

print("\nProcessed successfully:", processed_ok, flush=True)
print("Errors:", len(error_log), flush=True)
if error_log:
    for entry in error_log:
        print(" -", entry[0], "|", entry[1])