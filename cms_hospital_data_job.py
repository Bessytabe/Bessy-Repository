import os
import json
import re
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

# Constants
CMS_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DOWNLOAD_DIR = "hospital_datasets"
METADATA_FILE = "last_run.json"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def to_snake_case(s):
    s = re.sub(r"[’'\"().,-]", "", s)
    s = re.sub(r"[^a-zA-Z0-9\s]", "", s)
    return re.sub(r"\s+", "_", s.strip().lower())

def load_last_run():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)
    return {"last_run": "1970-01-01T00:00:00"}

def save_last_run(timestamp):
    with open(METADATA_FILE, 'w') as f:
        json.dump({"last_run": timestamp}, f)

def download_and_process(dataset):
    title = dataset.get("title", "unknown")
    download_url = dataset.get("distribution", [{}])[0].get("downloadURL")
    modified = dataset.get("modified")

    if not download_url or not modified:
        return f"Skipped: {title} (missing URL or date)"

    filename = os.path.join(DOWNLOAD_DIR, f"{to_snake_case(title)}.csv")
    try:
        df = pd.read_csv(download_url)
        df.columns = [to_snake_case(col) for col in df.columns]
        df.to_csv(filename, index=False)
        return f"Downloaded and processed: {title}"
    except Exception as e:
        return f"Failed: {title} - {str(e)}"

def main():
    last_run_data = load_last_run()
    last_run_time = datetime.fromisoformat(last_run_data["last_run"])

    response = requests.get(CMS_URL)
    response.raise_for_status()
    datasets = response.json().get("dataset", [])

    filtered = [
        ds for ds in datasets
        if "Hospitals" in ds.get("theme", []) and datetime.fromisoformat(ds.get("modified", "1970-01-01T00:00:00")) > last_run_time
    ]

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = executor.map(download_and_process, filtered)

    for res in results:
        print(res)

    save_last_run(datetime.now().isoformat())

if __name__ == "__main__":
    main()
