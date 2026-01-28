"""
Batch ingestion script for Wikimedia hourly pageview data.

This script:
- Downloads one hour of pageview data
- Parses the compressed dump
- Stores structured data locally for downstream analysis
"""

import argparse 
import gzip 
import os 
from datetime import datetime 

import pandas as pd 
import requests 

def parse_args():
    parser = argparse.ArgumentParser(description="Donwload and store Wikiemdia hourly pageview data")
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date in YYYY-MM-DD format (UTC)"
    )
    parser.add_argument(
        "--hour",
        type=int,
        required=True,
        choices=range(0,24),
        help="Hour in UTC (0-23)"
    )
    return parser.parse_args()

def build_pageviews_url(date: str, hour: int) -> str:
    """
    Construct the Wikimedia pageviews dump URL for a given date and hour.
    """
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    year_month = dt.strftime("%Y-%m")
    day = dt.strftime("%Y%m%d")
    hour_str = f"{hour:02d}0000"

    filename = f"pageviews-{day}-{hour_str}.gz"
    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year_month}/{filename}"
    )
    return url


def download_file(url: str, output_path:str):
    """
    Download a file from the given URL to the specified path.
    """
    response = requests.get(url , stream=True)
    response.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            

def parse_pageviews_gz(file_path: str, date: str ,  hour: int) -> pd.DataFrame:
    """
    Parse a Wikimedia pageviews.gz file into a data frame. 
    """

    records = []
    with gzip.open(file_path , "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.strip().split(" ")
            if len(parts) < 4:
                continue

            project, page, views , _ = parts[0], parts[1] , parts[2] , parts[3]

            try:
                views = int(views)
            except ValueError:
                continue

            records.append({
                "project": project, 
                "page": page,
                "views": views,
                "date": date,
                "hour": hour
            })

    df = pd.DataFrame.from_records(records)
    return df 


def save_parquet(df: pd.DataFrame, date: str,hour: int):
    dt = datetime.strptime(date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    output_dir = f"data/raw/pageviews/{year}/{month}/{day}"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f"{hour:02d}.parquet")
    df.to_parquet(output_path, index=False)

def main():
    args = parse_args()

    url = build_pageviews_url(args.date, args.hour)
    tmp_path = "data/raw/temp_pageviews.gz"

    print(f"Downlaoding data from {url}")
    download_file(url, tmp_path)

    print("Parsing data....")
    df = parse_pageviews_gz(tmp_path, args.date , args.hour)

    print(f"Parsed {len(df)} rows")

    print("Saving to parquet....")
    save_parquet(df, args.date , args.hour)

    os.remove(tmp_path)
    print("Done.")

if __name__ == "__main__":
    main()