"""Shared extraction utilities for downloading NYC taxi trip data."""

import gzip
import os
import shutil
import urllib.request


def download_and_decompress(taxi: str, year: str, month: str) -> str:
    """Download and decompress a taxi trip CSV from the DataTalksClub releases.

    Returns the path to the decompressed CSV file.
    """
    filename = f"{taxi}_tripdata_{year}-{month}.csv"
    url = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
        f"/{taxi}/{filename}.gz"
    )
    gz_out_path = f"/tmp/{filename}.gz"
    out_path = f"/tmp/{filename}"

    print(f"Downloading {url} → {gz_out_path}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req) as response, open(gz_out_path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)

        print(f"Decompressing {gz_out_path} → {out_path}")
        with gzip.open(gz_out_path, "rb") as f_in:
            with open(out_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
    finally:
        if os.path.exists(gz_out_path):
            os.remove(gz_out_path)

    size = os.path.getsize(out_path)
    print(f"Downloaded {size:,} bytes uncompressed")
    return out_path
