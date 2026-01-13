#!/usr/bin/env python3
"""
Download EPA ECOTOX database from EPA FTP.
Source: https://cfpub.epa.gov/ecotox/
"""

from pathlib import Path
import urllib.request
import zipfile
import os

def main():
    download_path = Path("download")
    download_path.mkdir(exist_ok=True)

    # EPA ECOTOX database - December 2025 version
    url = "https://gaftp.epa.gov/ecotox/ecotox_ascii_12_11_2025.zip"
    zip_file = download_path / "ecotox_ascii.zip"
    extract_dir = download_path / "extracted"

    if extract_dir.exists() and len(list(extract_dir.glob("*.txt"))) > 10:
        print("Already extracted")
        return

    # Download
    if not zip_file.exists():
        print(f"Downloading ECOTOX database...")
        print(f"  URL: {url}")
        urllib.request.urlretrieve(url, zip_file)
        print(f"  Downloaded: {zip_file.stat().st_size / 1024 / 1024:.1f} MB")
    else:
        print(f"Already downloaded: {zip_file}")

    # Extract
    print("Extracting...")
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_file, 'r') as z:
        z.extractall(extract_dir)

    # List extracted files
    files = list(extract_dir.rglob("*.txt"))
    print(f"Extracted {len(files)} files")
    for f in sorted(files)[:10]:
        print(f"  {f.name}")
    if len(files) > 10:
        print(f"  ... and {len(files)-10} more")

if __name__ == "__main__":
    main()
