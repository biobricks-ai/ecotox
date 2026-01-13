#!/usr/bin/env python3
"""
Process EPA ECOTOX database into parquet format.
"""

from pathlib import Path
import pandas as pd
import requests
import time
import json

def cas_to_smiles_batch(cas_numbers: list, cache_file: Path, batch_size: int = 100) -> dict:
    """Convert CAS numbers to SMILES via PubChem API with caching."""
    if cache_file.exists():
        with open(cache_file) as f:
            cas_to_smiles = json.load(f)
        print(f"  Loaded {len(cas_to_smiles)} cached SMILES")
    else:
        cas_to_smiles = {}

    # Filter to only uncached CAS numbers
    to_lookup = [c for c in cas_numbers if c not in cas_to_smiles]
    if not to_lookup:
        return cas_to_smiles

    print(f"  Looking up {len(to_lookup)} new CAS numbers...")
    for i in range(0, len(to_lookup), batch_size):
        batch = to_lookup[i:i+batch_size]
        if i % 500 == 0:
            print(f"    Batch {i//batch_size + 1}/{(len(to_lookup)-1)//batch_size + 1}...")

        for cas in batch:
            try:
                url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{cas}/property/CanonicalSMILES/JSON"
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    props = data.get('PropertyTable', {}).get('Properties', [])
                    if props:
                        smiles = props[0].get('CanonicalSMILES') or props[0].get('ConnectivitySMILES', '')
                        if smiles:
                            cas_to_smiles[cas] = smiles
                time.sleep(0.15)  # Rate limit
            except:
                pass

        # Save cache periodically
        if i % 1000 == 0:
            with open(cache_file, 'w') as f:
                json.dump(cas_to_smiles, f)

    # Final save
    with open(cache_file, 'w') as f:
        json.dump(cas_to_smiles, f)

    return cas_to_smiles

def main():
    import sys

    base = Path("download/extracted/ecotox_ascii_12_11_2025")
    brick_path = Path("brick")
    brick_path.mkdir(exist_ok=True)

    # Load chemicals (skip SMILES lookup for now - too slow for 18K chemicals)
    print("Loading chemicals...", flush=True)
    chem = pd.read_csv(base / "validation/chemicals.txt", sep='|', low_memory=False)
    print(f"  {len(chem)} chemicals", flush=True)

    # Note: SMILES could be added via DTXSID->CompTox or CAS->PubChem in future
    # For now, we have DTXSID which can be used for lookups
    chem['smiles'] = None  # Placeholder

    # Load species
    print("Loading species...", flush=True)
    species = pd.read_csv(base / "validation/species.txt", sep='|', low_memory=False)
    print(f"  {len(species)} species", flush=True)

    # Load endpoints
    print("Loading endpoints...", flush=True)
    endpoints = pd.read_csv(base / "validation/endpoint_codes.txt", sep='|', low_memory=False)
    print(f"  {len(endpoints)} endpoint types", flush=True)

    # Load tests (core table with chemical links)
    print("Loading tests...", flush=True)
    tests = pd.read_csv(base / "tests.txt", sep='|', low_memory=False,
                        usecols=['test_id', 'test_cas', 'species_number', 'exposure_type',
                                'test_location', 'test_type', 'media_type', 'organism_habitat'])
    print(f"  {len(tests)} tests", flush=True)

    # Load results (endpoints, concentrations)
    print("Loading results...", flush=True)
    results = pd.read_csv(base / "results.txt", sep='|', low_memory=False,
                          usecols=['result_id', 'test_id', 'endpoint', 'endpoint_assigned',
                                  'conc1_mean', 'conc1_unit', 'effect', 'obs_duration_mean',
                                  'obs_duration_unit'])
    print(f"  {len(results)} results", flush=True)

    # Save tables as parquet
    print("Saving chemicals...")
    for col in chem.select_dtypes(include=['object']).columns:
        chem[col] = chem[col].astype(str)
    chem.to_parquet(brick_path / "chemicals.parquet", index=False)

    print("Saving species...")
    for col in species.select_dtypes(include=['object']).columns:
        species[col] = species[col].astype(str)
    species.to_parquet(brick_path / "species.parquet", index=False)

    print("Saving endpoints...")
    for col in endpoints.select_dtypes(include=['object']).columns:
        endpoints[col] = endpoints[col].astype(str)
    endpoints.to_parquet(brick_path / "endpoints.parquet", index=False)

    print("Saving tests...")
    for col in tests.select_dtypes(include=['object']).columns:
        tests[col] = tests[col].astype(str)
    tests.to_parquet(brick_path / "tests.parquet", index=False)

    print("Saving results...")
    for col in results.select_dtypes(include=['object']).columns:
        results[col] = results[col].astype(str)
    results.to_parquet(brick_path / "results.parquet", index=False)

    # Summary
    print(f"\nSummary:", flush=True)
    print(f"  Chemicals: {len(chem)} (DTXSID available for lookups)", flush=True)
    print(f"  Species: {len(species)}", flush=True)
    print(f"  Tests: {len(tests)}", flush=True)
    print(f"  Results: {len(results)}", flush=True)

if __name__ == "__main__":
    main()
