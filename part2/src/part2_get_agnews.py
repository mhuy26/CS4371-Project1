"""Download the full Hugging Face AG News train split as Parquet (no hard-coded machine paths)."""
import argparse
import json
import urllib.request
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument('--output', required=True, help='Directory for AG News Parquet shards')
a = p.parse_args()
out = Path(a.output)
out.mkdir(parents=True, exist_ok=True)
api = 'https://datasets-server.huggingface.co/parquet?dataset=fancyzhx/ag_news'
with urllib.request.urlopen(api, timeout=60) as response:
    payload = json.load(response)
shards = [x for x in payload['parquet_files'] if x['split'] == 'train']
if not shards:
    raise SystemExit('No train parquet shards in Hugging Face API response')
for i, x in enumerate(shards):
    path = out / f'train_{i:03d}.parquet'
    print('Downloading', path, 'from', x['url'])
    if path.exists() and path.stat().st_size == x['size']:
        print('Already present, skipping')
        continue
    urllib.request.urlretrieve(x['url'], path)
    if path.stat().st_size != x['size']:
        raise SystemExit(f'Size mismatch for {path}: {path.stat().st_size} vs {x["size"]}')
print('Downloaded all training shards:', len(shards))
