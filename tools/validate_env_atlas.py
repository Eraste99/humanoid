"""Validator ensuring .env.atlas matches BotConfig.from_env extractions."""
import json
from pathlib import Path

def load_atlas_keys(path: Path):
    keys=[]
    for line in path.read_text().splitlines():
        line=line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            keys.append(line.split('=',1)[0])
    return keys

def main():
    atlas_path=Path('.env.atlas')
    extraction_path=Path('.codex_env_extraction.json')
    atlas_keys=set(load_atlas_keys(atlas_path))
    extracted=set(json.loads(extraction_path.read_text())['keys'])
    missing=extracted-atlas_keys
    extra=atlas_keys-extracted
    if missing or extra:
        raise SystemExit(
            f"Validation failed: missing={sorted(missing)} extra={sorted(extra)}"
        )
    print(f"Validation succeeded: {len(atlas_keys)} keys, bijection confirmed.")

if __name__ == '__main__':
    main()
