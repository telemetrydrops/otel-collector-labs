"""Build deterministic learner ZIPs from the explicit public pilot file list."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
FILES = {
    "prepare-lab-environment": ["README.md", "check.py", "collector.yaml"],
    "first-otlp-pipeline": ["README.md", "collector.yaml", "input.data", "run.py"],
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {}
    for lesson, names in FILES.items():
        filename = f"{lesson}-lab.zip"
        target = args.output / filename
        with zipfile.ZipFile(target, "w") as archive:
            for name in sorted(names):
                relative = Path("lessons") / lesson / "labs" / name
                source = ROOT / relative
                if source.resolve() != source or not source.is_file():
                    raise ValueError(f"Expected a regular source file: {relative}")
                info = zipfile.ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, source.read_bytes())
        data = target.read_bytes()
        manifest[lesson] = {"filename": filename, "sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
