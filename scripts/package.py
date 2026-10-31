"""Build deterministic learner ZIPs from explicit module file lists."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PILOTS = {
    "prepare-lab-environment": ["README.md", "check.py", "collector.yaml"],
    "first-otlp-pipeline": ["README.md", "collector.yaml", "input.data", "run.py"],
}


def package(root, output):
    modules = json.loads((root / "modules.json").read_text())
    if modules.get("schema_version") != 1:
        raise ValueError("Unsupported module inventory")
    packages = {lesson: [f"lessons/{lesson}/labs/{name}" for name in names]
                for lesson, names in PILOTS.items()}
    seen = set()
    for module in modules["modules"]:
        identifier = module["id"]
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", identifier) or identifier in seen:
            raise ValueError("Invalid or duplicate module ID")
        seen.add(identifier)
        if not module["files"] or len(set(module["files"])) != len(module["files"]):
            raise ValueError("Expected unique explicit module files")
        packages[identifier] = module["files"]
    output.mkdir(parents=True, exist_ok=False)
    manifest = {}
    for identifier, names in packages.items():
        filename = identifier + (".zip" if identifier.endswith("-lab") else "-lab.zip")
        target = output / filename
        with zipfile.ZipFile(target, "w") as archive:
            for name in sorted(names):
                relative = Path(name)
                source = root / relative
                if (relative.is_absolute() or ".." in relative.parts
                        or relative.parts[0] != "lessons"
                        or source.resolve() != source or not source.is_file()
                        or source.suffix not in {".py", ".yaml", ".data", ".txt", ".sh", ".md"}
                        or (source.suffix == ".md" and source.name != "README.md")
                        or "evidence" in relative.parts
                        or ("validation" in relative.parts and source.suffix != ".py")):
                    raise ValueError(f"Expected an allowlisted regular runnable file: {relative}")
                info = zipfile.ZipInfo(relative.as_posix(), date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_STORED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, source.read_bytes())
        data = target.read_bytes()
        manifest[identifier] = {"filename": filename, "sha256": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(package(ROOT, args.output), indent=2))


if __name__ == "__main__":
    main()
