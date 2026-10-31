import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("package", ROOT / "scripts/package.py")
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


class ModulePackages(unittest.TestCase):
    def test_all_modules_are_deterministic_and_keep_runner_paths(self):
        modules = json.loads((ROOT / "modules.json").read_text())["modules"]
        self.assertEqual(len(modules), 13)
        self.assertEqual(sum(len(module["lessons"]) for module in modules), 47)
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            first = package.package(ROOT, folder / "first")
            second = package.package(ROOT, folder / "second")
            self.assertEqual(first, second)
            for module in modules:
                filename = first[module["id"]]["filename"]
                data = (folder / "first" / filename).read_bytes()
                self.assertEqual(data, (folder / "second" / filename).read_bytes())
                self.assertEqual(hashlib.sha256(data).hexdigest(), first[module["id"]]["sha256"])
                with zipfile.ZipFile(folder / "first" / filename) as archive:
                    self.assertEqual(archive.namelist(), sorted(module["files"]))
                    for runner in module["runners"]:
                        self.assertIn(runner["path"], archive.namelist())
                    for name in archive.namelist():
                        self.assertFalse(any(part in name for part in ["quiz.json", "assessment-evidence", "script.md", "transcript.md", "/evidence/"]))

    def test_public_lesson_tree_contains_only_listed_learner_files(self):
        modules = json.loads((ROOT / "modules.json").read_text())["modules"]
        listed = {name for module in modules for name in module["files"]}
        actual = {path.relative_to(ROOT).as_posix() for path in (ROOT / "lessons").rglob("*")
                  if path.is_file() and "__pycache__" not in path.parts}
        self.assertEqual(actual, listed)

    def test_legacy_pilot_bytes_are_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "packages"
            result = package.package(ROOT, output)
            for lesson, names in package.PILOTS.items():
                with zipfile.ZipFile(output / result[lesson]["filename"]) as archive:
                    self.assertEqual(archive.namelist(), [f"lessons/{lesson}/labs/{name}" for name in sorted(names)])
                    for name in names:
                        relative = f"lessons/{lesson}/labs/{name}"
                        original = subprocess.check_output(["git", "show", f"1500b3bdca691b651e4a6e19c10e304aed8c575e:{relative}"], cwd=ROOT)
                        self.assertEqual(archive.read(relative), original)

    def test_private_paths_and_escaping_aliases_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / "source"
            root.mkdir()
            shutil.copytree(ROOT / "lessons", root / "lessons")
            inventory = json.loads((ROOT / "modules.json").read_text())
            for index, path in enumerate(["../private.py", "lessons/private/quiz.json", "lessons/private/assessment-evidence.md", "lessons/private/validation/audio-review.data"]):
                candidate = root / path
                candidate.parent.mkdir(parents=True, exist_ok=True)
                candidate.write_text("private fixture")
                inventory["modules"][0]["files"] = [path]
                (root / "modules.json").write_text(json.dumps(inventory))
                with self.assertRaises(ValueError):
                    package.package(root, root / ("out" + str(index)))
            safe = root / "lessons/private"
            safe.mkdir(exist_ok=True)
            (safe / "alias.py").symlink_to(ROOT / "scripts/package.py")
            inventory["modules"][0]["files"] = ["lessons/private/alias.py"]
            (root / "modules.json").write_text(json.dumps(inventory))
            with self.assertRaises(ValueError):
                package.package(root, root / "alias-output")

    def test_every_runner_exposes_its_original_cli(self):
        inventory = json.loads((ROOT / "modules.json").read_text())
        for module in inventory["modules"]:
            for runner in module["runners"]:
                completed = subprocess.run([sys.executable, str(ROOT / runner["path"]), "--help"], capture_output=True, timeout=10)
                self.assertEqual(completed.returncode, 0, runner["path"] + completed.stderr.decode())


if __name__ == "__main__":
    unittest.main()
