"""Check the local Collector lab foundation without sending telemetry."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

VERSION = "otelcol-contrib version 0.163.1"
ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, type=Path)
    args = parser.parse_args()
    workspace = args.workspace.expanduser().resolve()
    collector = workspace / "bin" / "otelcol-contrib"
    if sys.version_info < (3, 11):
        raise RuntimeError("Install Python 3.11 or newer, then rerun this check.")
    if not collector.is_file():
        raise RuntimeError("Collector missing: install the verified 0.163.1 binary in workspace/bin/otelcol-contrib.")
    version = subprocess.check_output([str(collector), "--version"], text=True, timeout=10).strip()
    if version != VERSION:
        raise RuntimeError(f"Expected {VERSION}; observed {version}. Install the pinned release.")
    evidence_root = workspace / "evidence"
    evidence_root.mkdir(exist_ok=True)
    evidence = Path(tempfile.mkdtemp(prefix="setup-", dir=evidence_root))
    print(f"Evidence directory: {evidence}", flush=True)
    checks = ["python-version", "collector-version", "writable-workspace"]
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    env = dict(os.environ, LAB_OTLP_ENDPOINT=f"127.0.0.1:{port}")
    command = [str(collector), "--config", str(ROOT / "collector.yaml")]
    validation = subprocess.run([str(collector), "validate", "--config", str(ROOT / "collector.yaml")], env=env, capture_output=True, text=True, timeout=15)
    (evidence / "validate.txt").write_text(validation.stdout + validation.stderr)
    if validation.returncode != 0:
        raise RuntimeError("Configuration validation failed; inspect validate.txt in the evidence directory.")
    checks.append("valid-configuration")
    with (evidence / "collector.txt").open("w") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        try:
            for _ in range(100):
                if process.poll() is not None:
                    raise RuntimeError("Collector stopped before readiness; inspect collector.txt.")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.05)
            else:
                raise RuntimeError("Receiver did not bind; inspect collector.txt and retry with a fresh check.")
            checks.append("loopback-receiver")
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
    if process.returncode != 0:
        raise RuntimeError("Collector did not shut down cleanly; inspect collector.txt.")
    checks.append("clean-shutdown")
    (evidence / "results.json").write_text(json.dumps({"collector_version": version, "checks": checks}, indent=2) + "\n")
    print(f"READY: {len(checks)} setup checks passed. No telemetry was sent.")
    print("Keep workspace/bin for the lesson labs. Setup readiness does not prove telemetry delivery.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"SETUP FAILED: {error}", file=sys.stderr)
        sys.exit(1)
