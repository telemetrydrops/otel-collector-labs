"""Execute bounded, local evidence checks with the exact course binary."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--collector", required=True)
parser.add_argument("--evidence", type=Path)
args = parser.parse_args()
if args.evidence:
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
else:
    evidence = Path(tempfile.mkdtemp(prefix="read-evidence-results-"))
binary = str(Path(args.collector).resolve())
version = subprocess.check_output([binary, "--version"], text=True).strip()
assert version == "otelcol-contrib version 0.163.1", version
results = []

def check(label, condition):
    assert condition, label
    results.append(label)

with tempfile.TemporaryDirectory(prefix="read-evidence-") as scratch:
    for name in ["working", "broken-reference", "metrics-only"]:
        with socket.socket() as port_socket:
            port_socket.bind(("127.0.0.1", 0))
            port = port_socket.getsockname()[1]
        env = dict(os.environ, LESSON_PORT=str(port))
        config = root / "labs" / (name + ".yaml")
        command = [binary, "--config", str(config)]
        validation = subprocess.run([binary, "validate", "--config", str(config)], env=env, capture_output=True, text=True, timeout=15)
        (evidence / (name + "-validate.txt")).write_text(validation.stdout + validation.stderr)
        check(name + "-validation", (validation.returncode != 0) == (name == "broken-reference"))
        log = Path(scratch) / (name + ".txt")
        with log.open("w") as out:
            process = subprocess.Popen(command, env=env, stdout=out, stderr=subprocess.STDOUT)
            try:
                if name == "broken-reference":
                    check("broken-reference-exits", process.wait(timeout=15) != 0)
                    check("broken-reference-identifies-name", "otlp/missing" in log.read_text())
                    continue
                deadline = time.monotonic() + 15
                while "Everything is ready" not in log.read_text():
                    assert process.poll() is None, log.read_text()
                    assert time.monotonic() < deadline, "startup timeout"
                    time.sleep(0.05)
                check(name + "-ready", True)
                check(name + "-idle-no-span", "checkout-success" not in log.read_text())
                request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/traces", data=(root / "labs/input.data").read_bytes(), headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(request, timeout=5) as response:
                        status, body = response.status, response.read().decode()
                except urllib.error.HTTPError as error:
                    status, body = error.code, error.read().decode()
                (evidence / (name + "-response.txt")).write_text(f"HTTP {status}\n{body}\n")
                check(name + "-http-status", status == (200 if name == "working" else 404))
                if name == "working":
                    deadline = time.monotonic() + 5
                    while "Status code    : Error" not in log.read_text():
                        assert time.monotonic() < deadline, log.read_text()
                        time.sleep(0.05)
                    check("working-response-no-partial-rejection", json.loads(body) in ({}, {"partialSuccess": {}}))
                    output = log.read_text()
                    check("working-exact-span-count", output.count("Span #") == 2)
                    for token in ["service.name: Str(checkout-demo)", "checkout-success", "checkout-error", "Status code    : Ok", "Status code    : Error", "lesson.case: Str(success)", "lesson.case: Str(error)"]:
                        check("exported-" + token, token in output)
                else:
                    check("metrics-only-no-trace-export", "checkout-success" not in log.read_text())
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
                    process.wait(timeout=10)
                (evidence / (name + "-collector.txt")).write_text(log.read_text())
(evidence / "results.data").write_text(json.dumps({"collector": version, "checks": results, "passed": len(results)}, indent=2) + "\n")
for output in evidence.glob("*.txt"):
    output.write_text("\n".join(line.rstrip() for line in output.read_text().splitlines()).rstrip() + "\n")
print(f"{len(results)} checks passed: {version}; evidence: {evidence}")
