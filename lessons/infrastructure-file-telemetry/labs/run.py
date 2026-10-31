import argparse
import json
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True, type=Path)
parser.add_argument('--evidence', required=True, type=Path)
args = parser.parse_args()
BINARY = str(args.collector.resolve())
assert subprocess.check_output([BINARY, '--version'], text=True).strip() == 'otelcol-contrib version 0.163.1'
args.evidence.mkdir(parents=True, exist_ok=False)
checks = []


def check(name, condition, observed):
    assert condition, (name, observed)
    checks.append({'id': name, 'status': 'pass', 'observed': observed})
    (args.evidence / 'results.data').write_text(json.dumps({'collector': '0.163.1', 'checks': checks}, indent=2) + '\n')


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def start(work, name, replacements):
    config = (ROOT / (name + '.yaml')).read_text()
    for key, value in replacements.items():
        config = config.replace('@' + key + '@', str(value))
    path = work / (name + '.yaml')
    path.write_text(config)
    valid = subprocess.run([BINARY, 'validate', '--config', str(path)], capture_output=True, text=True)
    (args.evidence / (name + '-validate.txt')).write_text(valid.stdout + valid.stderr)
    check(name + '-valid', valid.returncode == 0, 'validate exit 0')
    handle = (work / (name + '.log')).open('w')
    process = subprocess.Popen([BINARY, '--config', str(path)], stdout=handle, stderr=handle)
    return process, handle, name


def wait_listener(process, endpoint):
    for _ in range(150):
        assert process.poll() is None, 'Collector exited before listener'
        try:
            with socket.create_connection(('127.0.0.1', endpoint), timeout=.1):
                return
        except OSError:
            time.sleep(.05)
    raise TimeoutError('Collector did not bind loopback socket')


def stop(work, owned):
    for process, handle, name in reversed(owned):
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        handle.flush()
        text = (work / (name + '.log')).read_text()
        text = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', text, flags=re.MULTILINE)
        text = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', text)
        text = text.replace(str(work), '<WORK>')
        (args.evidence / (name + '-collector.txt')).write_text(text)
        handle.close()
        check(name + '-shutdown', process.returncode == 0, 'owned process exit 0')
    for path in work.glob('*.jsonl'):
        (args.evidence / path.name).write_bytes(path.read_bytes())


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading


class Target(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if self.path == '/metrics' else 404)
        self.send_header('Content-Type', 'text/plain; version=0.0.4')
        self.end_headers()
        if self.path == '/metrics':
            self.wfile.write((ROOT / 'prometheus.txt').read_bytes())

    def log_message(self, *unused):
        pass


with tempfile.TemporaryDirectory(prefix='infrastructure-signals-') as directory:
    work = Path(directory)
    (work / 'application.log').write_bytes((ROOT / 'application.txt').read_bytes())
    server = ThreadingHTTPServer(('127.0.0.1', 0), Target)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    owned = []
    try:
        owned.append(start(work, 'collector', {'TARGET': server.server_port, 'INPUT': work / 'application.log', 'OUTPUT': work}))
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            assert owned[0][0].poll() is None, 'Collector exited'
            try:
                exported = rows(work / 'metrics.jsonl')
                metrics = [m for row in exported for r in row.get('resourceMetrics', []) for s in r['scopeMetrics'] for m in s['metrics']]
                logs = [record for row in rows(work / 'logs.jsonl') for r in row.get('resourceLogs', []) for s in r['scopeLogs'] for record in s['logRecords']]
                if any(m['name'] == 'system.cpu.time' for m in metrics) and any(m['name'] == 'lesson_queue_depth' for m in metrics) and len(logs) == 2:
                    break
            except (FileNotFoundError, json.JSONDecodeError):
                pass
            time.sleep(.1)
        else:
            raise TimeoutError('Expected host, scrape and file records not observed')
    finally:
        stop(work, owned)
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    metrics = [m for row in rows(work / 'metrics.jsonl') for r in row['resourceMetrics'] for s in r['scopeMetrics'] for m in s['metrics']]
    cpu = [m for m in metrics if m['name'] == 'system.cpu.time']
    gauge = [m for m in metrics if m['name'] == 'lesson_queue_depth']
    logs = [record for row in rows(work / 'logs.jsonl') for r in row['resourceLogs'] for s in r['scopeLogs'] for record in s['logRecords']]
    check('host-cpu-observed', bool(cpu) and bool(cpu[0]['sum']['dataPoints']), 'native host CPU cumulative observations present; values are machine-specific')
    check('prometheus-target-observed', bool(gauge) and all(float(point.get('asDouble', point.get('asInt', 0))) == 7 for m in gauge for point in m['gauge']['dataPoints']), 'synthetic gauge lesson_queue_depth=7')
    check('existing-file-read', sorted(r['body']['stringValue'] for r in logs) == ['checkout-error', 'checkout-ready'], 'exact two preexisting lines read with start_at beginning')
print(json.dumps({'passed': len(checks), 'collector': '0.163.1'}))
