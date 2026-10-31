import argparse
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

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


def clean(text, work):
    text = text.replace(str(work), '<WORK>')
    text = re.sub(r'127\.0\.0\.1:\d+', '127.0.0.1:<PORT>', text)
    text = re.sub(r'"ts":"[^"]+"', '"ts":"<TIME>"', text)
    text = re.sub(r'"service.instance.id"\s*:\s*"[^"]+"', '"service.instance.id":"<INSTANCE>"', text)
    return text


def start(work, name, replacements):
    text = (ROOT / (name + '.yaml')).read_text()
    for key, value in replacements.items():
        if key == 'METRICS':
            text = text.replace('"@METRICS@"', str(value))
        else:
            text = text.replace('@' + key + '@', str(value))
    path = work / (name + '.yaml')
    path.write_text(text)
    valid = subprocess.run([BINARY, 'validate', '--config', str(path)], capture_output=True, text=True, timeout=10)
    (args.evidence / (name + '-validate.txt')).write_text(clean(valid.stdout + valid.stderr, work))
    check(name + '-valid', valid.returncode == 0, 'validate exit 0')
    handle = (work / (name + '.log')).open('w')
    process = subprocess.Popen([BINARY, '--config', str(path)], stdout=handle, stderr=handle)
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError((work / (name + '.log')).read_text())
        if 'Everything is ready' in (work / (name + '.log')).read_text():
            return process, handle, name
        time.sleep(.03)
    process.kill()
    process.wait()
    raise TimeoutError('Collector readiness timeout')


def stop(work, owned):
    for process, handle, name in reversed(owned):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        handle.flush()
        (args.evidence / (name + '-collector.txt')).write_text(clean((work / (name + '.log')).read_text(), work))
        handle.close()
        check(name + '-shutdown', process.returncode == 0, 'owned process exited 0')
    for path in work.glob('*.jsonl'):
        text = path.read_text()
        text = re.sub(r'"key":"service.instance.id","value":\{"stringValue":"[^"]+"\}', '"key":"service.instance.id","value":{"stringValue":"<INSTANCE>"}', text)
        (args.evidence / path.name).write_text(text)


def get(endpoint, path='/metrics'):
    with urllib.request.urlopen(f'http://127.0.0.1:{endpoint}' + path, timeout=6) as response:
        return response.status, response.read()


def post(endpoint):
    request = urllib.request.Request(f'http://127.0.0.1:{endpoint}/v1/traces', data=(ROOT / 'traces.data').read_bytes(), headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def metric(text, name):
    values = re.findall('^' + re.escape(name) + r'(?:\{[^}]*\})? ([^\s]+)', text, re.MULTILINE)
    return sum(float(v) for v in values)


def read_spans(path):
    if not path.exists():
        return []
    return [span for line in path.read_text().splitlines() if line for r in json.loads(line).get('resourceSpans', []) for scope in r['scopeSpans'] for span in scope['spans']]
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading


class RejectingDestination(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', '0')))
        self.send_response(503)
        self.end_headers()
        self.wfile.write(b'bounded fixture destination unavailable')

    def log_message(self, *unused):
        pass

with tempfile.TemporaryDirectory(prefix='collector-health-') as directory:
    work = Path(directory)
    server = ThreadingHTTPServer(('127.0.0.1', 0), RejectingDestination)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    entry, metrics_port, health = port(), port(), port()
    owned = []
    try:
        owned.append(start(work, 'degraded', {'HTTP': entry, 'METRICS': metrics_port, 'TARGET': server.server_port, 'HEALTH': health}))
        statuses = [post(entry)[0] for _ in range(6)]
        (args.evidence / 'request-statuses.data').write_text(json.dumps(statuses) + '\n')
        time.sleep(.1)
        status, body = get(health, '/')
        (args.evidence / 'health-response.data').write_text(json.dumps({'http_status': status, 'body': body.decode()}, indent=2) + '\n')
        text = get(metrics_port)[1].decode()
        (args.evidence / 'degraded-metrics.txt').write_text(text)
        check('health-still-200', status == 200, 'default legacy health remains ready during demonstrated export failure')
        check('accepted-and-refused-requests', 200 in statuses and any(s >= 400 for s in statuses), statuses)
        check('queue-observed', metric(text, 'otelcol_exporter_queue_capacity') == 2 and metric(text, 'otelcol_exporter_queue_size') > 0, 'queue capacity two; pending batches observed')
        check('enqueue-and-refusal-observed', metric(text, 'otelcol_exporter_enqueue_failed_spans') > 0 and metric(text, 'otelcol_receiver_refused_spans') > 0, 'queue overflow produces enqueue failure and receiver refusal')
        time.sleep(1.2)
        later = get(metrics_port)[1].decode()
        (args.evidence / 'later-metrics.txt').write_text(later)
        check('send-failure-observed', metric(later, 'otelcol_exporter_send_failed_spans') > 0 and metric(later, 'otelcol_exporter_sent_spans') == 0, 'failed retry-bounded export, no successfully sent spans')
    finally:
        stop(work, owned)
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
print(json.dumps({'passed': len(checks)}))
