import argparse
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True)
parser.add_argument('--evidence', required=True)
args = parser.parse_args()
binary = str(Path(args.collector).resolve())
assert subprocess.check_output([binary, '--version'], text=True).strip() == 'otelcol-contrib version 0.163.1'
evidence = Path(args.evidence).resolve()
evidence.mkdir(parents=True, exist_ok=False)
checks = []

def check(name, condition):
    assert condition, name
    checks.append(name)
    (evidence / 'results.data').write_text(json.dumps({'collector': '0.163.1', 'passed': len(checks), 'checks': checks}, indent=2) + '\n')

def wait_until(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, 'bounded wait expired'
        time.sleep(.03)

def available_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

def names(payloads):
    return [span['name'] for payload in payloads for r in payload.get('resourceSpans', []) for scope in r['scopeSpans'] for span in scope['spans']]

class Destination:
    def __init__(self):
        self.healthy = threading.Event()
        self.failed_attempt = threading.Event()
        self.delivered = []
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                if self.headers.get('Content-Encoding') == 'gzip':
                    body = gzip.decompress(body)
                payload = json.loads(body)
                if owner.healthy.is_set():
                    owner.delivered.append(payload)
                    self.send_response(200)
                else:
                    owner.failed_attempt.set()
                    self.send_response(503)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{}')
            def log_message(self, *unused):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

class Collector:
    def __init__(self, config, work, destination, suffix):
        self.port = available_port()
        self.log_path = work / (suffix + '.log')
        storage = work / 'storage'
        storage.mkdir(exist_ok=True)
        env = dict(os.environ, LESSON_PORT=str(self.port), LESSON_DESTINATION=str(destination.server.server_port), LESSON_STORAGE=str(storage))
        path = ROOT / 'labs' / (config + '.yaml')
        validation = subprocess.run([binary, 'validate', '--config', str(path)], env=env, capture_output=True, text=True, timeout=15)
        retain(suffix + '-validate', validation.stdout + validation.stderr, work)
        check(suffix + '-valid', validation.returncode == 0)
        self.handle = self.log_path.open('w')
        self.process = subprocess.Popen([binary, '--config', str(path)], env=env, stdout=self.handle, stderr=self.handle)
        self.work = work
        self.suffix = suffix
        try:
            wait_until(lambda: 'Everything is ready' in self.log_path.read_text(), timeout=15)
            assert self.process.poll() is None
        except BaseException:
            self.stop(kill=True)
            raise
    def post(self, sequence):
        payload = json.loads((ROOT / 'labs/input.data').read_text())
        for r in payload['resourceSpans']:
            for scope in r['scopeSpans']:
                for span in scope['spans']:
                    span['name'] += '-' + str(sequence)
        request = urllib.request.Request(f'http://127.0.0.1:{self.port}/v1/traces', data=json.dumps(payload).encode(), headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return {'status': response.status, 'body': response.read().decode(), 'sequence': sequence}
        except urllib.error.HTTPError as error:
            return {'status': error.code, 'body': error.read().decode(), 'sequence': sequence}
    def stop(self, kill=False):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGKILL if kill else signal.SIGTERM)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.handle.close()
        retain(self.suffix + '-collector', self.log_path.read_text(), self.work)

def retain(name, text, work):
    text = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', text, flags=re.MULTILINE)
    text = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', text)
    text = text.replace(str(work), '<WORK>')
    (evidence / (name + '.txt')).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip() + '\n')

def retain_delivery(name, destination, responses):
    (evidence / (name + '-responses.data')).write_text(json.dumps(responses, indent=2) + '\n')
    (evidence / (name + '-destination.jsonl')).write_text(''.join(json.dumps(payload) + '\n' for payload in destination.delivered))

accepted_by_sizer = {}
for case in ['requests', 'items']:
    with tempfile.TemporaryDirectory(prefix='queue-sizing-') as directory:
        destination = Destination()
        collector = None
        responses = []
        try:
            collector = Collector(case, Path(directory), destination, case)
            responses.append(collector.post(1))
            wait_until(destination.failed_attempt.is_set)
            for sequence in range(2, 7):
                responses.append(collector.post(sequence))
                if responses[-1]['status'] != 200:
                    break
            accepted = sum(r['status'] == 200 for r in responses)
            accepted_by_sizer[case] = accepted
            check(case + '-bounded-overflow', responses[-1]['status'] >= 400)
            check(case + '-outage-delivered-none', not destination.delivered)
            destination.healthy.set()
            wait_until(lambda: len(names(destination.delivered)) == accepted * 2)
            check(case + '-accepted-population-recovered', sorted(names(destination.delivered)) == sorted('checkout-' + outcome + '-' + str(r['sequence']) for r in responses if r['status'] == 200 for outcome in ['success', 'error']))
        finally:
            destination.healthy.set()
            if collector:
                collector.stop()
            retain_delivery(case, destination, responses)
            destination.close()
check('requests-exact-capacity', accepted_by_sizer['requests'] == 2)
check('items-exact-capacity', accepted_by_sizer['items'] == 1)
check('sizer-unit-changes-capacity', accepted_by_sizer['requests'] > accepted_by_sizer['items'])
(evidence / 'capacity.data').write_text(json.dumps({'queue_size': 2, 'num_consumers': 1, 'spans_per_request': 2, 'accepted_before_overflow': accepted_by_sizer}, indent=2) + '\n')
print(f'{len(checks)} checks passed')
