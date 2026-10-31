import argparse
import json
from pathlib import Path
import re
import signal
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


def post(endpoint, signal_name):
    request = urllib.request.Request(f'http://127.0.0.1:{endpoint}/v1/{signal_name}', data=(ROOT / (signal_name + '.data')).read_bytes(), headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def spans(path):
    return [span for row in rows(path) for resource in row.get('resourceSpans', []) for scope in resource['scopeSpans'] for span in scope['spans']]

for mode in ['http', 'grpc', 'mismatch']:
    with tempfile.TemporaryDirectory(prefix='otlp-transport-') as directory:
        work = Path(directory)
        http, grpc, entry = port(), port(), port()
        owned = []
        try:
            owned.append(start(work, 'receiver', {'HTTP': http, 'GRPC': grpc, 'OUTPUT': work}))
            wait_listener(owned[0][0], http)
            wait_listener(owned[0][0], grpc)
            if mode == 'http':
                endpoint = http
            else:
                owned.append(start(work, 'sender', {'ENTRY': entry, 'TARGET': grpc if mode == 'grpc' else http}))
                wait_listener(owned[-1][0], entry)
                endpoint = entry
            status, body = post(endpoint, 'traces')
            (args.evidence / (mode + '-response.data')).write_text(json.dumps({'http_status': status, 'body': body}, indent=2) + '\n')
            check(mode + '-request', status == 200 if mode != 'mismatch' else status >= 400, {'http_status': status})
        finally:
            stop(work, owned)
        output = work / 'destination.jsonl'
        observed = spans(output) if output.exists() and output.stat().st_size else []
        check(mode + '-destination', len(observed) == (0 if mode == 'mismatch' else 2), {'span_count': len(observed)})
        if observed:
            check(mode + '-names', sorted(s['name'] for s in observed) == ['checkout-error', 'checkout-success'], 'expected named spans')
        if output.exists():
            (args.evidence / (mode + '-destination.jsonl')).write_bytes(output.read_bytes())
        for path in args.evidence.glob('*-collector.txt'):
            if not path.name.startswith(('http-', 'grpc-', 'mismatch-')):
                path.rename(path.with_name(mode + '-' + path.name))
print(json.dumps({'passed': len(checks), 'collector': '0.163.1'}))
