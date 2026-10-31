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


with tempfile.TemporaryDirectory(prefix='export-verify-') as directory:
    work = Path(directory)
    target, entry = port(), port()
    owned = []
    try:
        owned.append(start(work, 'destination', {'HTTP': target, 'OUTPUT': work}))
        wait_listener(owned[0][0], target)
        owned.append(start(work, 'forwarder', {'HTTP': entry, 'TARGET': target}))
        wait_listener(owned[-1][0], entry)
        for signal_name in ['traces', 'metrics', 'logs']:
            status, body = post(entry, signal_name)
            (args.evidence / (signal_name + '-response.data')).write_text(json.dumps({'http_status': status, 'body': body}, indent=2) + '\n')
            check(signal_name + '-forwarded-request', status == 200 and not json.loads(body).get('partialSuccess'), 'HTTP 200, no reported rejection')
    finally:
        stop(work, owned)
    output = rows(work / 'received.jsonl')
    for signal_name, outer, scoped, records in [('traces', 'resourceSpans', 'scopeSpans', 'spans'), ('metrics', 'resourceMetrics', 'scopeMetrics', 'metrics'), ('logs', 'resourceLogs', 'scopeLogs', 'logRecords')]:
        expected = json.loads((ROOT / (signal_name + '.data')).read_text())[outer][0]
        resources = [r for row in output for r in row.get(outer, [])]
        scopes = [s for r in resources for s in r[scoped]]
        actual = [record for scope in scopes for record in scope[records]]
        check(signal_name + '-received-content', bool(resources) and all(r['resource'] == expected['resource'] for r in resources) and all(s['scope'] == expected[scoped][0]['scope'] for s in scopes) and sorted(actual, key=lambda r: json.dumps(r, sort_keys=True)) == sorted(expected[scoped][0][records], key=lambda r: json.dumps(r, sort_keys=True)), {'records': len(actual), 'resource_scope_and_content': 'preserved at destination'})
print(json.dumps({'passed': len(checks), 'collector': '0.163.1'}))
