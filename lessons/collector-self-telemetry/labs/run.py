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

with tempfile.TemporaryDirectory(prefix='collector-self-') as directory:
    work = Path(directory)
    entry, telemetry_port, observer = port(), port(), port()
    owned = []
    try:
        owned.append(start(work, 'observer', {'HTTP': observer, 'OUTPUT': work}))
        owned.append(start(work, 'source', {'HTTP': entry, 'METRICS': telemetry_port, 'OBSERVER': observer, 'OUTPUT': work}))
        status, body = post(entry)
        check('trace-request-accepted', status == 200, {'http_status': status})
        text = get(telemetry_port)[1].decode()
        (args.evidence / 'internal-metrics.txt').write_text(text)
        check('accepted-eight', metric(text, 'otelcol_receiver_accepted_spans') == 8, 'receiver accepted eight spans')
        check('sent-eight', metric(text, 'otelcol_exporter_sent_spans') == 8, 'file exporter sent eight spans')
        check('refused-zero', 'otelcol_receiver_refused_spans' in text and metric(text, 'otelcol_receiver_refused_spans') == 0, 'refused instrument present with zero')
        check('resource-indicators', metric(text, 'otelcol_process_memory_rss') > 0 and metric(text, 'otelcol_process_runtime_heap_alloc_bytes') > 0 and metric(text, 'otelcol_process_cpu_seconds') >= 0, 'positive RSS/heap and cumulative CPU observation; no pressure threshold inferred')
    finally:
        stop(work, owned)
    check('actual-data-eight', len(read_spans(work / 'output.jsonl')) == 8, 'application fixture destination contains eight original spans')
    internal = read_spans(work / 'internal.jsonl')
    check('internal-spans-observed', any(s['name'] == 'exporter/file/traces' for s in internal) and any(s['name'] == 'receiver/otlp/TraceDataReceived' for s in internal), {'names': sorted(set(s['name'] for s in internal))})
    lines = [json.loads(line) for line in (work / 'source.log').read_text().splitlines()]
    check('structured-startup-log', any(line.get('msg') == 'Everything is ready. Begin running and processing data.' for line in lines), 'actual JSON internal log with startup message')
print(json.dumps({'passed': len(checks)}))
