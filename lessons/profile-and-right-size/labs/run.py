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

import concurrent.futures
import hashlib


go_version = subprocess.check_output(['go', 'version'], text=True).strip()
comparison = {}
for name in ['basic', 'detailed']:
    with tempfile.TemporaryDirectory(prefix='collector-profile-') as directory:
        work = Path(directory)
        entry, metrics_port, profile_port = port(), port(), port()
        owned = []
        try:
            owned.append(start(work, name, {'HTTP': entry, 'METRICS': metrics_port, 'PROFILE': profile_port}))
            before = get(metrics_port)[1].decode()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                profile = pool.submit(get, profile_port, '/debug/pprof/profile?seconds=3')
                time.sleep(.1)
                statuses = [post(entry)[0] for _ in range(2000)]
                status, profile_bytes = profile.result(timeout=6)
            check(name + '-load-completed', statuses == [200] * 2000, '2000 finite requests, eight spans per request')
            check(name + '-cpu-profile', status == 200 and profile_bytes.startswith(b'\x1f\x8b'), {'bytes': len(profile_bytes), 'sha256': hashlib.sha256(profile_bytes).hexdigest()})
            path = work / 'cpu.pprof'
            path.write_bytes(profile_bytes)
            analysis = subprocess.run(['go', 'tool', 'pprof', '-top', '-nodecount=8', BINARY, str(path)], capture_output=True, text=True, timeout=20)
            (args.evidence / (name + '-cpu-top.txt')).write_text(clean(analysis.stdout + analysis.stderr, work))
            check(name + '-profile-parsed', analysis.returncode == 0 and 'Type: cpu' in analysis.stdout, 'Go pprof parsed the actual CPU profile')
            heap = get(profile_port, '/debug/pprof/heap?debug=1')[1].decode()
            (args.evidence / (name + '-heap-summary.txt')).write_text(heap)
            check(name + '-heap-observed', heap.startswith('heap profile:'), 'actual runtime heap profile text')
            after = get(metrics_port)[1].decode()
            (args.evidence / (name + '-metrics.txt')).write_text(after)
            check(name + '-accepted-sent', metric(after, 'otelcol_receiver_accepted_spans') == 16000 and metric(after, 'otelcol_exporter_sent_spans') == 16000, 'all 16000 supplied spans reached debug export')
            comparison[name] = {'requests': 2000, 'spans': 16000, 'cpu_delta_seconds': metric(after, 'otelcol_process_cpu_seconds') - metric(before, 'otelcol_process_cpu_seconds'), 'rss_bytes': metric(after, 'otelcol_process_memory_rss'), 'heap_alloc_bytes': metric(after, 'otelcol_process_runtime_heap_alloc_bytes'), 'cpu_profile_bytes': len(profile_bytes), 'cpu_profile_sha256': hashlib.sha256(profile_bytes).hexdigest()}
        finally:
            stop(work, owned)
        comparison[name]['internal_log_bytes'] = (work / (name + '.log')).stat().st_size
check('detailed-diagnostic-volume', comparison['detailed']['internal_log_bytes'] > comparison['basic']['internal_log_bytes'], 'detailed debug export generates more diagnostic text for the same finite input')
(args.evidence / 'profile-comparison.data').write_text(json.dumps({'go_tool': go_version, 'cases': comparison, 'interpretation': 'Single bounded local observations, not production sizing, representative load, a stable speedup or a capacity guarantee.'}, indent=2) + '\n')
print(json.dumps({'passed': len(checks)}))
