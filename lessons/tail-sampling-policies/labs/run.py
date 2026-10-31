import argparse
from collections import Counter
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


def attributes(item):
    return {entry['key']: next(iter(entry['value'].values())) for entry in item.get('attributes', [])}


def records(items, signal):
    outer, scope, key = {'traces': ('resourceSpans', 'scopeSpans', 'spans'), 'logs': ('resourceLogs', 'scopeLogs', 'logRecords'), 'metrics': ('resourceMetrics', 'scopeMetrics', 'metrics')}[signal]
    return [record for batch in items for resource in batch.get(outer, []) for group in resource[scope] for record in group[key]]


def execute(name, inputs, delay=0):
    with tempfile.TemporaryDirectory(prefix=ROOT.parent.name + '-') as directory:
        work = Path(directory)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        config = (ROOT / (name + '.yaml')).read_text().replace('@PORT@', str(port)).replace('@OUTPUT@', str(work))
        path = work / 'collector.yaml'
        path.write_text(config)
        valid = subprocess.run([BINARY, 'validate', '--config', str(path)], capture_output=True, text=True)
        (args.evidence / (name + '-validate.txt')).write_text(valid.stdout + valid.stderr)
        check(name + '-valid', valid.returncode == 0, 'validate exit 0')
        with (work / 'collector.log').open('w') as handle:
            process = subprocess.Popen([BINARY, '--config', str(path)], stdout=handle, stderr=handle)
            try:
                for _ in range(150):
                    assert process.poll() is None, 'Collector exited before readiness'
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise TimeoutError('Loopback listener did not start')
                for signal_name, filename in inputs:
                    req = urllib.request.Request(f'http://127.0.0.1:{port}/v1/{signal_name}', data=(ROOT / filename).read_bytes(), headers={'Content-Type': 'application/json'})
                    with urllib.request.urlopen(req, timeout=5) as response:
                        body = response.read().decode()
                        (args.evidence / (name + '-' + signal_name + '-response.data')).write_text(body + '\n')
                        check(name + '-' + signal_name + '-accepted', response.status == 200 and not json.loads(body).get('partialSuccess'), 'HTTP 200, no partial rejection')
                if delay:
                    path = work / 'output.jsonl'
                    check(name + '-buffered-before-decision', not path.exists() or path.stat().st_size == 0, 'No immediate output before configured wait/aggregation window')
                    time.sleep(delay)
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                handle.flush()
                log = (work / 'collector.log').read_text().replace(str(work), '<WORK>').replace('127.0.0.1:' + str(port), '127.0.0.1:<PORT>')
                log = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', log, flags=re.MULTILINE)
                log = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', log)
                (args.evidence / (name + '-collector.txt')).write_text(log)
                for output in work.glob('*.jsonl'):
                    (args.evidence / (name + '-' + output.name)).write_bytes(output.read_bytes())
            check(name + '-shutdown', process.returncode == 0, 'owned process stopped cleanly')
        path = work / 'output.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines() if line] if path.exists() else []

expected = {'errors': [2, 4], 'slow': [3, 4], 'compound': [4], 'either': [2, 3, 4], 'representative': [2, 3, 4]}
original = {s['spanId']: s for s in records([json.loads((ROOT / 'traces.data').read_text())], 'traces')}
for case, ids in expected.items():
    output = records(execute(case, [('traces', 'traces.data')], 2.5), 'traces')
    count = Counter(s['traceId'] for s in output)
    wanted = [f'{n:032x}' for n in ids]
    check(case + '-kept-ids', sorted(count) == wanted, {'expected_trace_ids': wanted, 'observed_trace_ids': sorted(count)})
    check(case + '-complete-fixture-traces', all(v == 2 for v in count.values()), 'both root and child retained for every selected complete fixture trace')
    check(case + '-identities', all(all(s.get(key) == original[s['spanId']].get(key) for key in ['traceId', 'spanId', 'name', 'parentSpanId', 'status', 'startTimeUnixNano', 'endTimeUnixNano']) for s in output), 'selected span identities, timing and outcome preserved')
print(json.dumps({'passed': len(checks)}))
