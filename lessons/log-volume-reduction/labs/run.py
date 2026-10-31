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

inventory = subprocess.check_output([BINARY, 'components'], text=True)
for component in ['log_dedup', 'groupbyattrs', 'transform', 'count']:
    check('inventory-' + component, ('- name: ' + component + '\n') in inventory, 'component present in official distribution')
(args.evidence / 'component-inventory.txt').write_text('\n'.join(line for line in inventory.splitlines() if any(('- name: ' + c) in line for c in ['log_dedup', 'groupbyattrs', 'transform', 'count'])) + '\n')
dedup = records(execute('dedup', [('logs', 'logs.data')], 1.5), 'logs')
check('dedup-records', len(dedup) == 4, 'six input records become four representatives')
check('dedup-multiplicity', sorted(int(attributes(l)['log_count']) for l in dedup) == [1, 1, 1, 3], 'representative multiplicities total six')
check('dedup-bodies', sorted(l['body']['stringValue'] for l in dedup) == ['heartbeat ok', 'order 123 created', 'order 456 created', 'order 789 failed'], 'distinct order text preserved; identical heartbeat occurrences summarized')
unique = records(execute('unique-control', [('logs', 'unique-logs.data')], 1.5), 'logs')
check('unique-keys-prevent-dedup', len(unique) == 6 and all(int(attributes(l)['log_count']) == 1 for l in unique), 'per-event attributes make all six records distinct')
grouped = execute('grouped', [('logs', 'logs.data')])
grouped_logs = records(grouped, 'logs')
check('grouping-retains-records', len(grouped_logs) == 6 and sorted(l['body']['stringValue'] for l in grouped_logs) == sorted(l['body']['stringValue'] for l in records([json.loads((ROOT / 'logs.data').read_text())], 'logs')), 'curated grouping retains all six original bodies')
patterns = [attributes(r['resource']).get('lesson.pattern') for batch in grouped for r in batch.get('resourceLogs', [])]
check('grouping-patterns', sorted(patterns) == ['heartbeat', 'order-created', 'order-failed'], 'three configured resource groups, not automatic pattern discovery')
aggregated = execute('aggregated', [('logs', 'logs.data')])
metrics = records(aggregated, 'metrics')
points = [p for m in metrics if m['name'] == 'lesson.log.records' for p in m['sum']['dataPoints']]
check('aggregate-counts', {attributes(p)['lesson.pattern']: int(p['asInt']) for p in points} == {'heartbeat': 3, 'order-created': 2, 'order-failed': 1}, 'three aggregate points retain six occurrences by pattern')
check('aggregation-no-raw-logs', not records(aggregated, 'logs') and 'order 123 created' not in json.dumps(aggregated), 'raw log bodies are absent from the metric-only destination')
print(json.dumps({'passed': len(checks)}))
