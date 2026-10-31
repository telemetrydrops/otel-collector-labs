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

items = execute('baseline', [('traces', 'traces.data'), ('metrics', 'metrics.data'), ('logs', 'logs.data')])
summary = {}
for signal_name, outer in [('traces', 'resourceSpans'), ('metrics', 'resourceMetrics'), ('logs', 'resourceLogs')]:
    signal_records = records(items, signal_name)
    source = Counter()
    for batch in items:
        for resource in batch.get(outer, []):
            name = attributes(resource['resource'])['service.name']
            source[name] += len(records([{outer: [resource]}], signal_name))
    summary[signal_name] = {'records': len(signal_records), 'by_service': dict(source)}
traces = records(items, 'traces')
logs = records(items, 'logs')
metrics = records(items, 'metrics')
summary['traces']['by_status'] = dict(Counter(str(s['status']['code']) for s in traces))
summary['traces']['by_case'] = dict(Counter(attributes(s)['lesson.case'] for s in traces))
summary['logs']['by_severity'] = dict(Counter(l['severityText'] for l in logs))
summary['logs']['by_body'] = dict(Counter(l['body']['stringValue'] for l in logs))
check('signal-baseline', [len(traces), len(metrics), len(logs)] == [8, 1, 6], summary)
check('trace-outcomes', summary['traces']['by_status'] == {'1': 4, '2': 4}, 'four OK and four ERROR spans in four complete fixture traces')
check('log-outcomes', summary['logs']['by_severity'] == {'INFO': 5, 'ERROR': 1}, 'five INFO and one ERROR log')
check('source-baseline', summary['traces']['by_service'] == {'checkout-demo': 8} and summary['logs']['by_service'] == {'edge-proxy': 6}, 'source counts remain distinct')
check('gauge-is-not-volume', len(metrics[0]['gauge']['dataPoints']) == 1 and metrics[0]['gauge']['dataPoints'][0]['asInt'] == '7', 'one metric point with value seven, not seven telemetry records')
(args.evidence / 'baseline-summary.data').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({'passed': len(checks)}))
