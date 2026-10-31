import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
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

def retain(name, text, work):
    text = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', text, flags=re.MULTILINE)
    text = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', text)
    text = text.replace(str(work), '<WORK>')
    (evidence / (name + '.txt')).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip() + '\n')

def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line] if path.exists() else []

def metrics(path):
    return [m for row in rows(path) for r in row.get('resourceMetrics', []) for s in r['scopeMetrics'] for m in s['metrics']]

def spans(path):
    return [s for row in rows(path) for r in row.get('resourceSpans', []) for scope in r['scopeSpans'] for s in scope['spans']]

def logs(path):
    return [record for row in rows(path) for r in row.get('resourceLogs', []) for scope in r['scopeLogs'] for record in scope['logRecords']]

def total(values, name):
    return sum(float(point.get('asInt', point.get('asDouble', 0))) for m in values if m['name'] == name for point in m['sum']['dataPoints'])

def execute(name, signal_name='traces', invalid=False, asynchronous=False):
    with tempfile.TemporaryDirectory(prefix='connector-lesson-') as temporary:
        work = Path(temporary)
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        env = dict(os.environ, LESSON_PORT=str(port), LESSON_WORK=str(work))
        config = ROOT / 'labs' / (name + '.yaml')
        validation = subprocess.run([binary, 'validate', '--config', str(config)], env=env, capture_output=True, text=True, timeout=15)
        retain(name + '-validate', validation.stdout + validation.stderr, work)
        check(name + '-validation', (validation.returncode != 0) == invalid)
        if invalid:
            check(name + '-missing-receiver-diagnostic', 'connector \"count\"' in validation.stdout + validation.stderr and 'not used in any supported receiver pipeline' in validation.stdout + validation.stderr)
            return None
        log_path = work / 'collector.log'
        with log_path.open('w') as output:
            process = subprocess.Popen([binary, '--config', str(config)], env=env, stdout=output, stderr=output)
            try:
                deadline = time.monotonic() + 15
                while 'Everything is ready' not in log_path.read_text():
                    assert process.poll() is None, log_path.read_text()
                    assert time.monotonic() < deadline, 'readiness timeout'
                    time.sleep(.05)
                request = urllib.request.Request(f'http://127.0.0.1:{port}/v1/{signal_name}', data=(ROOT / 'labs' / (signal_name + '.data')).read_bytes(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=5) as response:
                    body = response.read().decode()
                    check(name + '-accepted', response.status == 200 and not json.loads(body).get('partialSuccess'))
                if asynchronous:
                    deadline = time.monotonic() + 5
                    while True:
                        try:
                            if total(metrics(work / 'metrics.jsonl'), 'traces.span.metrics.calls') >= 2:
                                break
                        except json.JSONDecodeError:
                            pass
                        assert time.monotonic() < deadline, 'span metrics flush timeout'
                        time.sleep(.05)
                process.send_signal(signal.SIGTERM)
                check(name + '-shutdown', process.wait(timeout=10) == 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                retain(name + '-startup', log_path.read_text(), work)
        result = {file.stem: file.read_text() for file in work.glob('*.jsonl')}
        for file in work.glob('*.jsonl'):
            (evidence / (name + '-' + file.name)).write_bytes(file.read_bytes())
        return {key: [json.loads(line) for line in value.splitlines() if line] for key, value in result.items()}

def result_spans(result, stream='traces'):
    return [span for row in result.get(stream, []) for r in row.get('resourceSpans', []) for scope in r['scopeSpans'] for span in scope['spans']]

def result_metrics(result):
    return [m for row in result.get('metrics', []) for r in row.get('resourceMetrics', []) for scope in r['scopeMetrics'] for m in scope['metrics']]

def result_logs(result):
    return [record for row in result.get('logs', []) for r in row.get('resourceLogs', []) for scope in r['scopeLogs'] for record in scope['logRecords']]

def verify_trace_fixture(name, records, expected=2):
    check(name + '-span-count', len(records) == expected)
    if expected:
        check(name + '-names', sorted(s['name'] for s in records) == ['checkout-error', 'checkout-success'])
        check(name + '-statuses', sorted(s['status']['code'] for s in records) == [1, 2])

result = execute('span-metrics', asynchronous=True)
verify_trace_fixture('span-metrics', result_spans(result))
values = result_metrics(result)
check('span-metrics-calls', total(values, 'traces.span.metrics.calls') == 2)
calls = [point for metric in values if metric['name'] == 'traces.span.metrics.calls' for point in metric['sum']['dataPoints']]
attributes = [{a['key']: a['value'].get('stringValue') for a in point['attributes']} for point in calls if float(point.get('asInt', point.get('asDouble', 0))) > 0]
check('span-metrics-case-dimensions', sorted(a['lesson.case'] for a in attributes) == ['error', 'success'])
check('span-metrics-service-dimension', all(a['service.name'] == 'checkout-demo' for a in attributes))
check('span-metrics-name-dimensions', sorted(a['span.name'] for a in attributes) == ['checkout-error', 'checkout-success'])
check('span-metrics-kind-dimension', all(a['span.kind'] == 'SPAN_KIND_SERVER' for a in attributes))
check('span-metrics-status-dimensions', sorted(a['status.code'] for a in attributes) == ['STATUS_CODE_ERROR', 'STATUS_CODE_OK'])
histograms = [m for m in values if m['name'] == 'traces.span.metrics.duration']
check('span-metrics-histogram-unit', bool(histograms) and all(m['unit'] == 'ms' for m in histograms))
points = [point for metric in histograms for point in metric['histogram']['dataPoints']]
check('span-metrics-duration-count', sum(int(point['count']) for point in points) == 2)
check('span-metrics-duration-sum', sum(float(point['sum']) for point in points) == 20)
print(f'{len(checks)} checks passed')
