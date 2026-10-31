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

for name, incoming, kept in [('keep-all', 2, 2), ('keep-none', 2, 0), ('count-after-drop', 0, 0)]:
    result = execute(name)
    verify_trace_fixture(name + '-before', result_spans(result, 'input'), incoming)
    verify_trace_fixture(name + '-after', result_spans(result), kept)
    values = result_metrics(result)
    check(name + '-incoming-span-metric', total(values, 'lesson.spans.incoming') == incoming)
    check(name + '-kept-span-metric', total(values, 'lesson.spans.kept') == kept)
print(f'{len(checks)} checks passed')
