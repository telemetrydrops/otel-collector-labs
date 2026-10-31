import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True)
parser.add_argument('--evidence', required=True)

args = parser.parse_args()
binary = str(Path(args.collector).resolve())
evidence = Path(args.evidence).resolve()
evidence.mkdir(parents=True, exist_ok=True)
assert subprocess.check_output([binary, '--version'], text=True).strip() == 'otelcol-contrib version 0.163.1'
results = []

def retain(name, text):
    (evidence / (name + '.txt')).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip() + '\n')

def check(name, value):
    assert value, name
    results.append(name)

def execute(name, files, expect=None, stage='staging', selected_binary=None, invalid=False, occupy=False, missing_file=False):
    selected_binary = selected_binary or binary
    with tempfile.TemporaryDirectory(prefix='collector-config-') as temporary:
        temp = Path(temporary)
        for source in (root / 'labs').glob('*.yaml'):
            (temp / source.name).write_bytes(source.read_bytes())
        if missing_file:
            (temp / 'processors.yaml').unlink()
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
        if occupy:
            listener.listen()
        else:
            listener.close()
        output = temp / 'output.jsonl'
        env = dict(os.environ, LESSON_PORT=str(port), LESSON_OUTPUT=str(output))
        env.pop('LESSON_STAGE', None)
        if stage is not None:
            env['LESSON_STAGE'] = stage
        command = []
        for file in files:
            command += ['--config', file]
        valid = subprocess.run([selected_binary, 'validate', *command], cwd=temp, env=env, capture_output=True, text=True, timeout=20)
        retain(name + '-validate', valid.stdout + valid.stderr)
        check(name + '-validation', (valid.returncode != 0) == invalid)
        if invalid:
            expected_error = {'core-transform-missing': 'unknown type: \"transform\"', 'missing-file': 'processors.yaml', 'malformed-yaml': 'yaml:', 'unsupported-key': 'invented_option', 'missing-reference': 'otlp/missing'}[name]
            check(name + '-diagnostic', expected_error in valid.stdout + valid.stderr)
            listener.close()
            return
        with (temp / 'collector.log').open('w') as log:
            process = subprocess.Popen([selected_binary, *command], cwd=temp, env=env, stdout=log, stderr=log)
            try:
                if occupy:
                    check(name + '-startup-fails', process.wait(timeout=15) != 0)
                    check(name + '-bind-diagnostic', 'bind' in (temp / 'collector.log').read_text().lower())
                    return
                deadline = time.monotonic() + 15
                while 'Everything is ready' not in (temp / 'collector.log').read_text():
                    assert process.poll() is None, (temp / 'collector.log').read_text()
                    assert time.monotonic() < deadline, 'readiness timeout'
                    time.sleep(.05)
                request = urllib.request.Request(f'http://127.0.0.1:{port}/v1/traces', data=(root / 'labs/input.data').read_bytes(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=5) as response:
                    check(name + '-http', response.status == 200)
                deadline = time.monotonic() + 5
                while not output.exists() or not output.read_text().strip():
                    assert time.monotonic() < deadline, 'export timeout'
                    time.sleep(.05)
                process.send_signal(signal.SIGTERM)
                check(name + '-stop', process.wait(timeout=10) == 0)
                raw = output.read_text()
                rows = [json.loads(line) for line in raw.splitlines()]
                resources = [r for row in rows for r in row['resourceSpans']]
                spans = [s for r in resources for scope in r['scopeSpans'] for s in scope['spans']]
                check(name + '-span-names', sorted(s['name'] for s in spans) == ['checkout-error', 'checkout-success'])
                check(name + '-statuses', sorted(s['status']['code'] for s in spans) == [1, 2])
                check(name + '-case-attributes', sorted(a['value']['stringValue'] for s in spans for a in s['attributes'] if a['key'] == 'lesson.case') == ['error', 'success'])
                attrs = [{a['key']: a['value'].get('stringValue') for a in r['resource']['attributes']} for r in resources]
                check(name + '-service', all(a['service.name'] == 'checkout-demo' for a in attrs))
                if expect:
                    key, value = expect
                    check(name + '-mutation', all(a.get(key) == value for a in attrs))
                (evidence / (name + '-output.jsonl')).write_text(raw)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                listener.close()
                retain(name + '-startup', (temp / 'collector.log').read_text())

execute('explicit-stage', ['base.yaml'], expect=('lesson.stage', 'staging'))
execute('default-stage', ['base.yaml'], stage=None, expect=('lesson.stage', 'local'))
execute('list-replacement', ['base.yaml', 'overlay.yaml'], expect=('lesson.stage', None))
execute('missing-file', ['base.yaml'], invalid=True, missing_file=True)
(evidence / 'results.data').write_text(json.dumps({'collector': '0.163.1', 'checks': results, 'passed': len(results)}, indent=2) + '\n')
print(f'{len(results)} checks passed')
