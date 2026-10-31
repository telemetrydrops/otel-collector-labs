import argparse
import json
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True)
parser.add_argument('--evidence', required=True)
args = parser.parse_args()
binary = str(Path(args.collector).resolve())
evidence = Path(args.evidence).resolve()
evidence.mkdir(parents=True, exist_ok=True)
root = Path(__file__).resolve().parents[1]
version = subprocess.check_output([binary, '--version'], text=True).strip()
assert version == 'otelcol-contrib version 0.163.1', version
results = []


def read_export(path):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line]
    resources = [r for row in rows for r in row['resourceSpans']]
    spans = [s for r in resources for scope in r['scopeSpans'] for s in scope['spans']]
    assert sorted(s['name'] for s in spans) == ['checkout-error', 'checkout-success']
    assert sorted(s['status']['code'] for s in spans) == [1, 2]
    assert sorted(a['value']['stringValue'] for s in spans for a in s['attributes'] if a['key'] == 'lesson.case') == ['error', 'success']
    attrs = [{a['key']: a['value']['stringValue'] for a in r['resource']['attributes']} for r in resources]
    assert all(a['service.name'] == 'checkout-demo' for a in attrs)
    return attrs


for name in ['inactive', 'ordered', 'reversed', 'fanout', 'branches', 'missing-reference']:
    with tempfile.TemporaryDirectory(prefix='enabled-pipelines-') as temp:
        temp = Path(temp)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        config = (root / 'labs' / (name + '.yaml')).read_text().replace('@PORT@', str(port)).replace('@OUTPUT@', str(temp))
        path = temp / 'config.yaml'
        path.write_text(config)
        valid = subprocess.run([binary, 'validate', '--config', str(path)], capture_output=True, text=True)
        if name == 'missing-reference':
            assert valid.returncode != 0
            lines = [line for line in (valid.stdout + valid.stderr).splitlines() if 'resource/missing' in line]
            assert lines
            (evidence / 'missing-reference.txt').write_text('\n'.join(lines) + '\n')
            results.append({'case': name, 'result': 'pass', 'proof': 'undefined processor reference rejected'})
            continue
        assert valid.returncode == 0, valid.stderr
        with (temp / 'collector.log').open('w') as log:
            process = subprocess.Popen([binary, '--config', str(path)], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 10
                while True:
                    if process.poll() is not None:
                        raise RuntimeError((temp / 'collector.log').read_text())
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.1):
                            break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(.05)
                request = urllib.request.Request(f'http://127.0.0.1:{port}/v1/traces', data=(root / 'labs/input.data').read_bytes(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=5) as response:
                    assert response.status == 200
                process.send_signal(signal.SIGTERM)
                assert process.wait(timeout=10) == 0
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
        files = sorted(temp.glob('*.jsonl'))
        assert [f.name for f in files] == (['audit.jsonl', 'primary.jsonl'] if name in ['fanout', 'branches'] else ['primary.jsonl'])
        for file in files:
            attrs = read_export(file)
            expected = {'ordered': 'second', 'reversed': 'first', 'fanout': 'second', 'branches': 'first' if file.name == 'audit.jsonl' else 'second'}.get(name)
            assert all(a.get('lesson.stage') == expected for a in attrs), attrs
            (evidence / (name + '-' + file.name)).write_bytes(file.read_bytes())
        if name == 'fanout':
            assert json.loads(files[0].read_text()) == json.loads(files[1].read_text())
        results.append({'case': name, 'result': 'pass', 'exports': [f.name for f in files]})
(evidence / 'results.data').write_text(json.dumps({'version': version, 'results': results}, indent=2) + '\n')
print(json.dumps({'version': version, 'passing_cases': len(results)}))
