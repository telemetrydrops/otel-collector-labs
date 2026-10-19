"""Execute the isolated first OTLP pipeline and retain observed evidence."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collector', required=True, type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    args = parser.parse_args()
    collector = args.collector.resolve()
    version = subprocess.check_output([str(collector), '--version'], text=True).strip()
    assert version == 'otelcol-contrib version 0.163.1', version
    args.evidence.mkdir(parents=True, exist_ok=False)
    checks = []

    def check(name, condition, result):
        if not condition:
            raise AssertionError((name, result))
        checks.append({'id': name, 'status': 'pass', 'observed': result})

    with tempfile.TemporaryDirectory(prefix='first-otlp-') as directory:
        work = Path(directory)
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        endpoint = f'127.0.0.1:{port}'
        output = work / 'traces.data'
        env = dict(os.environ, LESSON_OTLP_ENDPOINT=endpoint, LESSON_TRACE_FILE=str(output))
        command = [str(collector), '--config', str(ROOT / 'collector.yaml')]
        validation = subprocess.run([str(collector), 'validate', '--config', str(ROOT / 'collector.yaml')], env=env, capture_output=True, text=True)
        (args.evidence / 'validate.txt').write_text(validation.stdout + validation.stderr)
        check('valid-configuration', validation.returncode == 0, 'Collector validate exit 0')
        with (work / 'collector.log').open('w') as log:
            process = subprocess.Popen(command, env=env, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise AssertionError((work / 'collector.log').read_text())
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise TimeoutError('Receiver did not bind; ephemeral port may have been taken')
                check('empty-before-input', not output.exists() or output.stat().st_size == 0, 'No exported telemetry before input')
                request = urllib.request.Request(f'http://{endpoint}/v1/traces', (ROOT / 'input.data').read_bytes(), {'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=3) as response:
                    body = response.read().decode()
                    status = response.status
                partial = json.loads(body).get('partialSuccess', {})
                (args.evidence / 'response.data').write_text(body + '\n')
                check('accepted-request', status == 200 and not partial.get('rejectedSpans') and not partial.get('errorMessage'), {'http_status': status, 'body': body})
                request = urllib.request.Request(f'http://{endpoint}/v1/traces', b'{', {'Content-Type': 'application/json'})
                try:
                    urllib.request.urlopen(request, timeout=3)
                    raise AssertionError('Malformed request was accepted')
                except urllib.error.HTTPError as error:
                    bad_body = error.read().decode()
                    check('malformed-rejected', error.code == 400, {'http_status': error.code})
                    (args.evidence / 'rejected-response.data').write_text(bad_body + '\n')
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                if output.exists():
                    shutil.copyfile(output, args.evidence / 'exported-traces.data')
                log.flush()
                observed = (work / 'collector.log').read_text().replace(str(work), '<WORK>').replace(str(ROOT), '<LAB>')
                observed = observed.replace(endpoint, '127.0.0.1:<PORT>')
                observed = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', observed, flags=re.MULTILINE)
                observed = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', observed)
                (args.evidence / 'collector.txt').write_text(observed)
        check('clean-shutdown', process.returncode == 0, 'Owned Collector exited 0 after SIGTERM')
        batches = [json.loads(line) for line in output.read_text().splitlines()]
        resources = [resource for batch in batches for resource in batch.get('resourceSpans', [])]
        scopes = [scope for resource in resources for scope in resource.get('scopeSpans', [])]
        spans = [span for scope in scopes for span in scope.get('spans', [])]
        expected = json.loads((ROOT / 'input.data').read_text())['resourceSpans'][0]
        actual_spans = sorted(spans, key=lambda span: span['name'])
        expected_spans = sorted(expected['scopeSpans'][0]['spans'], key=lambda span: span['name'])
        check('two-spans', len(spans) == 2, {'span_count': len(spans), 'names': [span['name'] for span in actual_spans]})
        check('resource-and-scope', all(r['resource'] == expected['resource'] for r in resources) and all(s['scope'] == expected['scopeSpans'][0]['scope'] for s in scopes), 'service.name=checkout-demo; scope=telemetrydrops.lesson/1.0.0')
        keys = ('traceId', 'spanId', 'name', 'kind', 'startTimeUnixNano', 'endTimeUnixNano', 'attributes', 'status')
        check('preserved-span-content', all({k: a.get(k) for k in keys} == {k: e.get(k) for k in keys} for a, e in zip(actual_spans, expected_spans)), 'Both IDs, names, times, kinds, case attributes and OK/ERROR statuses match input')
    result = {'collector': version, 'binary_sha256': hashlib.sha256(collector.read_bytes()).hexdigest(), 'input_sha256': hashlib.sha256((ROOT / 'input.data').read_bytes()).hexdigest(), 'checks': checks}
    (args.evidence / 'results.data').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': len(checks), 'collector': version}))


if __name__ == '__main__':
    main()
