import argparse
import json
import os
from pathlib import Path
import re
import secrets
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True, type=Path)
parser.add_argument('--case', choices=['baseline', 'security', 'outage'], default='baseline')
parser.add_argument('--evidence', required=True, type=Path)
args = parser.parse_args()
BINARY = str(args.collector.resolve())
assert subprocess.check_output([BINARY, '--version'], text=True).strip() == 'otelcol-contrib version 0.163.1'
args.evidence.mkdir(parents=True, exist_ok=False)
checks = []


def check(name, condition, observed):
    assert condition, (name, observed)
    checks.append({'id': name, 'status': 'pass', 'observed': observed})
    (args.evidence / 'results.data').write_text(json.dumps({'collector': '0.163.1', 'case': args.case, 'checks': checks}, indent=2) + '\n')


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def records(items, signal):
    outer, group, key = {'traces': ('resourceSpans', 'scopeSpans', 'spans'), 'metrics': ('resourceMetrics', 'scopeMetrics', 'metrics'), 'logs': ('resourceLogs', 'scopeLogs', 'logRecords')}[signal]
    return [record for row in items for resource in row.get(outer, []) for scope in resource[group] for record in scope[key]]


def attributes(item):
    return {a['key']: next(iter(a['value'].values())) for a in item.get('attributes', [])}


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line] if path.exists() else []


with tempfile.TemporaryDirectory(prefix='collector-capstone-') as directory:
    work = Path(directory)
    ports = {key: port() for key in ['INGRESS_A', 'INGRESS_B', 'SINK_A', 'SINK_B', 'METRICS', 'HEALTH']}
    tokens = {key: secrets.token_urlsafe(32) for key in ['INGRESS_A_TOKEN', 'INGRESS_B_TOKEN', 'DESTINATION_A_TOKEN', 'DESTINATION_B_TOKEN']}
    env = dict(os.environ, **tokens)
    owned = []
    source_requests = []
    for tenant in ['a', 'b']:
        (work / ('storage-' + tenant)).mkdir()

    def clean(text):
        text = text.replace(str(work), '<WORK>')
        for value in tokens.values():
            text = text.replace(value, '<RUNTIME-TOKEN>')
        text = re.sub(r'127\.0\.0\.1:\d+', '127.0.0.1:<PORT>', text)
        text = re.sub(r'"ts":"[^"]+"', '"ts":"<TIME>"', text)
        text = re.sub(r'"service.instance.id"\s*:\s*"[^"]+"', '"service.instance.id":"<INSTANCE>"', text)
        text = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', text, flags=re.MULTILINE)
        return text

    def certificate(name, ca=None):
        command = ['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(work / (name + '.key')), '-subj', '/CN=' + name]
        if ca is None:
            command += ['-x509', '-days', '1', '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign', '-out', str(work / (name + '.crt'))]
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.run(command + ['-out', str(work / (name + '.csr'))], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            extension = work / 'server.ext'
            extension.write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n')
            subprocess.run(['openssl', 'x509', '-req', '-in', str(work / (name + '.csr')), '-CA', str(work / (ca + '.crt')), '-CAkey', str(work / (ca + '.key')), '-CAcreateserial', '-days', '1', '-extfile', str(extension), '-out', str(work / (name + '.crt'))], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def start(label, template):
        text = (ROOT / template).read_text().replace('@WORK@', str(work))
        for key, value in ports.items():
            if key == 'METRICS':
                text = text.replace('"@METRICS@"', str(value))
            else:
                text = text.replace('@' + key + '@', str(value))
        config = work / (label + '.yaml')
        config.write_text(text)
        result = subprocess.run([BINARY, 'validate', '--config', str(config)], env=env, capture_output=True, text=True, timeout=15)
        (args.evidence / (label + '-validate.txt')).write_text(clean(result.stdout + result.stderr))
        check(label + '-valid', result.returncode == 0, clean(result.stderr))
        handle = (work / (label + '.log')).open('w')
        process = subprocess.Popen([BINARY, '--config', str(config)], env=env, stdout=handle, stderr=handle)
        unit = {'label': label, 'process': process, 'handle': handle, 'expected_exit': 0}
        owned.append(unit)
        for _ in range(300):
            assert process.poll() is None, (label, clean((work / (label + '.log')).read_text()))
            if 'Everything is ready' in (work / (label + '.log')).read_text():
                return unit
            time.sleep(.03)
        raise TimeoutError(label + ' did not become ready')

    def metrics():
        with urllib.request.urlopen('http://127.0.0.1:' + str(ports['METRICS']) + '/metrics', timeout=3) as response:
            return response.read().decode()

    def send(tenant, signal, token=None, context=None, case=None):
        source_requests.append({'tenant': tenant, 'signal': signal, 'case': case or tenant + '-permitted'})
        data = json.loads((ROOT / (tenant + '-' + signal + '.data')).read_text())
        outer = {'traces': 'resourceSpans', 'metrics': 'resourceMetrics', 'logs': 'resourceLogs'}[signal]
        data[outer][0]['resource']['attributes'].append({'key': 'lesson.request', 'value': {'stringValue': case or tenant + '-permitted'}})
        header = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + (token or tokens['INGRESS_' + tenant.upper() + '_TOKEN']), 'X-Tenant': 'b'}
        request = urllib.request.Request('https://127.0.0.1:' + str(ports['INGRESS_' + tenant.upper()]) + '/v1/' + signal, data=json.dumps(data).encode(), headers=header)
        try:
            with urllib.request.urlopen(request, context=context or trusted, timeout=5) as response:
                return response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            return error.code, error.read().decode()
        except urllib.error.URLError as error:
            if isinstance(error.reason, ssl.SSLCertVerificationError):
                return 'tls-rejected', str(error.reason)
            raise

    def wait_output(tenant):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                data = rows(work / (tenant + '.jsonl'))
                if [len(records(data, signal)) for signal in ['traces', 'metrics', 'logs']] == [6, 1, 2]:
                    return data
            except json.JSONDecodeError:
                pass
            time.sleep(.05)
        raise TimeoutError('Expected retained values not observed for tenant ' + tenant)

    def verify_output(tenant):
        data = rows(work / (tenant + '.jsonl'))
        check(tenant + '-signal-populations', [len(records(data, s)) for s in ['traces', 'metrics', 'logs']] == [6, 1, 2], 'six retained spans, one gauge metric, two retained logs')
        for signal, outer in [('traces', 'resourceSpans'), ('metrics', 'resourceMetrics'), ('logs', 'resourceLogs')]:
            resources = [r for row in data for r in row.get(outer, [])]
            check(tenant + '-' + signal + '-authority', bool(resources) and all(attributes(r['resource']).get('tenant.id') == tenant and attributes(r['resource']).get('service.name') == 'checkout-' + tenant and attributes(r['resource']).get('lesson.request') == tenant + '-permitted' for r in resources), 'assigned tenant/source/request membership matches intended authenticated ingress')
        trace_ids = sorted(set(s['traceId'] for s in records(data, 'traces')))
        offset = 100 if tenant == 'a' else 200
        observed_spans = records(data, 'traces')
        original = {span['spanId']: span for span in records([json.loads((ROOT / (tenant + '-traces.data')).read_text())], 'traces')}
        identity = ['traceId', 'spanId', 'parentSpanId', 'name', 'kind', 'startTimeUnixNano', 'endTimeUnixNano', 'status']
        preserved = all(span['spanId'] in original and all(span.get(key) == original[span['spanId']].get(key) for key in identity) and attributes(span) == {key: value for key, value in attributes(original[span['spanId']]).items() if key != 'synthetic.token'} for span in observed_spans)
        check(tenant + '-trace-value', trace_ids == [f'{offset+n:032x}' for n in [2, 3, 4]] and all(sum(span['traceId'] == trace for span in observed_spans) == 2 for trace in trace_ids) and preserved, {'kept_trace_ids': trace_ids, 'original_pair_identities_timing_outcomes_and_teaching_attributes': 'preserved'})
        metrics_data = records(data, 'metrics')
        check(tenant + '-metric-value', metrics_data[0]['name'] == 'lesson.queue.depth' and metrics_data[0]['unit'] == '{item}' and metrics_data[0]['gauge']['dataPoints'][0]['asInt'] == '7', 'original gauge name/unit/value preserved')
        logs = records(data, 'logs')
        check(tenant + '-log-value', sorted(l['body']['stringValue'] for l in logs) == ['order created', 'payment failed token=[REDACTED] reason=declined'] and sorted(l['severityNumber'] for l in logs) == [9, 17], 'routine heartbeat removed; error reason/severity and useful order log retained')
        serialized = json.dumps(data)
        check(tenant + '-known-raw-values-absent', 'synthetic-token-not-a-secret' not in serialized and 'nobody@example.invalid' not in serialized, 'known synthetic token/email absent across all retained signals')
        for outer, group in [('resourceSpans', 'scopeSpans'), ('resourceMetrics', 'scopeMetrics'), ('resourceLogs', 'scopeLogs')]:
            check(tenant + '-' + outer + '-scope', all(s['scope'] == {'name': 'telemetrydrops.capstone', 'version': '1.0.0'} for row in data for r in row.get(outer, []) for s in r[group]), 'instrumentation scope preserved')

    try:
        certificate('ca')
        certificate('server', 'ca')
        certificate('wrong-ca')
        trusted = ssl.create_default_context(cafile=str(work / 'ca.crt'))
        untrusted = ssl.create_default_context(cafile=str(work / 'wrong-ca.crt'))
        if args.case != 'outage':
            start('sink-a', 'sink-a.yaml')
        start('sink-b', 'sink-b.yaml')
        gateway = start('gateway-first', 'gateway.yaml')
        if args.case == 'security':
            status, body = send('b', 'traces', token=tokens['INGRESS_A_TOKEN'], case='wrong-ingress')
            check('cross-ingress-token-rejected', status == 401, {'http_status': status})
            status, body = send('a', 'traces', context=untrusted, case='wrong-server-trust')
            check('untrusted-server-rejected', status == 'tls-rejected', {'result': status})
        for tenant in ['a', 'b']:
            for signal in ['traces', 'metrics', 'logs']:
                status, body = send(tenant, signal)
                check(tenant + '-' + signal + '-accepted', status == 200 and not json.loads(body).get('partialSuccess'), {'http_status': status})
        wait_output('b')
        if args.case == 'outage':
            check('a-unavailable-before-recovery', not rows(work / 'a.jsonl'), 'no A destination exists yet')
            check('b-progress-during-a-outage', len(records(rows(work / 'b.jsonl'), 'traces')) == 6, 'exercised B destination retains its intended values while A is unavailable')
            deadline = time.monotonic() + 8
            while True:
                text = metrics()
                pending = [line for line in text.splitlines() if line.startswith('otelcol_exporter_queue_size{') and 'exporter="otlp_http/a"' in line and float(line.rsplit(' ', 1)[1]) > 0]
                if len(pending) == 3:
                    break
                assert time.monotonic() < deadline, text
                time.sleep(.05)
            (args.evidence / 'outage-metrics.txt').write_text(clean(text))
            with urllib.request.urlopen('http://127.0.0.1:' + str(ports['HEALTH']) + '/', timeout=3) as response:
                check('health-ready-during-outage', response.status == 200, 'default legacy health remains ready while A output is unavailable')
            check('a-three-signal-backlog', len(pending) == 3, 'persistent queued work observed for traces, metrics and logs')
            (work / 'incoming-before-restart.jsonl').write_bytes((work / 'incoming.jsonl').read_bytes())
            gateway['expected_exit'] = -9
            gateway['process'].kill()
            check('gateway-actual-kill', gateway['process'].wait(timeout=8) == -9, 'actual SIGKILL before A destination recovery')
            check('queue-files-preserved', any((work / 'storage-a').iterdir()), 'same writable file-storage directory retained across process lifetimes')
            start('sink-a', 'sink-a.yaml')
            start('gateway-restarted', 'gateway.yaml')
        wait_output('a')
        verify_output('a')
        verify_output('b')
        incoming = rows(work / ('incoming-before-restart.jsonl' if args.case == 'outage' else 'incoming.jsonl'))
        by_source = {}
        for row in incoming:
            for r in row.get('resourceMetrics', []):
                service = attributes(r['resource']).get('service.name')
                by_source[service] = by_source.get(service, 0) + sum(int(p['asInt']) for m in records([{'resourceMetrics': [r]}], 'metrics') if m['name'] == 'trace.span.count' for p in m['sum']['dataPoints'])
        check('upstream-denominator', by_source == {'checkout-a': 8, 'checkout-b': 8}, 'eight incoming spans counted per source before tail decision; six retained')
        if args.case == 'outage':
            restarted_metrics = metrics()
            (args.evidence / 'post-restart-metrics.txt').write_text(clean(restarted_metrics))
            accepted = [float(line.rsplit(' ', 1)[1]) for line in restarted_metrics.splitlines() if line.startswith('otelcol_receiver_accepted_spans{')]
            check('no-source-resend-after-restart', len(source_requests) == 6 and not any(value > 0 for value in accepted), 'six original source requests only; restarted gateway has no positive received-span count while stored output recovers')
        (args.evidence / 'final-metrics.txt').write_text(clean(metrics()))
    finally:
        stop_results = []
        for unit in reversed(owned):
            process = unit['process']
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            unit['handle'].flush()
            (args.evidence / (unit['label'] + '-collector.txt')).write_text(clean((work / (unit['label'] + '.log')).read_text()))
            unit['handle'].close()
            stop_results.append((unit['label'], process.returncode, unit['expected_exit']))
        for output in work.glob('*.jsonl'):
            (args.evidence / output.name).write_bytes(output.read_bytes())
        (args.evidence / 'source-requests.data').write_text(json.dumps(source_requests, indent=2) + '\n')
        for config in work.glob('*.yaml'):
            (args.evidence / (config.stem + '-effective.yaml')).write_text(clean(config.read_text()))
        for label, actual, expected in stop_results:
            check(label + '-expected-stop', actual == expected, {'exit': actual})
print(json.dumps({'case': args.case, 'passed': len(checks)}))
