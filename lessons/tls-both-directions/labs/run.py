"""Isolated version-pinned security lab; never retain keys or bearer values."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import ssl
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
    binary = args.collector.resolve()
    version = subprocess.check_output([str(binary), '--version'], text=True).strip()
    assert version == 'otelcol-contrib version 0.163.1', version
    args.evidence.mkdir(parents=True, exist_ok=False)
    checks = []
    processes = []
    handles = []
    sensitive = []
    lesson = ROOT.parent.name

    def check(name, condition, observed):
        if not condition:
            raise AssertionError((name, observed))
        checks.append({'id': name, 'status': 'pass', 'observed': observed})
    with tempfile.TemporaryDirectory(prefix='collector-security-') as directory:
        work = Path(directory)
        ports = []

        def port():
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                value = sock.getsockname()[1]
            ports.append(value)
            return value

        def clean(text):
            text = text.replace(str(work), '<WORK>').replace(str(ROOT), '<LAB>')
            for secret in sensitive:
                text = text.replace(secret, '<RUNTIME-TOKEN>')
            for value in ports:
                text = text.replace(':' + str(value), ':<PORT>')
            text = re.sub('127\\.0\\.0\\.1:\\d+', '127.0.0.1:<PORT>', text)
            text = re.sub('^\\d{4}-\\d\\d-\\d\\dT\\S+', '<TIME>', text, flags=re.MULTILINE)
            text = re.sub('"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', text)
            return '\n'.join((line.rstrip() for line in text.splitlines())) + '\n'

        def start(name, template, environment=None):
            cfg = work / (name + '.yaml')
            cfg.write_text(template)
            env = dict(os.environ, **environment or {})
            validated = subprocess.run([str(binary), 'validate', '--config', str(cfg)], env=env, capture_output=True, text=True)
            (args.evidence / (name + '-validate.txt')).write_text(clean(validated.stdout + validated.stderr))
            check(name + '-validates', validated.returncode == 0, 'validate exit 0')
            log = (work / (name + '.txt')).open('w')
            handles.append(log)
            process = subprocess.Popen([str(binary), '--config', str(cfg)], env=env, stdout=log, stderr=log)
            processes.append((name, process))
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                log.flush()
                if process.poll() is not None:
                    raise AssertionError((name, (work / (name + '.txt')).read_text()))
                if 'Everything is ready' in (work / (name + '.txt')).read_text():
                    return process
                time.sleep(0.03)
            raise TimeoutError(name + ' did not become ready')

        def stop(name, process):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            check(name + '-shutdown', process.returncode == 0, 'SIGTERM exit 0')

        def certificate(name, ca=None, san=None):
            key = work / (name + '.key')
            crt = work / (name + '.crt')
            command = ['openssl', 'req', '-new', '-newkey', 'rsa:2048', '-nodes', '-keyout', str(key), '-subj', '/CN=' + name]
            if ca is None:
                command += ['-x509', '-days', '1', '-addext', 'basicConstraints=critical,CA:TRUE', '-addext', 'keyUsage=critical,keyCertSign,cRLSign', '-out', str(crt)]
                subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                csr = work / (name + '.csr')
                subprocess.run(command + ['-out', str(csr)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                ext = work / (name + '.ext')
                ext.write_text('subjectAltName=' + (san or 'DNS:' + name) + '\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n')
                subprocess.run(['openssl', 'x509', '-req', '-in', str(csr), '-CA', str(work / (ca + '.crt')), '-CAkey', str(work / (ca + '.key')), '-CAcreateserial', '-days', '1', '-extfile', str(ext), '-out', str(crt)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return (crt, key)

        def payload(case, tenant=None):
            data = json.loads((ROOT / 'input.data').read_text())
            resource = data['resourceSpans'][0]['resource']['attributes']
            resource.append({'key': 'lesson.case', 'value': {'stringValue': case}})
            if tenant is not None:
                resource.append({'key': 'tenant.id', 'value': {'stringValue': tenant}})
            return data

        def send(endpoint, data, context=None, headers=None):
            request = urllib.request.Request(endpoint + '/v1/traces', json.dumps(data).encode(), {'Content-Type': 'application/json', **(headers or {})})
            try:
                with urllib.request.urlopen(request, context=context, timeout=4) as response:
                    return (response.status, response.read().decode())
            except urllib.error.HTTPError as error:
                return (error.code, error.read().decode())
            except (urllib.error.URLError, ssl.SSLError) as error:
                return ('tls-rejected', str(error))

        def traces(path):
            if not path.exists():
                return []
            return [r for line in path.read_text().splitlines() for r in json.loads(line).get('resourceSpans', [])]

        def span_count(records):
            return sum((len(s.get('spans', [])) for r in records for s in r.get('scopeSpans', [])))

        def assert_payload(name, path, cases, tenant=None):
            records = traces(path)
            check(name + '-count', span_count(records) == 2 * len(cases), {'spans': span_count(records)})
            attrs = [{a['key']: a['value'].get('stringValue') for a in r['resource']['attributes']} for r in records]
            check(name + '-cases', sorted((a['lesson.case'] for a in attrs)) == sorted(cases), {'cases': [a['lesson.case'] for a in attrs]})
            if tenant is not None:
                check(name + '-tenant', all((a.get('tenant.id') == tenant for a in attrs)), {'assigned_tenant': tenant})
            expected = json.loads((ROOT / 'input.data').read_text())['resourceSpans'][0]['scopeSpans'][0]
            check(name + '-content', all((s['scope'] == expected['scope'] and sorted(s['spans'], key=lambda v: v['name']) == sorted(expected['spans'], key=lambda v: v['name']) for r in records for s in r['scopeSpans'])), 'Both complete span records and scope preserved')
            shutil.copyfile(path, args.evidence / (name + '-traces.data'))

        def service(pipelines, extensions='[]'):
            return '\nservice:\n  extensions: ' + extensions + '\n  telemetry:\n    metrics:\n      level: none\n  pipelines:\n' + pipelines

        def receiver(name, endpoint, tls='', auth=''):
            return '  otlp/' + name + ':\n    protocols:\n      http:\n        endpoint: 127.0.0.1:' + str(endpoint) + '\n' + tls + auth

        def server_tls(require_client=False):
            return '        tls:\n          cert_file: ' + str(work / 'server.crt') + '\n          key_file: ' + str(work / 'server.key') + '\n' + ('          client_ca_file: ' + str(work / 'ca.crt') + '\n' if require_client else '')

        def file_exporter(name):
            return '  file/' + name + ':\n    path: ' + str(work / (name + '.data')) + '\n    flush_interval: 100ms\n'

        def single_config(name, endpoint, tls='', auth='', extensions=''):
            return 'receivers:\n' + receiver(name, endpoint, tls, auth) + 'exporters:\n' + file_exporter(name) + extensions + service('    traces:\n      receivers: [otlp/' + name + ']\n      exporters: [file/' + name + ']\n', '[bearertokenauth/server]' if extensions else '[]')
        try:
            certificate('ca')
            certificate('server', 'ca', 'DNS:localhost,IP:127.0.0.1')
            certificate('client', 'ca')
            certificate('wrong-ca')
            context = ssl.create_default_context(cafile=str(work / 'ca.crt'))
            wrong_context = ssl.create_default_context(cafile=str(work / 'wrong-ca.crt'))
            p = port()
            process = start('receiver-tls', single_config('secure', p, server_tls(True)))
            endpoint = 'https://127.0.0.1:' + str(p)
            status, _ = send(endpoint, payload('missing-client'), context)
            check('missing-client-cert-rejected', status == 'tls-rejected', {'result': status})
            context.load_cert_chain(str(work / 'client.crt'), str(work / 'client.key'))
            wrong_context.load_cert_chain(str(work / 'client.crt'), str(work / 'client.key'))
            status, _ = send(endpoint, payload('wrong-trust'), wrong_context)
            check('untrusted-server-rejected', status == 'tls-rejected', {'result': status})
            status, _ = send(endpoint, payload('mutual-tls'), context)
            check('mutual-tls-accepted', status == 200, {'http_status': status, 'response': _})
            try:
                with socket.create_connection(('127.0.0.1', p), timeout=3) as connection:
                    with context.wrap_socket(connection, server_hostname='wrong.example.invalid'):
                        raise AssertionError('Wrong hostname accepted')
            except ssl.SSLCertVerificationError:
                check('hostname-mismatch-rejected', True, 'Trusted CA does not authorize a different server name')
            for name, ca in [('export-good', 'ca'), ('export-wrong-trust', 'wrong-ca')]:
                client_port = port()
                cfg = 'receivers:\n' + receiver('client', client_port) + 'exporters:\n  otlp_http/destination:\n    endpoint: ' + endpoint + '\n    tls:\n      ca_file: ' + str(work / (ca + '.crt')) + '\n      cert_file: ' + str(work / 'client.crt') + '\n      key_file: ' + str(work / 'client.key') + '\n    sending_queue:\n      enabled: false\n    retry_on_failure:\n      enabled: false\n' + service('    traces:\n      receivers: [otlp/client]\n      exporters: [otlp_http/destination]\n')
                client = start(name, cfg)
                status, _ = send('http://127.0.0.1:' + str(client_port), payload(name))
                check(name + '-response', status == 200 if name == 'export-good' else isinstance(status, int) and status >= 400, {'http_status': status})
                stop(name, client)
            stop('receiver-tls', process)
            assert_payload('secure', work / 'secure.data', ['mutual-tls', 'export-good'])
        finally:
            for name, process in processes:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for handle in handles:
                handle.close()
            for name, _ in processes:
                (args.evidence / (name + '-collector.txt')).write_text(clean((work / (name + '.txt')).read_text()))
            for path in work.glob('*.yaml'):
                (args.evidence / (path.stem + '-effective.yaml')).write_text(clean(path.read_text()))
    results = {'collector': version, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'input_sha256': hashlib.sha256((ROOT / 'input.data').read_bytes()).hexdigest(), 'lesson': lesson, 'checks': checks}
    (args.evidence / 'results.data').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps({'lesson': lesson, 'passed': len(checks)}))
if __name__ == '__main__':
    main()
