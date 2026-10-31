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
import uuid

IMAGE = 'apache/kafka@sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40'
ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument('--collector', required=True)
parser.add_argument('--evidence', required=True)
parser.add_argument('--docker-context', default='colima')
args = parser.parse_args()
binary = str(Path(args.collector).resolve())
assert subprocess.check_output([binary, '--version'], text=True).strip() == 'otelcol-contrib version 0.163.1'
evidence = Path(args.evidence).resolve()
evidence.mkdir(parents=True, exist_ok=False)
checks = []
docker = ['docker', '--context', args.docker_context]
name = 'collector-lesson-kafka-' + uuid.uuid4().hex[:10]

def check(label, condition):
    assert condition, label
    checks.append(label)
    (evidence / 'results.data').write_text(json.dumps({'collector': '0.163.1', 'kafka': '4.1.1', 'image': IMAGE, 'passed': len(checks), 'checks': checks}, indent=2) + '\n')

def run(command, timeout=30):
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

def retain(label, text, work):
    text = text.replace(str(work), '<WORK>').replace(name, '<BROKER>')
    text = re.sub(r'"service.instance.id": "[^"]+"', '"service.instance.id": "<INSTANCE>"', text)
    text = re.sub(r'^\d{4}-\d\d-\d\dT\S+', '<TIME>', text, flags=re.MULTILINE)
    (evidence / (label + '.txt')).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip() + '\n')

with tempfile.TemporaryDirectory(prefix='kafka-lesson-') as directory:
    work = Path(directory)
    broker_port, entry_port = port(), port()
    processes = []
    handles = []
    started_broker = False
    env = dict(os.environ, LESSON_PORT=str(entry_port), KAFKA_PORT=str(broker_port), LESSON_WORK=str(work))
    try:
        image = run(docker + ['image', 'inspect', IMAGE, '--format', '{{.Architecture}} {{json .RepoDigests}}'])
        engine = run(docker + ['info', '--format', '{{.Architecture}}'])
        architecture = {'x86_64': 'amd64', 'amd64': 'amd64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(engine.stdout.strip())
        check('pinned-image-present', engine.returncode == 0 and architecture is not None and image.returncode == 0 and image.stdout.startswith(architecture + ' ') and IMAGE in image.stdout)
        retain('image-identity', image.stdout, work)
        launch = docker + ['run', '--detach', '--rm', '--name', name, '--cpus=2', '--memory=1536m', '--memory-swap=1536m', '--pids-limit=256', '-p', f'127.0.0.1:{broker_port}:9092']
        settings = {
            'KAFKA_NODE_ID': '1',
            'KAFKA_PROCESS_ROLES': 'broker,controller',
            'KAFKA_LISTENERS': 'INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093',
            'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://localhost:19092,EXTERNAL://127.0.0.1:{broker_port}',
            'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
            'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
            'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
            'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9093',
            'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
            'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
            'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
            'KAFKA_HEAP_OPTS': '-Xms256m -Xmx512m',
        }
        for key, value in settings.items():
            launch += ['-e', key + '=' + value]
        launch += [IMAGE]
        started = run(launch)
        assert started.returncode == 0, started.stderr
        started_broker = True
        deadline = time.monotonic() + 60
        while True:
            ready = run(docker + ['exec', name, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:19092', '--list'], timeout=10)
            if ready.returncode == 0:
                break
            assert time.monotonic() < deadline, ready.stderr
            time.sleep(.5)
        check('actual-broker-ready', True)
        version = run(docker + ['exec', name, '/opt/kafka/bin/kafka-topics.sh', '--version'])
        retain('broker-version', version.stdout + version.stderr, work)
        check('broker-version', version.returncode == 0 and '4.1.1' in version.stdout)
        topic = run(docker + ['exec', name, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:19092', '--create', '--topic', 'lesson-traces', '--partitions', '1', '--replication-factor', '1'])
        check('topic-created', topic.returncode == 0)
        describe = run(docker + ['exec', name, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:19092', '--describe', '--topic', 'lesson-traces'])
        retain('topic', describe.stdout + describe.stderr, work)
        check('single-replica-topic', describe.returncode == 0 and 'ReplicationFactor: 1' in describe.stdout)
        for role in ['producer', 'consumer']:
            config = ROOT / 'labs' / (role + '.yaml')
            valid = subprocess.run([binary, 'validate', '--config', str(config)], env=env, capture_output=True, text=True, timeout=15)
            retain(role + '-validate', valid.stdout + valid.stderr, work)
            check(role + '-valid', valid.returncode == 0)
            handle = (work / (role + '.log')).open('w')
            handles.append(handle)
            process = subprocess.Popen([binary, '--config', str(config)], env=env, stdout=handle, stderr=handle)
            processes.append((role, process))
            deadline = time.monotonic() + 20
            while 'Everything is ready' not in (work / (role + '.log')).read_text():
                assert process.poll() is None, (work / (role + '.log')).read_text()
                assert time.monotonic() < deadline, 'Collector startup timeout'
                time.sleep(.05)
            if role == 'producer':
                request = urllib.request.Request(f'http://127.0.0.1:{entry_port}/v1/traces', data=(ROOT / 'labs/input.data').read_bytes(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=10) as response:
                    body = response.read().decode()
                    check('produce-accepted', response.status == 200 and not json.loads(body).get('partialSuccess'))
                    (evidence / 'producer-response.data').write_text(json.dumps({'status': response.status, 'body': body}, indent=2) + '\n')
                process.send_signal(signal.SIGTERM)
                check('producer-stopped-before-consumer', process.wait(timeout=10) == 0)
                offsets = run(docker + ['exec', name, '/opt/kafka/bin/kafka-get-offsets.sh', '--bootstrap-server', 'localhost:19092', '--topic', 'lesson-traces'])
                retain('broker-offset-before-consumer', offsets.stdout + offsets.stderr, work)
                check('broker-holds-backlog', offsets.returncode == 0 and any(int(line.rsplit(':', 1)[1]) > 0 for line in offsets.stdout.splitlines() if line.startswith('lesson-traces:')))
                check('no-consumer-yet', len(processes) == 1)
            else:
                deadline = time.monotonic() + 20
                while True:
                    try:
                        rows = [json.loads(line) for line in (work / 'consumed.jsonl').read_text().splitlines() if line]
                        spans = [s for row in rows for r in row.get('resourceSpans', []) for scope in r['scopeSpans'] for s in scope['spans']]
                        if len(spans) == 2:
                            break
                    except (FileNotFoundError, json.JSONDecodeError):
                        pass
                    assert time.monotonic() < deadline, 'Kafka consumer export timeout'
                    time.sleep(.1)
                check('backlog-span-names', sorted(s['name'] for s in spans) == ['checkout-error', 'checkout-success'])
                check('backlog-statuses', sorted(s['status']['code'] for s in spans) == [1, 2])
                check('producer-still-stopped', processes[0][1].poll() == 0)
                process.send_signal(signal.SIGTERM)
                check('consumer-clean-stop', process.wait(timeout=10) == 0)
                (evidence / 'consumed.jsonl').write_bytes((work / 'consumed.jsonl').read_bytes())
    finally:
        for role, process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
            retain(role + '-collector', (work / (role + '.log')).read_text(), work)
        for handle in handles:
            handle.close()
        if started_broker:
            broker_log = run(docker + ['logs', name])
            retain('broker', broker_log.stdout + broker_log.stderr, work)
            removed = run(docker + ['rm', '--force', name])
            check('owned-broker-cleaned', removed.returncode == 0)
            check('owned-broker-absent', run(docker + ['inspect', name]).returncode != 0)
print(f'{len(checks)} checks passed')
