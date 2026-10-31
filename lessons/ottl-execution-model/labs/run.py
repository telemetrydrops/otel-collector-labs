"""Run this lesson's actual isolated OTTL cases using the exact course binary."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent

def attributes(item):
    return {entry['key']: next(iter(entry['value'].values()), None) for entry in item.get('attributes', [])}

def spans(items):
    return [span for batch in items for resource in batch.get('resourceSpans', []) for scope in resource.get('scopeSpans', []) for span in scope.get('spans', [])]

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
    def check(label, condition, observed):
        if not condition: raise AssertionError((label, observed))
        checks.append({'id': label, 'status': 'pass', 'observed': observed})
    with tempfile.TemporaryDirectory(prefix=ROOT.parent.name+'-') as temporary:
        work = Path(temporary)
        for case in CASES:
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
            endpoint = f'127.0.0.1:{port}'
            output = work / f'{case}.data'
            env = dict(os.environ, LESSON_OTLP_ENDPOINT=endpoint, LESSON_TRACE_FILE=str(output))
            config = ROOT / f'{case}.yaml'
            validate = subprocess.run([str(binary), 'validate', '--config', str(config)], env=env, capture_output=True, text=True)
            (args.evidence/f'{case}-validate.txt').write_text(validate.stdout+validate.stderr)
            invalid = case in INVALID
            check(case+'-validation', (validate.returncode != 0) == invalid, {'exit': validate.returncode})
            with (work/f'{case}.txt').open('w') as log:
                process = subprocess.Popen([str(binary), '--config', str(config)], env=env, stdout=log, stderr=log)
                responses = []
                try:
                    if invalid:
                        check(case+'-startup-rejected', process.wait(timeout=10) != 0, 'Invalid configuration cannot start')
                    else:
                        for _ in range(200):
                            if process.poll() is not None: raise AssertionError((work/f'{case}.txt').read_text())
                            try:
                                with socket.create_connection(('127.0.0.1',port),timeout=.1): break
                            except OSError: time.sleep(.05)
                        else: raise TimeoutError('Loopback receiver not ready')
                        for signal, filename in INPUTS:
                            request = urllib.request.Request(f'http://{endpoint}/v1/{signal}', (ROOT/filename).read_bytes(), {'Content-Type':'application/json'})
                            try:
                                with urllib.request.urlopen(request,timeout=4) as response:
                                    responses.append({'signal':signal,'status':response.status,'body':response.read().decode()})
                            except urllib.error.HTTPError as error:
                                responses.append({'signal':signal,'status':error.code,'body':error.read().decode()})
                        (args.evidence/f'{case}-responses.data').write_text(json.dumps(responses,indent=2)+'\n')
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try: process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill(); process.wait()
                    log.flush()
                    text = (work/f'{case}.txt').read_text().replace(str(work),'<WORK>').replace(str(ROOT),'<LAB>').replace(endpoint,'127.0.0.1:<PORT>')
                    text = re.sub(r'^\d{4}-\d\d-\d\dT\S+','<TIME>',text,flags=re.MULTILINE)
                    text = re.sub(r'"service.instance.id": "[^"]+"','"service.instance.id": "<INSTANCE>"',text)
                    (args.evidence/f'{case}-collector.txt').write_text(text)
                    if output.exists(): (args.evidence/f'{case}-output.data').write_bytes(output.read_bytes())
            if not invalid:
                check(case+'-shutdown',process.returncode==0,{'exit':process.returncode})
                items = [json.loads(line) for line in output.read_text().splitlines()] if output.exists() else []
                verify(case,items,responses,check)
                observed_spans = spans(items)
                if observed_spans:
                    original = {span['spanId']: span for span in spans([json.loads((ROOT/'input.data').read_text())])}
                    identity = ('traceId','spanId','name','kind','startTimeUnixNano','endTimeUnixNano','status')
                    check(case+'-span-identities',all(span['spanId'] in original and all(span.get(key)==original[span['spanId']].get(key) for key in identity) for span in observed_spans),'Every surviving span retains its original trace/span IDs, name, times, kind and operation status')
                    resources = [resource for batch in items for resource in batch.get('resourceSpans',[])]
                    check(case+'-resource-scope',all(attributes(resource['resource']).get('service.name')=='checkout-demo' and all(scope['scope']=={'name':'telemetrydrops.lesson','version':'1.0.0'} for scope in resource['scopeSpans']) for resource in resources),'Surviving traces retain service and instrumentation scope')
    (args.evidence/'results.data').write_text(json.dumps({'collector':version,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'checks':checks},indent=2)+'\n')
    print(json.dumps({'cases':len(CASES),'assertions':len(checks),'collector':version}))

CASES = ['ignore','propagate','wrong-context','unknown-function']
INVALID = ['wrong-context','unknown-function']
INPUTS = [('traces','input.data')]

def verify(case, items, responses, check):
    data = spans(items)
    if case == 'ignore':
        check('ignore-acceptance',responses[0]['status']==200,responses)
        check('ignore-four-spans',len(data)==4,{'count':len(data)})
        check('sequential-and-continues',all(attributes(span).get('lesson.order')=='second' and attributes(span).get('lesson.after') is True for span in data),'Later statements see earlier updates; ignore continues after bad Int conversion')
        check('type-conversion',attributes(data[1]).get('lesson.parsed') is None and attributes(data[0]).get('lesson.parsed')=='7','Invalid quantity has no parsed value; valid quantity becomes int64')
    else:
        check('propagate-rejection',responses[0]['status']!=200,responses)
        check('propagate-no-export',len(data)==0,{'count':len(data)})


if __name__ == "__main__":
    main()
