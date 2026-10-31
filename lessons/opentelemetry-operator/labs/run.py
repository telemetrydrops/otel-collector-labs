"""Run bounded fixtures only against an explicitly selected disposable k3d cluster."""
import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid

ROOT=Path(__file__).resolve().parent
IMAGE='otel/opentelemetry-collector-contrib:0.163.1'
READER='alpine:3.24.2'


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--kubeconfig',required=True,type=Path)
    parser.add_argument('--context',required=True)
    parser.add_argument('--evidence',required=True,type=Path)
    args=parser.parse_args()
    if not args.context.startswith('k3d-dropzone-collector-'):
        parser.error('Use a dedicated disposable k3d-dropzone-collector-* context, never an existing shared cluster')
    args.evidence.mkdir(parents=True,exist_ok=False)
    namespace='collector-lesson-'+uuid.uuid4().hex[:10]
    checks=[]
    created=[]

    def kubectl(*command,data=None,okay=True):
        result=subprocess.run(['kubectl','--kubeconfig',str(args.kubeconfig.resolve()),'--context',args.context,'--request-timeout=25s',*command],input=json.dumps(data) if data is not None else None,capture_output=True,text=True,timeout=100)
        if okay and result.returncode:
            raise AssertionError((command,result.stdout,result.stderr))
        return result

    def check(name,condition,observed):
        if not condition:raise AssertionError((name,observed))
        checks.append({'id':name,'status':'pass','observed':observed})

    def apply(obj):
        obj=copy.deepcopy(obj)
        if obj['kind'] not in ['Namespace','ClusterRole','ClusterRoleBinding']:
            obj.setdefault('metadata',{})['namespace']=namespace
        kubectl('apply','-f','-',data=obj)
        label=obj['metadata']['name']
        created.append(obj)
        return label

    def get(kind,name=None):
        command=['-n',namespace,'get',kind]+([name] if name else [])+['-o','json']
        return json.loads(kubectl(*command).stdout)

    def wait(kind,name):
        if kind=='pod':kubectl('-n',namespace,'wait','pod/'+name,'--for=condition=Ready','--timeout=75s')
        else:kubectl('-n',namespace,'rollout','status',kind+'/'+name,'--timeout=75s')

    def configuration(name,config):
        return apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name},'data':{'collector.yaml':config,'input.data':(ROOT/'input.data').read_text()}})

    def spec(config,agent=False):
        return {'terminationGracePeriodSeconds':2,'serviceAccountName':'collector','securityContext':{'fsGroup':10001},'containers':[
            {'name':'collector','image':IMAGE,'args':['--config=/conf/collector.yaml'], 'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'cpu':'500m','memory':'256Mi'}},'securityContext':{'runAsUser':0 if agent else 10001,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'env':[{'name':'KUBE_NODE_NAME','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}}], 'volumeMounts':[{'name':'config','mountPath':'/conf','readOnly':True},{'name':'output','mountPath':'/data'}]+([{'name':'podlogs','mountPath':'/var/log/pods','readOnly':True}] if agent else [])},
            {'name':'reader','image':READER,'command':['sh','-c','sleep 3600'],'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'cpu':'100m','memory':'32Mi'}},'volumeMounts':[{'name':'output','mountPath':'/data','readOnly':True},{'name':'config','mountPath':'/fixture','readOnly':True}]}],
            'volumes':[{'name':'config','configMap':{'name':config}},{'name':'output','emptyDir':{}}]+([{'name':'podlogs','hostPath':{'path':'/var/log/pods','type':'Directory'}}] if agent else [])}

    def workload(name,kind,config,agent=False,replicas=1):
        template={'metadata':{'labels':{'app':name}},'spec':spec(config,agent)}
        if kind=='Pod':obj={'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'labels':{'app':name}},'spec':template['spec']}
        else:
            obj={'apiVersion':'apps/v1','kind':kind,'metadata':{'name':name},'spec':{'selector':{'matchLabels':{'app':name}},'template':template}}
            if kind!='DaemonSet':obj['spec']['replicas']=replicas
        apply(obj); wait(kind.lower(),name)
        return [p for p in get('pods')['items'] if p['metadata'].get('labels',{}).get('app')==name]

    def read(pod,filename):
        return kubectl('-n',namespace,'exec',pod,'-c','reader','--','cat','/data/'+filename,okay=False).stdout

    def exported(text,signal):
        return [resource for line in text.splitlines() if line.strip() for resource in json.loads(line).get(signal,[])]

    def send(pod,address):
        return kubectl('-n',namespace,'exec',pod,'-c','reader','--','wget','-qO-','--header=Content-Type:application/json','--post-file=/fixture/input.data',address+'/v1/traces',okay=False)

    def save(name,text):
        text=text.replace(namespace,'<NAMESPACE>')
        text=re.sub(r'collector-lesson-[a-f0-9]+','<NAMESPACE>',text)
        (args.evidence/name).write_text('\n'.join(line.rstrip() for line in text.splitlines()).rstrip()+'\n')

    def base_config(receivers,signal='traces',processors=''):
        return 'receivers:\n'+receivers+'exporters:\n  file:\n    path: /data/output.data\n    flush_interval: 100ms\n'+processors+'service:\n  telemetry:\n    metrics:\n      level: none\n  pipelines:\n    '+signal+':\n      receivers: ['+('file_log' if 'file_log:' in receivers else 'otlp')+']\n'+('      processors: [k8s_attributes]\n' if processors else '')+'      exporters: [file]\n'

    version=json.loads(kubectl('version','-o','json').stdout)
    check('server-version',version['serverVersion']['gitVersion']=='v1.35.5+k3s1',{'server':version['serverVersion']['gitVersion']})
    nodes=json.loads(kubectl('get','nodes','-o','json').stdout)['items']
    check('two-ready-nodes',len(nodes)==2 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes),'Two dedicated k3d teaching nodes are Ready')
    apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':namespace}})
    try:
        apply({'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':'collector'}})
        role={'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRole','metadata':{'name':namespace},'rules':[{'apiGroups':[''],'resources':['nodes','nodes/spec','nodes/stats','namespaces','namespaces/status','pods','pods/status','events','services','replicationcontrollers','replicationcontrollers/status','resourcequotas','persistentvolumes','persistentvolumeclaims','limitranges'],'verbs':['get','list','watch']},{'apiGroups':['apps'],'resources':['daemonsets','deployments','replicasets','statefulsets'],'verbs':['get','list','watch']},{'apiGroups':['batch'],'resources':['jobs','cronjobs'],'verbs':['get','list','watch']},{'apiGroups':['autoscaling'],'resources':['horizontalpodautoscalers'],'verbs':['get','list','watch']},{'apiGroups':['discovery.k8s.io'],'resources':['endpointslices'],'verbs':['get','list','watch']}]}
        apply(role)
        apply({'apiVersion':'rbac.authorization.k8s.io/v1','kind':'ClusterRoleBinding','metadata':{'name':namespace},'subjects':[{'kind':'ServiceAccount','name':'collector','namespace':namespace}],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':namespace}})
        operator=get('namespace','opentelemetry-operator-system')
        check('operator-prerequisite',operator['status']['phase']=='Active','Pinned Operator namespace is installed in the disposable cluster')
        config={'receivers':{'otlp':{'protocols':{'http':{'endpoint':'0.0.0.0:4318'}}}},'exporters':{'debug':{'verbosity':'detailed'}},'service':{'telemetry':{'metrics':{'level':'none'}},'pipelines':{'traces':{'receivers':['otlp'],'exporters':['debug']}}}}
        cr={'apiVersion':'opentelemetry.io/v1beta1','kind':'OpenTelemetryCollector','metadata':{'name':'managed'},'spec':{'mode':'deployment','replicas':1,'image':IMAGE,'resources':{'requests':{'cpu':'50m','memory':'64Mi'},'limits':{'cpu':'500m','memory':'256Mi'}},'config':config}}
        apply(cr)
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            result=kubectl('-n',namespace,'get','deployment','managed-collector','-o','json',okay=False)
            if result.returncode==0:break
            time.sleep(.5)
        wait('deployment','managed-collector')
        deployment=get('deployment','managed-collector')
        check('owner-reference',any(o.get('kind')=='OpenTelemetryCollector' and o.get('name')=='managed' for o in deployment['metadata'].get('ownerReferences',[])),'Generated workload is owned by the Collector custom resource')
        check('explicit-runtime-image',deployment['spec']['template']['spec']['containers'][0]['image']==IMAGE,'Operator management preserves explicitly pinned Contrib0.163.1 image')
        # A tiny sender executes only a finite synthetic OTLP POST through the generated service.
        fixture=configuration('sender','receivers: {}\nexporters: {}\nservice: {}\n')
        sender={'apiVersion':'v1','kind':'Pod','metadata':{'name':'sender'},'spec':{'terminationGracePeriodSeconds':2,'containers':[spec(fixture)['containers'][1]],'volumes':spec(fixture)['volumes']}}
        apply(sender);wait('pod','sender')
        result=send('sender','http://managed-collector:4318')
        check('managed-route-accepts',result.returncode==0,{'response':result.stdout.strip()})
        pods=[p for p in get('pods')['items'] if p['metadata']['name'].startswith('managed-collector-')]
        logs=kubectl('-n',namespace,'logs',pods[0]['metadata']['name']).stdout
        check('managed-original-spans','checkout-success' in logs and 'checkout-error' in logs,'Generated Collector exports both expected named spans')
        save('managed-collector.txt',logs)
        cr['spec']['replicas']=2;apply(cr);wait('deployment','managed-collector')
        check('replicas-reconciled',get('deployment','managed-collector')['status']['readyReplicas']==2,'Custom-resource desired replicas reconcile into ready workloads')
        before=get('opentelemetrycollector','managed')['metadata']['generation']
        cr['spec']['config']['processors']={'resource':{'attributes':[{'key':'lesson.operator','value':'reconciled','action':'upsert'}]}}
        cr['spec']['config']['service']['pipelines']['traces']['processors']=['resource']
        prior_template=json.dumps(get('deployment','managed-collector')['spec']['template'],sort_keys=True)
        apply(cr)
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            observed=get('opentelemetrycollector','managed')
            if observed['metadata']['generation']>before and json.dumps(get('deployment','managed-collector')['spec']['template'],sort_keys=True)!=prior_template:break
            time.sleep(.3)
        wait('deployment','managed-collector')
        check('configuration-generation',observed['metadata']['generation']>before,'Collector custom-resource configuration change creates a new desired generation')
        result=send('sender','http://managed-collector:4318')
        time.sleep(.5)
        pods=[p for p in get('pods')['items'] if p['metadata']['name'].startswith('managed-collector-') and p.get('status',{}).get('phase')=='Running']
        logs='\n'.join(kubectl('-n',namespace,'logs',p['metadata']['name'],okay=False).stdout for p in pods)
        check('changed-data-path','lesson.operator' in logs and 'reconciled' in logs,'Updated active processor is proven in emitted debug output, not generation alone')
        save('reconciled-collector.txt',logs)
    finally:
        # Retain only deliberately generated public fixture manifests, never kubeconfig or SA tokens.
        save('fixture-manifests.data',json.dumps(created,indent=2)+'\n')
        for pod in get('pods').get('items',[]):
            for container in pod['spec'].get('containers',[]):
                if 'opentelemetry-collector' in container.get('image',''):
                    logs=kubectl('-n',namespace,'logs',pod['metadata']['name'],'-c',container['name'],okay=False)
                    save(pod['metadata']['name']+'-collector.txt',logs.stdout+logs.stderr)
        status=kubectl('-n',namespace,'get','pods','-o','wide',okay=False)
        save('pod-status.txt',status.stdout+status.stderr)
        kubectl('delete','namespace',namespace,'--wait=true','--timeout=60s',okay=False)
        kubectl('delete','clusterrolebinding',namespace,'--ignore-not-found',okay=False)
        kubectl('delete','clusterrole',namespace,'--ignore-not-found',okay=False)
    result={'collector_image':IMAGE,'kubernetes':version['serverVersion']['gitVersion'],'lesson':ROOT.parent.name,'checks':checks,'fixture_sha256':hashlib.sha256((ROOT/'input.data').read_bytes()).hexdigest(),'limits':'Disposable two-node cluster only; finite fixtures, no production sizing or general exactly-once guarantee'}
    (args.evidence/'results.data').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'lesson':ROOT.parent.name,'passed':len(checks)}))


if __name__=='__main__':main()
