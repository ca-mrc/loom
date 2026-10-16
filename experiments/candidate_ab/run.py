"""Unsigned native-Actions experiment. Never invokes protected build/sign/deploy.

App inputs are an independent exact-SHA checkout; current and frozen old image
helpers share the same source, builder resources, DB seed, native transfers and
publication validation. No Actions/environment identity is forged.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import uuid

TOOL=Path(__file__).resolve().parents[2]
ROOT=Path(os.environ['PROBE_ROOT'])
SOURCE=Path(os.environ['GITHUB_WORKSPACE'])/'source'
SHA=os.environ['PROBE_SOURCE_SHA']
WORKERS=3
PARTIAL=['service','control_plane','gateway','execution_runtime']
BUILDKIT='docker.io/moby/buildkit:v0.33.0@sha256:6c2fa84a6b61ccd72899dde4239f8d5717f05f9a8ca6f3cad185fb1a95a94de3'


def now():return datetime.now(timezone.utc).isoformat()
def save(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n');temp.replace(path)
def run(*args,timeout=900):
    return subprocess.run(args,check=True,text=True,capture_output=True,timeout=timeout).stdout.strip()
def modules():
    sys.path[:0]=[str(TOOL),str(TOOL/'src')]
    from scripts.ops import nebius_candidate as current
    spec=importlib.util.spec_from_file_location('ab_baseline',Path(__file__).with_name('baseline_candidate.py'))
    old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
    for m in [current,old]:m.ROOT=SOURCE
    return current,old

def context():
    info=json.loads((ROOT/'setup.json').read_text())
    os.sched_setaffinity(0,set(info['cpus']))
    os.environ['TRIVY_SKIP_DB_UPDATE']='true'
    os.environ['BUILDKIT_CLIENT']=str(ROOT/'tools/buildctl')
    os.environ['PATH']=str(ROOT/'tools')+os.pathsep+os.environ['PATH']
    p,old=modules()
    if run('git','-C',str(SOURCE),'rev-parse','HEAD')!=SHA:raise ValueError('wrong pristine application checkout')
    rows=p.release_image_matrix(p.load_manifest(SOURCE/'config/component-ownership.toml'),image_set='nebius')
    return info,p,old,rows

def start_builder(info,output):
    name='loom-candidate-ab-'+uuid.uuid4().hex[:12]
    config=SOURCE/'config/ci-buildkit.toml'
    if not config.is_file():raise ValueError('missing source BuildKit config')
    cmd=['docker','run','-d','--name',name,'--privileged','--cpus','4','--cpuset-cpus',','.join(map(str,info['cpus'])),
         '--memory','9611563008','--memory-swap','9611563008','--mount',f'type=bind,source={config},target=/etc/buildkit/buildkitd.toml,readonly',
         '-p','127.0.0.1::1234',BUILDKIT,'--config','/etc/buildkit/buildkitd.toml','--addr=tcp://0.0.0.0:1234']
    run(*cmd)
    save(output/'builder.json',{'name':name,'command':cmd,'cpus':info['cpus'],'workers':WORKERS})
    run('docker','cp',name+':/usr/bin/buildctl',str(ROOT/'tools/buildctl'))
    (ROOT/'tools/buildctl').chmod(0o700)
    port=run('docker','port',name,'1234/tcp').rsplit(':',1)[1]
    os.environ['BUILDKIT_HOST']='tcp://127.0.0.1:'+port
    for _ in range(60):
        try:run(str(ROOT/'tools/buildctl'),'debug','workers',timeout=10);break
        except subprocess.CalledProcessError:time.sleep(1)
    else:raise RuntimeError('builder did not become ready')
    return name,os.environ['BUILDKIT_HOST']

def stop_builder(name):
    if name:run('docker','rm','-fv',name,timeout=120)

def scans(info,p,prepared,rows,output,*,docker_archives=False,publish=False):
    output.mkdir(parents=True,exist_ok=True)
    cache=output/'cache';shutil.copytree(ROOT/'db-seed',cache)
    os.environ['TRIVY_CACHE_DIR']=str(cache)
    config,ignore=output/'trivy.yaml',output/'ignore.yaml'
    p.write_release_policy(config,ignore,use_exceptions=False)
    owners={r['image_name']:r for r in rows};stages={};registry=None
    if publish:
        registry='loom-ab-reg-'+uuid.uuid4().hex[:10]
        run('docker','run','-d','--name',registry,'--cpus','1','--memory','512m','-p','127.0.0.1::5000',info['registry_image'])
        port=run('docker','port',registry,'5000/tcp').rsplit(':',1)[1]
    try:
        for component,image in prepared.items():
            archive=Path(image['archive']);owner=owners[p.COMPONENTS[component]]
            report,vulns,sbom=[output/f'{component}.{s}' for s in ['policy.json','vulnerability.json','sbom.cdx.json']]
            t=time.monotonic()
            def scan(path):
                p._run(info['scanner'],'--config',str(config),'image','--input',str(path),'--format','json','--output',str(report),'--ignorefile',str(ignore),'--show-suppressed')
                p.validate_trivy_release_report(owner['image'],'amd64',report,ignore,use_exceptions=False,expected_artifact=path)
                p._run(info['scanner'],'image','--input',str(path),'--scanners','vuln','--format','json','--output',str(vulns),'--ignorefile',str(ignore))
                p._run(info['scanner'],'image','--input',str(path),'--format','cyclonedx','--output',str(sbom))
            if docker_archives:scan(archive)
            else:
                with p.oci_scan_layout(archive,output/(component+'.oci')) as layout:scan(layout)
            stage={'scan_seconds':time.monotonic()-t}
            if publish:
                tag=f'127.0.0.1:{port}/loom-ab/{component}:{SHA}'
                t=time.monotonic();p._copy_image(archive,tag,timeout_seconds=900);stage['copy_seconds']=time.monotonic()-t
                t=time.monotonic();digest=p._run('skopeo','inspect','--format','{{.Digest}}','docker://'+tag);stage['inspect_seconds']=time.monotonic()-t
                if digest!=image['scanned_digest']:raise ValueError('remote digest differs from scanned image')
            stages[component]=stage;save(output/'stages.json',stages)
    finally:
        if registry:run('docker','rm','-fv',registry)
    shutil.rmtree(cache)
    return stages

def harbor_probe(archive,output,label,info):
    with tarfile.open(archive) as tar:tag=json.load(tar.extractfile('manifest.json'))[0]['RepoTags'][0]
    start=time.monotonic();run('docker','load','--input',str(archive))
    logs={}
    try:
        for name in ['terminus_conformance_probe.py','terminus_continuation_probe.py']:
            result=run('docker','run','--rm','--network','none','--cap-drop','ALL','--security-opt','no-new-privileges','--memory','1g','--cpus','1',
                       '--cpuset-cpus',','.join(map(str,info['cpus'])),'-e','LITELLM_LOCAL_MODEL_COST_MAP=True','-v',f'{SOURCE}/tests/support/{name}:/probe.py:ro',
                       '--entrypoint','python',tag,'-I','-B','/probe.py')
            logs[name]=result
    finally:subprocess.run(['docker','image','rm',tag],capture_output=True)
    save(output/(label+'-harbor-probes.json'),{'seconds':time.monotonic()-start,'outputs':logs})

def setup():
    ROOT.mkdir(parents=True,exist_ok=False);(ROOT/'tools').mkdir();(ROOT/'metrics').mkdir()
    cpus=sorted(os.sched_getaffinity(0))[:4]
    if len(cpus)!=4 or shutil.disk_usage(ROOT).free<20*1024**3:raise ValueError('require4CPU/20GiB free')
    p,_=modules();t=time.monotonic();scanner=p.install_trivy(ROOT/'tools',architecture='amd64')
    (ROOT/'db-seed').mkdir();p._run(str(scanner),'--cache-dir',str(ROOT/'db-seed'),'image','--download-db-only')
    # Only ephemeral test-registry traffic can opt out of TLS, always loopback.
    wrapper=ROOT/'tools/skopeo'
    wrapper.write_text('#!/usr/bin/env python3\nimport subprocess,sys\na=sys.argv[1:]\nassert any("127.0.0.1:" in v for v in a)\na[1:1]=(["--dest-tls-verify=false","--src-tls-verify=false"] if a[0]=="copy" else ["--tls-verify=false"])\nraise SystemExit(subprocess.call(["/usr/bin/skopeo",*a]))\n')
    wrapper.chmod(0o700)
    run('docker','pull','mirror.gcr.io/library/registry:3')
    image=run('docker','image','inspect','mirror.gcr.io/library/registry:3','--format','{{.Id}}')
    save(ROOT/'setup.json',{'source_sha':SHA,'workflow_sha':os.environ['GITHUB_SHA'],'runner':os.environ['RUNNER_NAME'],'cpus':cpus,'scanner':str(scanner),
         'registry_image':image,'setup_seconds':time.monotonic()-t,'created_at':now(),'workers':WORKERS,'memory_bytes':9611563008})

def producer():
    info,p,old,rows=context();output=ROOT/'metrics/producer';output.mkdir()
    for module in [p,old]:module._diagnostic_dir=output
    start=time.monotonic();name,_=start_builder(info,output)
    try:
        prepared=old.prepare_images(SOURCE,SHA,'nebius-'+SHA,rows,ROOT/'payload',workers=WORKERS)
        cold=time.monotonic()-start
        legacy=ROOT/'legacy';warm=ROOT/'warm';legacy.mkdir();warm.mkdir()
        owners={r['image_name']:r for r in rows}
        def pair(component):
            p._build_diagnostic.filename=f"failed-warm-{component}.json"
            owner=owners[p.COMPONENTS[component]];tag=f'loom-ab-{component}-{os.environ["GITHUB_RUN_ID"]}:probe'
            a=legacy/(component+'.tar');b=warm/(component+'.tar')
            cmd=p.build_image_command(SOURCE,SHA,'nebius-'+SHA,owner,component,tag,b)
            before=cmd[:-2]+['--output',f'type=docker,name={tag},dest={a}']
            t=time.monotonic();p._run(*before);old_time=time.monotonic()-t
            if component=='harbor_runtime':cmd+=['--output',f'type=docker,name={tag},dest={warm}/harbor.docker.tar']
            t=time.monotonic();p._run(*cmd);new_time=time.monotonic()-t
            return component,{'docker_export_seconds':old_time,'oci_export_seconds':new_time,'positive_delta_seconds':max(0,new_time-old_time)}
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:pairs=dict(pool.map(pair,p.COMPONENTS))
        legacy_images={c:{'archive':legacy/(c+'.tar')} for c in p.COMPONENTS}
        legacy_scans=scans(info,p,legacy_images,rows,output/'legacy-scans',docker_archives=True)
        current_scans=scans(info,p,prepared,rows,output/'oci-scans',publish=True)
        harbor_probe(legacy/'harbor_runtime.tar',output,'legacy',info)
        harbor_probe(warm/'harbor.docker.tar',output,'dual-export',info)
        save(output/'result.json',{'source_sha':SHA,'cold_prepare_seconds':cold,'warm_export_pairs':pairs,'legacy_scans':legacy_scans,'current_scans':current_scans,
             'bytes':{c:Path(x['archive']).stat().st_size for c,x in prepared.items()},'validation':{'trivy_commands':42,'policy_checks':14,'harbor_probes':4,'digest_checks':7},'finished_at':now()})
    finally:stop_builder(name)

def case_init(case):
    info,p,old,rows=context();output=ROOT/'metrics'/case;output.mkdir()
    started=time.time();name,host=start_builder(info,output)
    scenario,mode=case.rsplit('-',1)
    available=list(p.COMPONENTS) if scenario=='all-hit' else PARTIAL
    state={'case':case,'scenario':scenario,'mode':mode,'source_sha':SHA,'started':started,'builder_name':name,'builder_host':host,'available':available,
           'expected_built':[c for c in p.COMPONENTS if c not in available],'work':str(ROOT/case),'ready':time.time()}
    work=Path(state['work']);work.mkdir()
    if mode=='new':
        with (output/'worker.log').open('w') as log:
            process=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'worker','--case',case],stdout=log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True,
                    env=os.environ|{'AB_STATE':json.dumps(state)})
        state['worker_pid']=process.pid
    state['download_before']=time.time();save(output/'state.json',state)

def worker(case):
    state=json.loads(os.environ['AB_STATE']);info,p,old,rows=context();p._diagnostic_dir=ROOT/'metrics'/case
    os.environ['BUILDKIT_HOST']=state['builder_host'];start=time.time();result=1
    try:
        images=p.export_missing_images(SOURCE,SHA,'nebius-'+SHA,rows,Path(state['work'])/'early',available_components=state['available'],workers=WORKERS)
        save(ROOT/'metrics'/case/'early.json',{'started':start,'finished':time.time(),'images':{c:{k:str(v) if isinstance(v,Path) else v for k,v in x.items()} for c,x in images.items()}})
        result=0
    finally:save(ROOT/'metrics'/case/'worker-exit.json',{'returncode':result,'finished':time.time()})

def case_finish(case,outcome):
    info,p,old,rows=context();output=ROOT/'metrics'/case;state=json.loads((output/'state.json').read_text());after=time.time()
    work=Path(state['work']);download=work/'download';os.environ['BUILDKIT_HOST']=state['builder_host']
    for module in [p,old]:module._diagnostic_dir=output
    if state['scenario']=='full-fallback' and outcome=='success':
        # All4 exact-SHA OCI payloads pass native digest; only names are cyclically
        # mismatched to exercise the existing per-component rejection boundary.
        hold=work/'wrong-components';hold.mkdir()
        for name in PARTIAL:shutil.move(str(download/(name+'.tar')),hold/(name+'.tar'))
        for source,target in zip(PARTIAL,PARTIAL[1:]+PARTIAL[:1],strict=True):shutil.move(str(hold/(source+'.tar')),download/(target+'.tar'))
        save(output/'injection.json',{'kind':'cyclic component name mismatch after successful native download','source_sha':SHA,'mapping':dict(zip(PARTIAL,PARTIAL[1:]+PARTIAL[:1],strict=True))})
    if state['mode']=='new':
        status=output/'worker-exit.json';deadline=time.monotonic()+1200
        while not status.is_file():
            if time.monotonic()>deadline:raise TimeoutError('prebuild join timed out')
            os.kill(state['worker_pid'],0);time.sleep(.2)
        if json.loads(status.read_text())['returncode']:raise RuntimeError('real early build failed; no fallback')
    joined=time.time();kwargs={'workers':WORKERS,'artifacts_dir':download if outcome=='success' else None}
    if state['mode']=='new':kwargs.update(built_dir=work/'early',built_components=state['expected_built'])
    module=old if state['mode']=='old' else p
    prepared=module.prepare_images(SOURCE,SHA,'nebius-'+SHA,rows,work/'images',**kwargs);at=time.time()
    expected={'all-hit':7,'partial-hit':4,'full-fallback':0}[state['scenario']]
    if sum(x['origin']=='reused' for x in prepared.values())!=expected:raise RuntimeError('did not exercise intended cache scenario')
    stages=scans(info,p,prepared,rows,output/'validation',publish=True);end=time.time()
    early=json.loads((output/'early.json').read_text()) if (output/'early.json').exists() else None
    record=state|{'download_outcome':outcome,'download_after':after,'join_after_download_seconds':joined-after,'prepare_after_join_seconds':at-joined,
                'transport_prepare_seconds':at-state['ready'],'validation_publish_seconds':end-at,'total_seconds':end-state['started'],
                'early':early,'overlap_seconds':max(0,min(early['finished'],after)-max(early['started'],state['download_before'])) if early else 0,
                'images':{c:{k:str(v) if isinstance(v,Path) else v for k,v in x.items()} for c,x in prepared.items()},'stages':stages,
                'validation':{'trivy_commands':21,'policy_checks':7,'digest_checks':7},'finished_at':now()}
    save(output/'result.json',record)
    t=time.monotonic();stop_builder(state['builder_name']);shutil.rmtree(work)
    record['cleanup_seconds']=time.monotonic()-t;record['total_including_cleanup_seconds']=record['total_seconds']+record['cleanup_seconds'];save(output/'result.json',record)
    print(json.dumps({'case':case,'total_seconds':record['total_seconds'],'transport_prepare_seconds':record['transport_prepare_seconds'],'overlap_seconds':record['overlap_seconds']}),flush=True)

def mark(label):
    with (ROOT/'metrics/markers.jsonl').open('a') as out:out.write(json.dumps({'label':label,'time':time.time(),'run_id':os.environ['GITHUB_RUN_ID']})+'\n')

def cleanup():
    for path in (ROOT/'metrics').glob('*/state.json'):
        state=json.loads(path.read_text());pid=state.get('worker_pid')
        if pid and not (path.parent/'worker-exit.json').is_file():
            try:os.killpg(pid,signal.SIGTERM);time.sleep(.2);os.killpg(pid,signal.SIGKILL)
            except ProcessLookupError:pass
    names=run('docker','ps','-aq','--filter','name=loom-candidate-ab-').split()
    names+=run('docker','ps','-aq','--filter','name=loom-ab-reg-').split()
    if names:run('docker','rm','-fv',*names)
    # Called only after retained evidence upload. Docker's files can be root-owned.
    if ROOT.exists():subprocess.run(['sudo','rm','-rf','--',str(ROOT)],check=True)

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('operation',choices=['setup','producer','init','finish','worker','mark','cleanup'])
parser.add_argument('--case');parser.add_argument('--outcome',default='success');parser.add_argument('--label')
args=parser.parse_args()
if not SHA or len(SHA)!=40 or any(c not in '0123456789abcdef' for c in SHA):raise ValueError('exactSHA required')
if args.operation=='setup':setup()
elif args.operation=='producer':producer()
elif args.operation=='init':case_init(args.case)
elif args.operation=='finish':case_finish(args.case,args.outcome)
elif args.operation=='worker':worker(args.case)
elif args.operation=='mark':mark(args.label)
elif args.operation=='cleanup':cleanup()
