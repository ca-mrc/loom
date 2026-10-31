"""Disposable native model proxy probes; no cloud/provider credentials or calls."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

ROOT = Path(__file__).resolve().parents[2]
ENVOY_IMAGE = "envoyproxy/envoy:v1.30-latest"
PYTHON_IMAGE = "python:3.11-slim"
CONNECTION_IDS = [str(UUID(int=value)) for value in (1, 2, 3)]
# The synthetic generator owns both 1200 outbound and 1200 upstream sockets.
# Docker daemon defaults can inherit soft nofile=1024 from a CI service. Keep
# this allowance inside the disposable containers, including cancel/reconnect
# headroom; it changes no host, product or provider concurrency policy.
_FIXTURE_NOFILE = 8192
_CHECKED_IMAGES: set[str] = set()

_RUNNER = r"""import asyncio,collections,errno,http.client,json,resource,ssl,time
from pathlib import Path

CFG=json.loads(Path('/fixture/scenario.json').read_text())
CIDS=CFG['connection_ids'];COUNT=CFG['requests'];INGRESS=CFG.get('hop')=='ingress';TLS=CFG['scheme']=='https' and not INGRESS
STATE={'active':0,'arrived':0,'peak':0,'header_leaks':0,'auth_errors':0,'per_cid':collections.Counter(),'total_requests':0,'status_requests':collections.Counter()}
CLIENT={'connected':0,'sent':0,'completed':0,'errors':collections.Counter()}
CLIENT_FD_LIMITS=resource.getrlimit(resource.RLIMIT_NOFILE)
assert CLIENT_FD_LIMITS[0]==resource.RLIM_INFINITY or CLIENT_FD_LIMITS[0]>=2*COUNT+128, 'native fixture needs client and upstream socket headroom; launch with its explicit nofile allowance'
RELEASE=asyncio.Event()
CLIENT_SSL=ssl.create_default_context(cafile='/fixture/cert.pem') if TLS or INGRESS else None

async def upstream(reader,writer):
 active=False
 try:
  raw=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),20)
  STATE['total_requests']+=1
  lines=raw.decode().split('\r\n');path=lines[0].split()[1];headers={k.lower():v.strip() for line in lines[1:] if ':' in line for k,v in [line.split(':',1)]}
  holding=path.startswith('/hold/') or headers.get('x-fixture-hold')=='1'
  cid=headers.get('authorization','').removeprefix('Bearer synthetic-')
  STATE['header_leaks']+=int('x-loom-connection-id' in headers)
  STATE['auth_errors']+=int(headers.get('authorization')!='Bearer synthetic-'+cid)
  if holding:
   STATE['active']+=1;STATE['arrived']+=1;STATE['peak']=max(STATE['peak'],STATE['active']);STATE['per_cid'][cid]+=1;active=True
   released=asyncio.create_task(RELEASE.wait());gone=asyncio.create_task(reader.read(1))
   done,pending=await asyncio.wait([released,gone],return_when=asyncio.FIRST_COMPLETED)
   for task in pending:task.cancel()
   await asyncio.gather(*pending,return_exceptions=True)
   if gone in done and gone.result()==b'':return
  code=int(headers.get('x-fixture-status','200')) if INGRESS else int(path.split('/')[-1]) if path.startswith('/status/') else 200
  if code>=400:STATE['status_requests'][str(code)]+=1
  writer.write(f'HTTP/1.1 {code} Synthetic\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok'.encode());await writer.drain()
 except (asyncio.IncompleteReadError,ConnectionError,TimeoutError,ssl.SSLError):pass
 finally:
  if active:STATE['active']-=1
  writer.close()
  try:await asyncio.wait_for(writer.wait_closed(),1)
  except (Exception,asyncio.CancelledError):pass

def error_category(error):
 if isinstance(error,TimeoutError):return 'timeout'
 if isinstance(error,ssl.SSLError):return 'tls'
 if isinstance(error,ConnectionError):return 'connection'
 if isinstance(error,OSError):return 'file_descriptor_exhausted' if error.errno in {errno.EMFILE,errno.ENFILE} else 'os'
 if isinstance(error,(ValueError,IndexError,asyncio.IncompleteReadError)):return 'protocol'
 return 'other'

def client_snapshot(start):
 return {**{key:CLIENT[key]-start[key] for key in ('connected','sent','completed')},'errors':{key:value-start['errors'].get(key,0) for key,value in CLIENT['errors'].items() if value>start['errors'].get(key,0)}}

async def request(cid,path=None,authority=None,capture=False):
 writer=None;outcome=None
 try:
  reader,writer=await asyncio.open_connection('127.0.0.1',CFG.get('front_port',30443),ssl=CLIENT_SSL if INGRESS else None,server_hostname='provider.example.test' if INGRESS else None)
  CLIENT['connected']+=1
  target=authority or ('provider.example.test' if INGRESS else 'provider.example.test:19090')
  if TLS:
   writer.write(f'CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nx-loom-connection-id: {cid}\r\n\r\n'.encode());await writer.drain()
   CLIENT['sent']+=1
   head=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),30)
   status=int(head.split(b' ',2)[1])
   if status!=200:return {'status':status,'content_length':0} if capture else status
   await writer.start_tls(CLIENT_SSL,server_hostname='provider.example.test',ssl_handshake_timeout=20)
  path=path or '/hold/'+cid
  uri='/openai/v1/chat/completions' if INGRESS else path if TLS else 'http://'+target+path
  outer='' if TLS or INGRESS else f'x-loom-connection-id: {cid}\r\n'
  if INGRESS:
   outer+=('x-fixture-hold: 1\r\n' if path.startswith('/hold/') else '')
   if path.startswith('/status/'):outer+='x-fixture-status: '+path.split('/')[-1]+'\r\n'
  writer.write(f'POST {uri} HTTP/1.1\r\nHost: {target}\r\n{outer}Authorization: Bearer synthetic-{cid}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'.encode());await writer.drain()
  if not TLS:CLIENT['sent']+=1
  head=await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'),60)
  status=int(head.split(b' ',2)[1]);length=next((int(line.split(b':',1)[1]) for line in head.split(b'\r\n') if line.lower().startswith(b'content-length:')),None)
  return {'status':status,'content_length':length} if capture else status
 except asyncio.CancelledError:outcome='cancelled';raise
 except Exception as error:outcome=error_category(error);return 'transport_error'
 finally:
  CLIENT['completed']+=1
  if outcome:CLIENT['errors'][outcome]+=1
  if writer:
   writer.close()
   try:await asyncio.wait_for(writer.wait_closed(),1)
   except (Exception,asyncio.CancelledError):pass

async def wait_for(predicate,seconds=15):
 cutoff=time.monotonic()+seconds
 while not predicate() and time.monotonic()<cutoff:await asyncio.sleep(.02)
 return predicate()

async def stats(seconds=5):
 def read():
  connection=http.client.HTTPConnection('127.0.0.1',9901,timeout=max(.001,seconds))
  try:
   connection.request('GET','/stats?format=json')
   response=connection.getresponse();assert response.status==200
   data=json.loads(response.read());wanted={'upstream_cx_active','upstream_cx_total','upstream_cx_overflow','upstream_cx_connect_fail','upstream_cx_connect_timeout','upstream_rq_active','upstream_rq_pending_active','upstream_rq_pending_overflow','upstream_rq_timeout','downstream_cx_active','downstream_cx_total','downstream_cx_overflow'}
   counters={x['name']:x['value'] for x in data['stats'] if x.get('name','').startswith(('cluster.egress-','http.forward_proxy.','listener.')) and x['name'].rsplit('.',1)[-1] in wanted}
   return {'available':True,'pending_active':sum(value for name,value in counters.items() if name.startswith('cluster.egress-') and name.endswith('.upstream_rq_pending_active')),'counters':counters}
  finally:connection.close()
 try:return await asyncio.wait_for(asyncio.to_thread(read),max(.001,seconds))
 except Exception as error:return {'available':False,'pending_active':None,'error_category':error_category(error),'counters':{}}

def completed_503(tasks):
 return sum(task.done() and not task.cancelled() and task.exception() is None and task.result()==503 for task in tasks)

async def wave(cids,cancel=False):
 RELEASE.clear();start=STATE['arrived'];STATE['per_cid'].clear()
 client_start={key:dict(value) if isinstance(value,dict) else value for key,value in CLIENT.items()}
 tasks=[];negative_complete=None;envoy=None;baseline_extra=None
 def launch(count,offset=0):return [asyncio.create_task(request(cids[(offset+i)%len(cids)])) for i in range(count)]
 if CFG['baseline']:
  # First occupy the old cluster ceiling, then measure what happens to every
  # remaining request: pending and overflow are both real legacy behavior.
  # Both phases share the original ten-second deadline.
  deadline=time.monotonic()+10;initial=min(COUNT,1024);extra=COUNT-initial
  tasks=launch(initial)
  await wait_for(lambda:STATE['active']==initial,max(0,deadline-time.monotonic()))
  tasks+=launch(extra,initial);negative_complete=False
  while time.monotonic()<deadline:
   envoy=await stats(max(.001,deadline-time.monotonic()))
   overflow=completed_503(tasks)
   negative_complete=(STATE['active']==initial and envoy['available'] and envoy['pending_active']+overflow==extra and CLIENT['sent']-client_start['sent']==COUNT)
   if negative_complete:break
   await asyncio.sleep(min(.02,max(0,deadline-time.monotonic())))
  envoy=envoy or await stats()
  baseline_extra={'requested':extra,'pending_before_release':envoy['pending_active'],'completed_503_before_release':completed_503(tasks)}
 else:
  tasks=launch(COUNT)
  await wait_for(lambda:STATE['active']==COUNT,20)
 reached=STATE['active']==COUNT
 if not INGRESS and envoy is None:envoy=await stats()
 observed={'requested':COUNT,'arrived_before_release':STATE['arrived']-start,'active_before_release':STATE['active'],'peak_before_release':STATE['peak'],'barrier_reached':reached,'negative_control_complete':negative_complete,'baseline_extra':baseline_extra,'pending_before_release':None if INGRESS else envoy['pending_active'],'envoy_before_release':envoy,'client_before_release':client_snapshot(client_start),'per_cid_before_release':dict(STATE['per_cid'])}
 if cancel and reached:
  cancelled=tasks[::2]
  for task in cancelled:task.cancel()
  await asyncio.gather(*cancelled,return_exceptions=True)
  assert await wait_for(lambda:STATE['active']==COUNT-len(cancelled)), 'cancel did not release upstream slots'
  replacements=[asyncio.create_task(request(cids[i%len(cids)])) for i in range(len(cancelled))]
  assert await wait_for(lambda:STATE['active']==COUNT),'replacement requests queued after cancellation'
  observed.update(cancelled=len(cancelled),replacement_active=STATE['active']);tasks=tasks[1::2]+replacements
 RELEASE.set();results=await asyncio.wait_for(asyncio.gather(*tasks,return_exceptions=True),60)
 assert await wait_for(lambda:STATE['active']==0),'upstream slots leaked after completion'
 observed['responses']={str(k):v for k,v in collections.Counter(str(r) for r in results).items()}
 observed['client_after_wave']=client_snapshot(client_start)
 observed['envoy_after_wave']=None if INGRESS else await stats()
 return observed

class H2Peer:
 def __init__(self):
  from h2.connection import H2Connection
  from h2.config import H2Configuration
  self.connection=H2Connection(config=H2Configuration(client_side=True,header_encoding='utf-8'));self.statuses={};self.completed=set();self.streams=[]
 async def open(self):
  from h2.events import RemoteSettingsChanged
  context=ssl.create_default_context(cafile='/fixture/cert.pem');context.set_alpn_protocols(['h2'])
  self.reader,self.writer=await asyncio.open_connection('127.0.0.1',CFG['front_port'],ssl=context,server_hostname='provider.example.test')
  assert self.writer.get_extra_info('ssl_object').selected_alpn_protocol()=='h2'
  self.connection.initiate_connection();self.writer.write(self.connection.data_to_send());await self.writer.drain()
  while True:
   events=self.connection.receive_data(await self.reader.read(65536));self.writer.write(self.connection.data_to_send());await self.writer.drain()
   if any(isinstance(event,RemoteSettingsChanged) for event in events):break
  self.maximum=self.connection.remote_settings.max_concurrent_streams;self.task=asyncio.create_task(self.read())
 async def read(self):
  from h2.events import ResponseReceived,DataReceived,StreamEnded,StreamReset
  while True:
   raw=await self.reader.read(65536)
   if not raw:return
   for event in self.connection.receive_data(raw):
    if isinstance(event,ResponseReceived):self.statuses[event.stream_id]=int(dict(event.headers)[':status'])
    elif isinstance(event,DataReceived):self.connection.acknowledge_received_data(event.flow_controlled_length,event.stream_id)
    elif isinstance(event,(StreamEnded,StreamReset)):self.completed.add(event.stream_id)
   self.writer.write(self.connection.data_to_send());await self.writer.drain()
 def send(self,count,offset=0,hold=True,status=None):
  result=[]
  for i in range(count):
   stream=self.connection.get_next_available_stream_id();cid=CIDS[(offset+i)%len(CIDS)]
   headers=[(':method','POST'),(':scheme','https'),(':authority','provider.example.test'),(':path','/openai/v1/chat/completions'),('authorization','Bearer synthetic-'+cid),('content-length','0')]
   if hold:headers.append(('x-fixture-hold','1'))
   if status:headers.append(('x-fixture-status',str(status)))
   self.connection.send_headers(stream,headers,end_stream=True);self.streams.append(stream);result.append(stream)
  self.writer.write(self.connection.data_to_send());return result
 def reset(self,streams):
  for stream in streams:self.connection.reset_stream(stream)
  self.writer.write(self.connection.data_to_send())
 async def close(self):
  self.writer.close()
  try:await asyncio.wait_for(self.writer.wait_closed(),1)
  except Exception:pass
  self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)

async def h2_wave():
 RELEASE.clear();peers=[];remaining=COUNT;offset=0
 try:
  while remaining:
   peer=H2Peer();await peer.open();count=min(remaining,peer.maximum);peer.initial=peer.send(count,offset);peers.append(peer);remaining-=count;offset+=count
  assert await wait_for(lambda:STATE['active']==COUNT,20),'H2 requests queued by proxy instead of new connections'
  result={'h2':{'active_before_release':STATE['active'],'connections':len(peers),'peer_max_streams':[p.maximum for p in peers],'per_cid':dict(STATE['per_cid'])}}
  cancelled=0
  for peer in peers:
   peer.cancelled=peer.initial[::2];peer.reset(peer.cancelled);cancelled+=len(peer.cancelled)
  assert await wait_for(lambda:STATE['active']==COUNT-cancelled),'H2 reset did not release H1 upstream'
  for peer in peers:peer.replacements=peer.send(len(peer.cancelled))
  assert await wait_for(lambda:STATE['active']==COUNT),'H2 replacements queued or leaked credit'
  result['h2'].update(cancelled=cancelled,replacement_active=STATE['active'])
  RELEASE.set()
  assert await wait_for(lambda:all(set(peer.initial[1::2]+peer.replacements)<=peer.completed for peer in peers),30)
  assert all(peer.statuses[stream]==200 for peer in peers for stream in peer.initial[1::2]+peer.replacements)
  assert await wait_for(lambda:STATE['active']==0),'H2 completion left upstream requests'
  peer=peers[0];special=[peer.send(1,hold=False,status=code)[0] for code in (429,500,200)]
  assert await wait_for(lambda:set(special)<=peer.completed)
  result['upstream_statuses']=[peer.statuses[s] for s in special[:2]];result['recovery']=peer.statuses[special[-1]]
  return result
 finally:
  for peer in peers:await peer.close()

async def readiness():
 async def probe(port,frontend=False):
  reader,writer=await asyncio.wait_for(asyncio.open_connection('127.0.0.1',port,ssl=CLIENT_SSL if frontend and INGRESS else None,server_hostname='provider.example.test' if frontend and INGRESS else None),1)
  try:
   if not frontend:
    writer.write(b'GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n');await writer.drain()
    if b' 200 ' not in await asyncio.wait_for(reader.readline(),1):raise OSError('Envoy not ready')
  finally:
   writer.close()
   try:await asyncio.wait_for(writer.wait_closed(),.1)
   except (OSError,TimeoutError):pass
 try:
  async with asyncio.timeout(10):
   while True:
    try:
     await probe(CFG.get('front_port',30443),frontend=True)
     if not INGRESS:await probe(9901)
     return
    except (OSError,TimeoutError):await asyncio.sleep(.05)
 except TimeoutError:raise RuntimeError('native proxy listener/admin readiness failed before concurrency wave') from None

async def main():
 server_ssl=None
 if TLS:
  server_ssl=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);server_ssl.load_cert_chain('/fixture/cert.pem','/fixture/key.pem')
 server=await asyncio.start_server(upstream,'127.0.0.1',19090,ssl=server_ssl,backlog=4096,limit=16384)
 async with server:
  await readiness()
  result={'scheme':CFG['scheme'],'baseline':CFG['baseline'],'client_file_descriptor_limits':list(CLIENT_FD_LIMITS)}
  if CFG.get('client_protocol')=='h2':
   result.update(await h2_wave());result['active_at_end']=STATE['active'];result['header_leaks']=STATE['header_leaks'];result['auth_errors']=STATE['auth_errors'];result['status_request_counts']=dict(STATE['status_requests']);print(json.dumps(result),flush=True);return
  result['same_cid']=await wave([CIDS[0]],cancel=not CFG['baseline'])
  if not CFG['baseline']:
   result['three_cid_reuse']=await wave(CIDS)
   result['upstream_statuses']=[await request(CIDS[0],'/status/'+str(status)) for status in (429,500)]
   result['recovery']=await request(CIDS[0],'/ok')
   before=STATE['total_requests']
   if not INGRESS:result['wrong_cid']=await request('ffffffff-ffff-ffff-ffff-ffffffffffff','/ok')
   bad=await request(CIDS[0],'/ok',authority='foreign.example.test:19090',capture=True);result['wrong_authority']=bad['status'];result['wrong_authority_empty']=bad['content_length']==0;result['denied_never_arrived']=STATE['total_requests']==before
  result['active_at_end']=STATE['active'];result['header_leaks']=STATE['header_leaks'];result['auth_errors']=STATE['auth_errors'];result['status_request_counts']=dict(STATE['status_requests'])
  print(json.dumps(result),flush=True)

asyncio.run(main())
"""


@dataclass
class _Connection:
    id: UUID
    resolved_egress_ips: list[str]
    upstream_host: str
    base_url: str
    deleted_at: None = None


def _run(args: list[str], *, timeout: int = 180, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise RuntimeError(f"disposable proxy command failed ({args[0]}): {result.stderr[-1500:]}")
    return result.stdout


def _container_fd_limits(name: str) -> dict[str, Any]:
    result = subprocess.run(["docker", "exec", name, "/bin/cat", "/proc/1/limits"],
                            capture_output=True, text=True, timeout=10)
    match = re.search(r"^Max open files\s+(\S+)\s+(\S+)", result.stdout, re.MULTILINE)
    return {"available": True, "soft": match[1], "hard": match[2]} if match else {"available": False}


def _container_diagnostics(names: list[str]) -> str:
    """Only disposable synthetic fixtures: keep state/logs until failure capture."""
    diagnostics = {}
    for name in names:
        state = subprocess.run(["docker", "inspect", "--format", "{{json .State}}", name],
                               capture_output=True, text=True, timeout=10)
        try:
            value = json.loads(state.stdout)
            safe_state = {key: value.get(key) for key in ("Running", "ExitCode", "OOMKilled", "Error")}
        except (ValueError, AttributeError):
            safe_state = {"unavailable": True}
        logs = subprocess.run(["docker", "logs", "--tail", "30", name],
                              capture_output=True, text=True, timeout=10)
        diagnostics[name] = {"state": safe_state, "file_descriptor_limits": _container_fd_limits(name),
                             "synthetic_logs": (logs.stdout + logs.stderr)[-4000:]}
    return json.dumps(diagnostics)


def _image(image: str) -> None:
    if image in _CHECKED_IMAGES:
        return
    result = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
    if result.returncode:
        _run(["docker", "pull", image], timeout=180)
    _CHECKED_IMAGES.add(image)


def certificate(directory: Path) -> None:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "provider.example.test")])
    now = datetime.now(UTC)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName("provider.example.test")]), False).sign(key, hashes.SHA256()))
    (directory / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    private = directory / "key.pem"
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    private.chmod(0o600)


def native_envoy_case(scheme: str, *, baseline: bool, requests: int = 1200, renderer_env: dict[str, str] | None = None) -> dict[str, Any]:
    """Native Envoy and upstream share an isolated --network=none namespace.

    The baseline removes only the new provider circuit-breaker policy, exactly
    restoring the formerly implicit 1024 defaults. No response is released until
    the upstream barrier has measured simultaneous arrivals (or timed out).
    """
    for image in (ENVOY_IMAGE, PYTHON_IMAGE):
        _image(image)
    payload = _run([sys.executable, "-c", "import json,sys; from tests.support.model_proxy_concurrency import _envoy_bootstrap; print(json.dumps(_envoy_bootstrap(sys.argv[1],baseline=sys.argv[2]=='1')))", scheme, str(int(baseline))], env=renderer_env)
    config = json.loads(payload)
    with tempfile.TemporaryDirectory(prefix="loom-native-model-proxy-") as temporary:
        directory = Path(temporary)
        # The official Envoy entrypoint drops to its non-root envoy user.
        # Linux bind mounts retain TemporaryDirectory's 0700 mode (Docker
        # Desktop can mask this); allow traversal of this synthetic fixture.
        # certificate() still keeps the disposable private key at 0600.
        directory.chmod(0o755)
        certificate(directory)
        bootstrap = directory / "bootstrap.json"
        bootstrap.write_text(json.dumps(config))
        bootstrap.chmod(0o644)
        (directory / "runner.py").write_text(_RUNNER)
        (directory / "scenario.json").write_text(json.dumps({"connection_ids": CONNECTION_IDS, "requests": requests,
                                                             "scheme": scheme, "baseline": baseline}))
        name = "loom-native-model-" + uuid4().hex[:10]
        mount = f"type=bind,source={directory},target=/fixture,readonly"
        try:
            _run(["docker", "run", "-d", "--pull=never", "--network", "none", "--name", name,
                  "--ulimit", f"nofile={_FIXTURE_NOFILE}:{_FIXTURE_NOFILE}",
                  "--mount", mount, ENVOY_IMAGE, "--concurrency", "1", "--disable-hot-restart",
                  "-c", "/fixture/bootstrap.json", "--log-level", "error"], timeout=30)
            output = _run(["docker", "run", "--rm", "--pull=never", "--network", "container:" + name,
                           "--ulimit", f"nofile={_FIXTURE_NOFILE}:{_FIXTURE_NOFILE}",
                           "--mount", mount, PYTHON_IMAGE, "python", "/fixture/runner.py"])
            result = json.loads(output)
            # Preserve bounded diagnostics before finally removes the container;
            # a successful runner can still fail the caller's count assertions.
            result["native_container_diagnostics"] = json.loads(_container_diagnostics([name]))
            return result
        except Exception as error:
            raise RuntimeError(f"{error}\nNative Envoy fixture: {_container_diagnostics([name])}") from error
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)


@contextmanager
def envoy_renderer_environment() -> Iterator[dict[str, str]]:
    """Use the standalone xDS image's exact proto pins in a subprocess only.

    Missing optional packages must not silently skip the real Docker proof. The
    regular integration interpreter/environment is never pip-modified.
    """
    requirements = re.findall(r'"((?:envoy-data-plane|xds-protos)==[^"\s]+)"', (ROOT / "deploy/Dockerfile.egress-xds").read_text())
    assert len(requirements) == 2
    probe = "from importlib.metadata import version; import sys; assert all(version(r.split('==')[0])==r.split('==')[1] for r in sys.argv[1:])"
    environment = dict(os.environ)
    existing = subprocess.run([sys.executable, "-c", probe, *requirements], capture_output=True, env=environment)
    if existing.returncode == 0:
        yield environment
        return
    with tempfile.TemporaryDirectory(prefix="loom-envoy-proto-dependencies-") as temporary:
        uv = shutil.which("uv")
        if uv is None:
            raise RuntimeError("uv is required to prepare the standalone xDS test dependencies")
        _run([uv, "pip", "install", "--python", sys.executable, "--target", temporary, *requirements])
        environment["PYTHONPATH"] = os.pathsep.join([temporary, str(ROOT / "src"), str(ROOT)])
        _run([sys.executable, "-c", probe, *requirements], env=environment)
        yield environment


def _envoy_bootstrap(scheme: str, *, baseline: bool) -> dict[str, Any]:
    from google.protobuf.json_format import MessageToDict

    from loom_egress_xds.config_builder import build_snapshot
    from loom_egress_xds.envoy_translator import build_envoy_config

    snapshot = build_snapshot([_Connection(UUID(cid), ["127.0.0.1"], "provider.example.test",
                                          f"{scheme}://provider.example.test:19090/v1") for cid in CONNECTION_IDS])
    rendered = build_envoy_config(snapshot)
    if baseline:
        for cluster in rendered.clusters:
            cluster.ClearField("circuit_breakers")
    config = yaml.safe_load((ROOT / "deploy/envoy/egress-proxy.yaml").read_text())
    config.pop("dynamic_resources")
    config["static_resources"]["clusters"] = [MessageToDict(c, preserving_proto_field_name=True) for c in rendered.clusters]
    hcm = config["static_resources"]["listeners"][0]["filter_chains"][0]["filters"][0]["typed_config"]
    hcm.pop("rds")
    hcm["route_config"] = MessageToDict(rendered.route_configuration, preserving_proto_field_name=True)
    return config


def native_ingress_case(config: dict[str, Any], *, protocol: str, renderer_env: dict[str, str]) -> dict[str, Any]:
    """Actual rendered Traefik TLS passthrough → Caddy → H1 model upstream.

    Only local addresses/certificate paths and discovery/issuance are adapted:
    Kubernetes discovery and ACME are not part of this disconnected load probe.
    All traffic shares the first container's isolated --network=none namespace.
    """
    from copy import deepcopy

    from loom.nebius_environment_contract import FoundationBinding
    from loom.nebius_platform_render import public_tls_config
    from loom.nebius_shared_ingress import SharedIngressInstallation, render_shared_ingress

    caddy_image, traefik_image = "caddy:2.11.4-alpine", "traefik:v3.7.13"
    for image in (caddy_image, traefik_image, PYTHON_IMAGE):
        _image(image)
    config = deepcopy(config)
    config["public_host"] = "provider.example.test"
    caddy = public_tls_config(config)
    caddy["storage"]["root"] = "/tmp/caddy"
    caddy["apps"]["tls"].pop("automation")
    caddy["apps"]["tls"]["certificates"] = {"load_files": [{"certificate": "/fixture/cert.pem", "key": "/fixture/key.pem"}]}
    public = caddy["apps"]["http"]["servers"]["public"]
    public["automatic_https"] = {"disable": True}
    routes = public["routes"][0]["handle"][-1]["routes"]
    model = next(route for route in routes if any("/openai/v1/chat/completions" in match.get("path", []) for match in route.get("match", [])))
    model["handle"][0]["upstreams"] = [{"dial": "127.0.0.1:19090"}]
    # The retained shared ingress foundation owns only the primary region;
    # regional model routing belongs to the separately rendered Caddy config.
    foundation_config = dict(config)
    foundation_config.pop("regional_execution_targets", None)
    foundation = FoundationBinding(platform_config_json=json.dumps(foundation_config), public_dns_zone="dev.example.test",
                                   ingress_class_name="loom-shared", ingress_namespace=config["namespace"],
                                   ingress_controller_label="loom-shared-ingress")
    documents = render_shared_ingress(SharedIngressInstallation(
        installation_id=UUID("30000000-0000-4000-8000-000000000001"), foundation=foundation,
        tls_secret_name="loom-shared-test-tls", image=f"cr.{config['region']}.nebius.cloud/test/traefik@sha256:" + "d" * 64))
    data = next(row["data"] for row in documents if row["kind"] == "ConfigMap")
    static, dynamic = json.loads(data["traefik.json"]), json.loads(data["routes.yaml"])
    static["entryPoints"]["websecure"]["address"] = ":8444"
    static["providers"] = {"file": {"filename": "/fixture/traefik-routes.yaml", "watch": True}}
    dynamic["tcp"]["services"]["standalone"]["loadBalancer"]["servers"] = [{"address": "127.0.0.1:8443"}]
    dynamic["tls"]["certificates"] = [{"certFile": "/fixture/cert.pem", "keyFile": "/fixture/key.pem"}]
    dynamic["tls"]["stores"]["default"]["defaultCertificate"] = {"certFile": "/fixture/cert.pem", "keyFile": "/fixture/key.pem"}
    with tempfile.TemporaryDirectory(prefix="loom-native-model-ingress-") as temporary:
        directory = Path(temporary)
        certificate(directory)
        (directory / "caddy.json").write_text(json.dumps(caddy))
        (directory / "traefik.json").write_text(json.dumps(static))
        (directory / "traefik-routes.yaml").write_text(json.dumps(dynamic))
        (directory / "runner.py").write_text(_RUNNER)
        (directory / "scenario.json").write_text(json.dumps({"connection_ids": CONNECTION_IDS, "requests": 1200,
            "scheme": "https", "baseline": False, "hop": "ingress", "front_port": 8444, "client_protocol": protocol}))
        vendor = directory / "vendor"
        vendor.mkdir()
        paths = json.loads(_run([sys.executable, "-c", "import json,h2,hpack,hyperframe; from pathlib import Path; print(json.dumps([str(Path(m.__file__).parent) for m in (h2,hpack,hyperframe)]))"], env=renderer_env))
        for path in paths:
            source = Path(path)
            shutil.copytree(source, vendor / source.name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        caddy_name, traefik_name = "loom-model-caddy-" + uuid4().hex[:10], "loom-model-ingress-" + uuid4().hex[:10]
        mount = f"type=bind,source={directory},target=/fixture,readonly"
        try:
            _run(["docker", "run", "-d", "--pull=never", "--network", "none", "--name", caddy_name,
                  "--ulimit", f"nofile={_FIXTURE_NOFILE}:{_FIXTURE_NOFILE}",
                  "--mount", mount, caddy_image, "caddy", "run", "--config", "/fixture/caddy.json"], timeout=30)
            _run(["docker", "run", "-d", "--pull=never", "--network", "container:" + caddy_name,
                  "--name", traefik_name, "--ulimit", f"nofile={_FIXTURE_NOFILE}:{_FIXTURE_NOFILE}",
                  "--mount", mount, traefik_image,
                  "--configFile=/fixture/traefik.json"], timeout=30)
            result = _run(["docker", "run", "--rm", "--pull=never", "--network", "container:" + caddy_name,
                           "--ulimit", f"nofile={_FIXTURE_NOFILE}:{_FIXTURE_NOFILE}",
                           "--mount", mount, "--env", "PYTHONPATH=/fixture/vendor", PYTHON_IMAGE, "python", "/fixture/runner.py"])
            return json.loads(result)
        except Exception as error:
            raise RuntimeError(f"{error}\nNative ingress fixture: {_container_diagnostics([traefik_name, caddy_name])}") from error
        finally:
            for name in (traefik_name, caddy_name):
                subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=15)
