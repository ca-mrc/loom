"""Native-action timing markers, explicit ZIP extraction, existing image scans."""
from __future__ import annotations
import argparse, concurrent.futures, json, os, pathlib, shutil, subprocess, time, zipfile
from verify_artifacts import _publisher, inspect_all, scan_one

def save(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+'\n')

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation',choices=['mark','extract','verify'])
    p.add_argument('--root',type=pathlib.Path,required=True)
    p.add_argument('--label',required=True)
    p.add_argument('--input',type=pathlib.Path)
    p.add_argument('--source',type=pathlib.Path)
    p.add_argument('--sha')
    p.add_argument('--scan',action='store_true')
    args=p.parse_args()
    root=args.root.resolve(); metrics=root/'metrics'; metrics.mkdir(parents=True,exist_ok=True)
    if args.operation=='mark':
        with (metrics/'action-markers.jsonl').open('a') as stream:
            stream.write(json.dumps({'label':args.label,'time':time.time(),'runner':os.environ.get('RUNNER_NAME'),'run_id':os.environ.get('GITHUB_RUN_ID')})+'\n')
        return
    started=time.monotonic()
    if args.operation=='extract':
        files=[p for p in args.input.rglob('*') if p.is_file()]
        assert len(files)==1, 'expected exactly one raw native artifact download'
        output=root/args.label
        assert not output.exists(), 'refusing an old extraction directory'
        with zipfile.ZipFile(files[0]) as bundle:
            for entry in bundle.infolist():
                assert (output/entry.filename).resolve().is_relative_to(output), 'artifact entry escapes extraction root'
            bundle.extractall(output)
        save(metrics/(args.label+'-extract.json'),{'seconds':time.monotonic()-started,'zip_bytes':files[0].stat().st_size,'file_count':sum(1 for f in output.rglob('*') if f.is_file()),'method':'diagnostic raw download then Python ZIP extraction; not the default overlapped download+extract'})
        files[0].unlink()
        return
    publisher=_publisher(args.source)
    manifest=json.loads((args.input/'manifest.json').read_text())
    assert manifest['source_sha']==args.sha
    items={component:{'archive':args.input/row['archive'],'source_sha':row['source_sha'],'digest':row['digest']} for component,row in manifest['images'].items()}
    assert len(list(args.input.glob('*.tar')))==7
    result=inspect_all(args.source,items,args.sha,'exact')
    result['inspection_wall_seconds']=time.monotonic()-started
    result['scans']={}
    if args.scan:
        setup=time.monotonic(); tools=root/'tools'; tools.mkdir(exist_ok=True)
        installed=tools/'scanner.json'
        if installed.exists(): scanner=pathlib.Path(json.loads(installed.read_text())['path'])
        else:
            scanner=publisher.install_trivy(tools,architecture='amd64')
            save(installed,{'path':str(scanner)})
        database=root/'scanner-db'; database.mkdir(exist_ok=True)
        if not (database/'db/metadata.json').exists():
            with (metrics/'db-download.log').open('wb') as stream:
                subprocess.run([str(scanner),'--cache-dir',str(database),'image','--download-db-only'],stdout=stream,stderr=subprocess.STDOUT,check=True,timeout=600)
        result['db_metadata']=json.loads((database/'db/metadata.json').read_text())
        result['scanner_setup_seconds']=time.monotonic()-setup
        os.environ['TRIVY_SKIP_DB_UPDATE']='true'
        os.environ['GOMAXPROCS']='1'
        owners={r['image_name']:r['image'] for r in publisher.release_image_matrix(publisher.load_manifest(args.source/'config/component-ownership.toml'),image_set='nebius')}
        def scan(component):
            cache=root/'analysis-cache'/component; cache.mkdir(parents=True,exist_ok=True)
            if not (cache/'db').exists(): (cache/'db').symlink_to(database/'db',target_is_directory=True)
            return scan_one(scanner,items[component]['archive'],component,owners[publisher.COMPONENTS[component]],metrics/args.label,cache,source_root=args.source)
        scan_start=time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(7,os.cpu_count() or 1)) as pool:
            pending={pool.submit(scan,c):c for c in items}
            for future in concurrent.futures.as_completed(pending): result['scans'][pending[future]]=future.result()
        result['scan_wall_seconds']=time.monotonic()-scan_start
    result['total_seconds']=time.monotonic()-started
    save(metrics/(args.label+'-verification.json'),result)
    print(json.dumps({'label':args.label,'images':len(items),'scan_count':len(result['scans']),'seconds':result['total_seconds']}),flush=True)
if __name__=='__main__': main()
