"""Reproducible local lab. Python 3.10+, standard library only."""
import argparse
import csv
import hashlib
import json
import platform
import subprocess
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / 'results'
RESULTS.mkdir(exist_ok=True)
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'compose.yaml')]
SF = 'sf50'
TABLES = ['supplier', 'part', 'partsupp', 'region', 'nation', 'customer', 'orders', 'lineitem']
CONFIGS = [('parquet_zstd', 'PARQUET', 'ZSTD'), ('orc_gzip', 'ORC', 'GZIP'), ('parquet_gzip', 'PARQUET', 'GZIP'), ('orc_zstd', 'ORC', 'ZSTD')]

def docker(*args):
    return subprocess.check_output(COMPOSE + list(args), text=True, encoding='utf-8', errors='replace')

def api(url, data=None, method=None, headers=None):
    if isinstance(data, dict):
        data = json.dumps(data).encode()
    if isinstance(data, str):
        data = data.encode()
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    with urllib.request.urlopen(req, timeout=120) as response:
        body = response.read()
        return json.loads(body) if body else None

def save(name, data):
    (RESULTS / name).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')

def wait_engine(prefix=None):
    for attempt in range(90):
        try:
            if not api('http://localhost:18080/v1/info').get('starting',True):
                nodes=sql("SELECT count(*) FROM system.runtime.nodes WHERE state='active'",'engine_readiness')
                if nodes['rows'][0][0]>0:
                    if prefix:
                        sql(f'SELECT regionkey FROM {prefix}.region LIMIT 1','catalog_worker_readiness')
                    return
        except (OSError,urllib.error.URLError):
            pass
        except RuntimeError as exc:
            if not any(text in str(exc) for text in ['No nodes available','still initializing','SERVER_STARTING_UP']):
                raise
        time.sleep(2)
    raise RuntimeError('Trino active worker registration timed out')

def sql(query, label=None, session=None):
    started = time.perf_counter()
    headers={'X-Trino-User': 'balanov', 'Content-Type': 'text/plain; charset=utf-8'}
    if session:
        headers['X-Trino-Session']=session
    page = api('http://localhost:18080/v1/statement', query, 'POST', headers)
    rows, columns = [], []
    while True:
        if 'columns' in page:
            columns = page['columns']
        rows.extend(page.get('data', []))
        if 'error' in page:
            error = {'sql': query, 'response': page, 'wall_seconds': time.perf_counter()-started}
            save('error_' + page.get('id', str(time.time())) + '.json', error)
            raise RuntimeError(page['error']['message'])
        if 'nextUri' not in page:
            break
        next_url = 'http://localhost:18080' + urlsplit(page['nextUri']).path
        page = api(next_url)
    result = {'label': label, 'sql': query, 'session':session, 'query_id': page['id'], 'utc': datetime.now(timezone.utc).isoformat(), 'wall_seconds': time.perf_counter()-started, 'stats': page.get('stats'), 'columns': columns, 'rows': rows}
    with (RESULTS / 'queries.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(result, ensure_ascii=False) + '\n')
    print(f"{label or query[:65]}: {result['wall_seconds']:.3f}s, {len(rows)} result rows", flush=True)
    return result

def q16(prefix):
    return f"""SELECT p.brand, p.type, p.size, count(DISTINCT ps.suppkey) AS supplier_cnt
FROM {prefix}.partsupp ps JOIN {prefix}.part p ON p.partkey=ps.partkey
WHERE p.brand <> 'Brand#45' AND p.type NOT LIKE 'MEDIUM POLISHED%'
AND p.size IN (49,14,23,45,19,3,36,9)
AND ps.suppkey NOT IN (SELECT suppkey FROM {prefix}.supplier WHERE comment LIKE '%Customer%Complaints%')
GROUP BY p.brand,p.type,p.size ORDER BY supplier_cnt DESC,p.brand,p.type,p.size"""

def table_stats(name):
    data = sql(f'SELECT count(*), sum(record_count), sum(file_size_in_bytes), min(file_path) FROM {name.rsplit(".",1)[0]}."{name.rsplit(".",1)[1]}$files"', 'file_stats_' + name)
    return dict(zip(['files', 'rows', 'data_bytes', 'first_data_file'], data['rows'][0]))

def init():
    if platform.system()=='Windows':
        subprocess.run(['powershell','-NoProfile','-File',str(ROOT/'scripts'/'Capture-System.ps1')],check=True)
    for layer in ['bronze', 'silver', 'gold', 'bench']:
        docker('exec', '-T', 'storage', 'mcli', 'alias', 'set', 'lab', 'http://127.0.0.1:9000', 'minioadmin', 'pr1-local-storage-2026')
        docker('exec', '-T', 'storage', 'mcli', 'mb', '--ignore-existing', 'lab/' + layer)
    try:
        api('http://localhost:18181/management/v1/bootstrap', {'accept-terms-of-use': True}, 'POST', {'Content-Type': 'application/json'})
    except urllib.error.HTTPError as exc:
        message = exc.read().decode()
        if 'AlreadyBootstrapped' not in message and 'not open for bootstrap' not in message:
            raise RuntimeError(message)
    for layer in ['bronze', 'silver', 'gold', 'bench']:
        payload = {'warehouse-name': layer, 'project-id': '00000000-0000-0000-0000-000000000000', 'storage-profile': {'type':'s3','bucket':layer,'key-prefix':'','endpoint':'http://storage:9000','region':'us-east-1','path-style-access':True,'flavor':'s3-compat','sts-enabled':False}, 'storage-credential': {'type':'s3','credential-type':'access-key','access-key-id':'minioadmin','secret-access-key':'pr1-local-storage-2026'}}
        try:
            api('http://localhost:18181/management/v1/warehouse', payload, 'POST', {'Content-Type': 'application/json'})
        except urllib.error.HTTPError as exc:
            message = exc.read().decode()
            if 'overlap' not in message.lower() and 'already' not in message.lower():
                raise RuntimeError(message)
    wait_engine()
    for layer in ['bronze', 'silver', 'gold', 'bench']:
        sql(f'CREATE SCHEMA IF NOT EXISTS {layer}.lakehouse')
    for key, _, _ in CONFIGS[1:]:
        sql(f'CREATE SCHEMA IF NOT EXISTS bench.{key}')
    sql('SHOW CATALOGS', 'catalogs')
    environment()

def environment():
    container_ids=docker('ps','--all','--quiet').split()
    containers=json.loads(subprocess.check_output(['docker','inspect',*container_ids],text=True))
    image_ids=sorted({container['Image'] for container in containers})
    inspected=json.loads(subprocess.check_output(['docker','image','inspect',*image_ids],text=True))
    save('environment.json', {'captured_utc':datetime.now(timezone.utc).isoformat(),'platform':platform.platform(), 'scale': SF, 'docker_ps': docker('ps', '--all'), 'images': [{k:image.get(k) for k in ['Id','RepoTags','RepoDigests','Architecture','Os']} for image in inspected]})

def materialize(key, fmt, codec, tables):
    prefix = 'bronze.lakehouse' if key=='parquet_zstd' else 'bench.'+key
    previous = json.loads((RESULTS/'writes.json').read_text()) if (RESULTS/'writes.json').exists() else []
    existing = {r[0] for r in sql(f'SHOW TABLES FROM {prefix}')['rows']}
    for table in tables:
        if table in existing:
            print('Already materialized:', prefix+'.'+table, flush=True)
            continue
        query = f"CREATE TABLE {prefix}.{table} WITH (format='{fmt}') AS SELECT * FROM tpch.{SF}.{table}"
        result=sql(query, 'write_'+key+'_'+table, session=f"{prefix.split('.')[0]}.compression_codec={codec}")
        entry={'config':key,'format':fmt,'codec_setting':codec,'table':table,'query_id':result['query_id'],'wall_seconds':result['wall_seconds'],'stats':result['stats'],**table_stats(prefix+'.'+table)}
        previous=[row for row in previous if (row['config'],row['table'])!=(key,table)]
        previous.append(entry)
        save('writes.json', previous)

def core():
    materialize(*CONFIGS[0], TABLES[:3])
    statements = [
        "CREATE TABLE silver.lakehouse.eligible_parts WITH (format='PARQUET') AS SELECT partkey, trim(brand) brand, trim(type) type, size FROM bronze.lakehouse.part WHERE partkey IS NOT NULL AND brand IS NOT NULL AND type IS NOT NULL AND size>0 AND brand<>'Brand#45' AND type NOT LIKE 'MEDIUM POLISHED%' AND size IN (49,14,23,45,19,3,36,9)",
        "CREATE TABLE silver.lakehouse.part_suppliers WITH (format='PARQUET') AS SELECT ps.partkey,ps.suppkey,p.brand,p.type,p.size FROM bronze.lakehouse.partsupp ps JOIN silver.lakehouse.eligible_parts p ON p.partkey=ps.partkey JOIN bronze.lakehouse.supplier s ON s.suppkey=ps.suppkey WHERE s.comment NOT LIKE '%Customer%Complaints%'",
        "CREATE TABLE gold.lakehouse.supplier_counts WITH (format='PARQUET') AS SELECT brand,type,size,count(DISTINCT suppkey) supplier_cnt FROM silver.lakehouse.part_suppliers GROUP BY brand,type,size",
    ]
    for query in statements:
        sql(query.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ',1))
    source = q16('tpch.'+SF)
    bronze = q16('bronze.lakehouse')
    gold = 'SELECT brand,type,size,supplier_cnt FROM gold.lakehouse.supplier_counts ORDER BY supplier_cnt DESC,brand,type,size'
    a,b,c=sql(source,'reference_q16'),sql(bronze,'bronze_q16'),sql(gold,'gold_q16')
    assert a['rows']==b['rows']==c['rows'], 'Q16 source/Bronze/Gold mismatch'
    save('gold.json',c)
    with (RESULTS/'gold.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.writer(stream);writer.writerow([x['name'] for x in c['columns']]);writer.writerows(c['rows'])
    checks={
        'supplier_null_or_invalid_key': 'SELECT count(*) FROM bronze.lakehouse.supplier WHERE suppkey IS NULL OR suppkey<=0',
        'part_null_or_invalid_key': 'SELECT count(*) FROM bronze.lakehouse.part WHERE partkey IS NULL OR partkey<=0',
        'supplier_duplicate_keys': 'SELECT count(*) FROM (SELECT suppkey FROM bronze.lakehouse.supplier GROUP BY suppkey HAVING count(*)>1)',
        'part_duplicate_keys': 'SELECT count(*) FROM (SELECT partkey FROM bronze.lakehouse.part GROUP BY partkey HAVING count(*)>1)',
        'partsupp_duplicate_pairs': 'SELECT count(*) FROM (SELECT partkey,suppkey FROM bronze.lakehouse.partsupp GROUP BY partkey,suppkey HAVING count(*)>1)',
        'orphan_part': 'SELECT count(*) FROM bronze.lakehouse.partsupp ps LEFT JOIN bronze.lakehouse.part p ON p.partkey=ps.partkey WHERE p.partkey IS NULL',
        'orphan_supplier': 'SELECT count(*) FROM bronze.lakehouse.partsupp ps LEFT JOIN bronze.lakehouse.supplier s ON s.suppkey=ps.suppkey WHERE s.suppkey IS NULL',
        'complaint_supplier_in_silver': "SELECT count(*) FROM silver.lakehouse.part_suppliers ps JOIN bronze.lakehouse.supplier s ON s.suppkey=ps.suppkey WHERE s.comment LIKE '%Customer%Complaints%'",
    }
    checked={name:sql(query,name)['rows'][0][0] for name,query in checks.items()}
    assert all(v==0 for v in checked.values()), checked
    checked['reference_bronze_gold_equal']=True
    checked['gold_rows']=len(c['rows'])
    checked['gold_sha256']=hashlib.sha256(json.dumps(c['rows'],ensure_ascii=False).encode()).hexdigest()
    save('checks.json',checked)
    save('layers.json',{name:table_stats(name) for name in ['bronze.lakehouse.supplier','bronze.lakehouse.part','bronze.lakehouse.partsupp','silver.lakehouse.eligible_parts','silver.lakehouse.part_suppliers','gold.lakehouse.supplier_counts']})

def evolution():
    sql("CREATE TABLE silver.lakehouse.evolution_demo WITH (format='PARQUET') AS SELECT suppkey,name,comment FROM bronze.lakehouse.supplier WHERE suppkey<=3")
    before=sql('SELECT snapshot_id FROM silver.lakehouse."evolution_demo$snapshots" ORDER BY committed_at DESC LIMIT 1')['rows'][0][0]
    sql('ALTER TABLE silver.lakehouse.evolution_demo ADD COLUMN quality_note varchar')
    sql('ALTER TABLE silver.lakehouse.evolution_demo RENAME COLUMN quality_note TO review_note')
    old=sql('SELECT * FROM silver.lakehouse.evolution_demo ORDER BY suppkey','evolution_null_default')
    assert all(row[-1] is None for row in old['rows'])
    sql("UPDATE silver.lakehouse.evolution_demo SET review_note='checked' WHERE suppkey=1")
    historical=sql(f'SELECT suppkey,name,comment FROM silver.lakehouse.evolution_demo FOR VERSION AS OF {before} ORDER BY suppkey','time_travel_before_update')
    current=sql('SELECT * FROM silver.lakehouse.evolution_demo ORDER BY suppkey','time_travel_current')
    sql(f"ALTER TABLE silver.lakehouse.evolution_demo EXECUTE rollback_to_snapshot(snapshot_id=>{before})")
    restored=sql('SELECT suppkey,name,comment FROM silver.lakehouse.evolution_demo ORDER BY suppkey','time_travel_restored')
    assert restored['rows']==historical['rows']
    save('evolution.json',{'original_snapshot':before,'historical':historical,'changed':current,'restored':restored,'schema':sql('DESCRIBE silver.lakehouse.evolution_demo')})

def benchmark():
    save('benchmark_checks.json',{'all_configs_equal':False,'status':'running'})
    all_results=[]
    for key,fmt,codec in CONFIGS:
        materialize(key,fmt,codec,TABLES)
        prefix='bronze.lakehouse' if key=='parquet_zstd' else 'bench.'+key
        queries={'q16':q16(prefix), 'lineitem_aggregation':f'SELECT returnflag,linestatus,count(*) row_count,sum(quantity) quantity,sum(CAST(extendedprice AS decimal(15,2))*(DECIMAL \'1.00\'-CAST(discount AS decimal(4,2)))) revenue FROM {prefix}.lineitem GROUP BY returnflag,linestatus ORDER BY returnflag,linestatus'}
        for name,query in queries.items():
            docker('restart','trino')
            wait_engine(prefix)
            for run in range(4):
                res=sql(query,f'benchmark_{key}_{name}_{run}')
                all_results.append({'config':key,'query':name,'run':run,'phase':'after_trino_restart' if run==0 else 'repeat','query_id':res['query_id'],'wall_seconds':res['wall_seconds'],'stats':res['stats'],'rows':res['rows']})
                save('reads.json',all_results)
    for query in ['q16','lineitem_aggregation']:
        comparable=[row['rows'] for row in all_results if row['query']==query]
        assert all(rows==comparable[0] for rows in comparable),query+' differs across formats'
    save('benchmark_checks.json',{'all_configs_equal':True,'queries':['q16','lineitem_aggregation'],'configs':[x[0] for x in CONFIGS]})

def inventory():
    items=[]
    for bucket in ['bronze','silver','gold','bench']:
        data=docker('exec','-T','storage','mcli','ls','--recursive','--json','lab/'+bucket)
        for line in data.splitlines():
            row=json.loads(line)
            if row.get('type')=='file':
                items.append({'bucket':bucket,**row})
    save('storage_inventory.json',items)

def audit():
    audited={}
    for key,_,_ in CONFIGS:
        prefix='bronze.lakehouse' if key=='parquet_zstd' else 'bench.'+key
        for table in TABLES:
            name=prefix+'.'+table
            audited[name]={'count':sql(f'SELECT count(*) FROM {name}','audit_count_'+name)['rows'][0][0], 'schema':sql('DESCRIBE '+name,'audit_schema_'+name)['rows']}
    for table in ['silver.lakehouse.eligible_parts','silver.lakehouse.part_suppliers','gold.lakehouse.supplier_counts']:
        audited[table]={'count':sql('SELECT count(*) FROM '+table,'audit_count_'+table)['rows'][0][0], 'schema':sql('DESCRIBE '+table,'audit_schema_'+table)['rows']}
    for table in TABLES:
        counts=[audited[('bronze.lakehouse' if key=='parquet_zstd' else 'bench.'+key)+'.'+table]['count'] for key,_,_ in CONFIGS]
        assert len(set(counts))==1,(table,counts)
    save('audit.json',audited)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['init','core','evolution','benchmark','inventory','audit','environment','all'])
    action=parser.parse_args().action
    if action=='all':
        init();core();evolution();benchmark();audit();inventory()
    else:
        globals()[action]()
