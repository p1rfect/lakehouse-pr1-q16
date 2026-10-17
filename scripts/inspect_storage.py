"""Optional physical-file inspection. pip install pyarrow boto3."""
import json
from pathlib import Path
from urllib.parse import urlsplit
import boto3
from botocore.config import Config
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pyarrow.orc as orc

ROOT=Path(__file__).resolve().parents[1]
results=ROOT/'results'
writes=json.loads((results/'writes.json').read_text())
layers=json.loads((results/'layers.json').read_text())
for name,entry in layers.items():
    if not name.startswith('bronze.'):
        writes.append({'config':name.split('.')[0],'table':name.split('.')[-1],'format':'PARQUET','codec_setting':'ZSTD',**entry})
s3=boto3.client('s3',endpoint_url='http://localhost:19000',aws_access_key_id='minioadmin',aws_secret_access_key='pr1-local-storage-2026',region_name='us-east-1',config=Config(s3={'addressing_style':'path'}))
arrow=fs.S3FileSystem(access_key='minioadmin',secret_key='pr1-local-storage-2026',region='us-east-1',scheme='http',endpoint_override='localhost:19000')
summaries=[]
for entry in writes:
    path=entry['first_data_file']
    parsed=urlsplit(path)
    table_prefix=parsed.path.lstrip('/').rsplit('/data/',1)[0]+'/'
    objects=[]
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=parsed.netloc,Prefix=table_prefix):
        objects.extend(page.get('Contents',[]))
    data=[o for o in objects if '/data/' in o['Key']]
    metadata=[o for o in objects if '/metadata/' in o['Key']]
    row={'config':entry['config'],'table':entry['table'],'all_object_bytes':sum(o['Size'] for o in objects),'data_object_bytes':sum(o['Size'] for o in data),'iceberg_metadata_bytes':sum(o['Size'] for o in metadata),'iceberg_metadata_objects':len(metadata),'data_objects':len(data),'sample_file':path}
    with arrow.open_input_file(parsed.netloc+parsed.path) as stream:
        if entry['format']=='PARQUET':
            f=pq.ParquetFile(stream)
            row.update({'actual_codec':f.metadata.row_group(0).column(0).compression,'sample_rows':f.metadata.num_rows,'sample_row_groups':f.metadata.num_row_groups,'sample_footer_bytes':f.metadata.serialized_size})
        else:
            f=orc.ORCFile(stream)
            row.update({'actual_codec':f.compression,'sample_rows':f.nrows,'sample_stripes':f.nstripes,'sample_footer_bytes':f.file_footer_length,'sample_stripe_statistics_bytes':f.stripe_statistics_length,'sample_postscript_bytes':f.file_postscript_length})
    assert row['data_object_bytes']==entry['data_bytes'], row
    expected='ZLIB' if entry['format']=='ORC' and entry['codec_setting']=='GZIP' else entry['codec_setting']
    assert row['actual_codec'].upper()==expected,(entry['config'],entry['table'],row['actual_codec'],expected)
    summaries.append(row)
    print(entry['config'],entry['table'],row['actual_codec'],flush=True)
(results/'physical_files.json').write_text(json.dumps(summaries,ensure_ascii=False,indent=2),encoding='utf-8')
