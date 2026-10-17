"""Additional processing engine: read Iceberg Bronze through Lakekeeper and S3."""
import json
import time
from pathlib import Path
from pyspark.sql import SparkSession

builder=SparkSession.builder.appName('PR1-Balanov-Bronze').master('local[2]')
for key,value in {
    'spark.sql.catalog.bronze':'org.apache.iceberg.spark.SparkCatalog',
    'spark.sql.catalog.bronze.type':'rest',
    'spark.sql.catalog.bronze.uri':'http://catalog:8181/catalog',
    'spark.sql.catalog.bronze.warehouse':'bronze',
    'spark.sql.catalog.bronze.io-impl':'org.apache.iceberg.aws.s3.S3FileIO',
    'spark.sql.catalog.bronze.s3.endpoint':'http://storage:9000',
    'spark.sql.catalog.bronze.s3.path-style-access':'true',
    'spark.sql.catalog.bronze.s3.access-key-id':'minioadmin',
    'spark.sql.catalog.bronze.s3.secret-access-key':'pr1-local-storage-2026',
    'spark.sql.catalog.bronze.client.region':'us-east-1',
    'spark.sql.shuffle.partitions':'8',
    'spark.ui.enabled':'false',
}.items():
    builder=builder.config(key,value)
spark=builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
queries={
    'tables':'SHOW TABLES IN bronze.lakehouse',
    'sample':'SELECT suppkey,name,nationkey FROM bronze.lakehouse.supplier ORDER BY suppkey LIMIT 5',
    'aggregation':'SELECT brand,count(*) parts,avg(retailprice) avg_retailprice FROM bronze.lakehouse.part GROUP BY brand ORDER BY brand',
}
results={'spark_version':spark.version,'source':'Iceberg REST + S3FileIO; original Bronze files, no export/import','queries':{}}
for name,query in queries.items():
    started=time.perf_counter()
    frame=spark.sql(query)
    rows=[row.asDict() for row in frame.collect()]
    results['queries'][name]={'sql':query,'wall_seconds':time.perf_counter()-started,'rows':rows}
    print(name,json.dumps(rows,ensure_ascii=False),flush=True)
Path('/lab/results/spark_bronze.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
spark.stop()
