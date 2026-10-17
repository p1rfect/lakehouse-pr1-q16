"""Export the SQL used by lab.py as convenient readable files."""
from pathlib import Path
from lab import q16, TABLES, SF, CONFIGS, ROOT

out=ROOT/'sql'
out.mkdir(exist_ok=True)
(out/'01_bronze.sql').write_text('\n\n'.join(f"CREATE TABLE bronze.lakehouse.{table} WITH (format='PARQUET') AS SELECT * FROM tpch.{SF}.{table};" for table in TABLES)+'\n',encoding='utf-8')
(out/'02_silver_gold.sql').write_text("""-- Вариант 4, Q16. Выполнять после 01_bronze.sql.
CREATE TABLE silver.lakehouse.eligible_parts
WITH (format='PARQUET') AS
SELECT partkey, trim(brand) brand, trim(type) type, size
FROM bronze.lakehouse.part
WHERE partkey IS NOT NULL AND brand IS NOT NULL AND type IS NOT NULL AND size>0
  AND brand<>'Brand#45' AND type NOT LIKE 'MEDIUM POLISHED%'
  AND size IN (49,14,23,45,19,3,36,9);

CREATE TABLE silver.lakehouse.part_suppliers
WITH (format='PARQUET') AS
SELECT ps.partkey,ps.suppkey,p.brand,p.type,p.size
FROM bronze.lakehouse.partsupp ps
JOIN silver.lakehouse.eligible_parts p ON p.partkey=ps.partkey
JOIN bronze.lakehouse.supplier s ON s.suppkey=ps.suppkey
WHERE s.comment NOT LIKE '%Customer%Complaints%';

CREATE TABLE gold.lakehouse.supplier_counts
WITH (format='PARQUET') AS
SELECT brand,type,size,count(DISTINCT suppkey) supplier_cnt
FROM silver.lakehouse.part_suppliers
GROUP BY brand,type,size;

SELECT * FROM gold.lakehouse.supplier_counts
ORDER BY supplier_cnt DESC,brand,type,size;
""",encoding='utf-8')
(out/'03_reference_q16.sql').write_text(q16('tpch.'+SF)+';\n\n'+q16('bronze.lakehouse')+';\n',encoding='utf-8')
(out/'04_benchmark.sql').write_text('\n\n'.join(f"-- {key}\nSET SESSION bench.compression_codec='{codec}';\nCREATE TABLE bench.{key}.part WITH (format='{fmt}') AS SELECT * FROM tpch.{SF}.part;" for key,fmt,codec in CONFIGS[1:])+'\n-- Полный перебор восьми таблиц и четыре чтения: python scripts/lab.py benchmark\n',encoding='utf-8')
print('SQL files exported')
