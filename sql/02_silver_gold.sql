-- Вариант 4, Q16. Выполнять после 01_bronze.sql.
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
