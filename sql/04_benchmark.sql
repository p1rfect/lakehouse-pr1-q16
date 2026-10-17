-- orc_gzip
SET SESSION bench.compression_codec='GZIP';
CREATE TABLE bench.orc_gzip.part WITH (format='ORC') AS SELECT * FROM tpch.sf50.part;

-- parquet_gzip
SET SESSION bench.compression_codec='GZIP';
CREATE TABLE bench.parquet_gzip.part WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.part;

-- orc_zstd
SET SESSION bench.compression_codec='ZSTD';
CREATE TABLE bench.orc_zstd.part WITH (format='ORC') AS SELECT * FROM tpch.sf50.part;
-- Полный перебор восьми таблиц и четыре чтения: python scripts/lab.py benchmark
