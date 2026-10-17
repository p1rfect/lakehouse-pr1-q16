CREATE TABLE bronze.lakehouse.supplier WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.supplier;

CREATE TABLE bronze.lakehouse.part WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.part;

CREATE TABLE bronze.lakehouse.partsupp WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.partsupp;

CREATE TABLE bronze.lakehouse.region WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.region;

CREATE TABLE bronze.lakehouse.nation WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.nation;

CREATE TABLE bronze.lakehouse.customer WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.customer;

CREATE TABLE bronze.lakehouse.orders WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.orders;

CREATE TABLE bronze.lakehouse.lineitem WITH (format='PARQUET') AS SELECT * FROM tpch.sf50.lineitem;
