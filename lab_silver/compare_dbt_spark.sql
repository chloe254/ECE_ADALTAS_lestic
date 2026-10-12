-- Compare dbt (DuckDB) and Spark silver outputs. Run: duckdb < lab_silver/compare_dbt_spark.sql
.mode box
SELECT 'orders' AS model,
  (SELECT count(*) FROM 's3://user-c-lestic-ece/silver/dbt/stg_orders.parquet') AS dbt_rows,
  (SELECT count(*) FROM 's3://user-c-lestic-ece/silver/spark/orders.parquet/*.parquet') AS spark_rows,
  (SELECT sum(quantity) FROM 's3://user-c-lestic-ece/silver/dbt/stg_orders.parquet') AS dbt_qty,
  (SELECT sum(quantity) FROM 's3://user-c-lestic-ece/silver/spark/orders.parquet/*.parquet') AS spark_qty;
SELECT 'users' AS model,
  (SELECT count(*) FROM 's3://user-c-lestic-ece/silver/dbt/stg_users.parquet') AS dbt_rows,
  (SELECT count(*) FROM 's3://user-c-lestic-ece/silver/spark/users.parquet/*.parquet') AS spark_rows,
  (SELECT count(*) FILTER (WHERE city IS NULL) FROM 's3://user-c-lestic-ece/silver/dbt/stg_users.parquet') AS dbt_city_null,
  (SELECT count(*) FILTER (WHERE city IS NULL) FROM 's3://user-c-lestic-ece/silver/spark/users.parquet/*.parquet') AS spark_city_null;
SELECT d.column_name, d.column_type AS dbt_type, s.column_type AS spark_type
FROM (DESCRIBE SELECT * FROM 's3://user-c-lestic-ece/silver/dbt/stg_orders.parquet') d
FULL JOIN (DESCRIBE SELECT * FROM 's3://user-c-lestic-ece/silver/spark/orders.parquet/*.parquet') s USING (column_name);
SELECT d.column_name, d.column_type AS dbt_type, s.column_type AS spark_type
FROM (DESCRIBE SELECT * FROM 's3://user-c-lestic-ece/silver/dbt/stg_users.parquet') d
FULL JOIN (DESCRIBE SELECT * FROM 's3://user-c-lestic-ece/silver/spark/users.parquet/*.parquet') s USING (column_name);
-- Row-level diff on orders (both directions)
SELECT count(*) AS orders_rows_only_in_one_side FROM (
  (SELECT order_id::VARCHAR, user_id::VARCHAR, ordered_at::TIMESTAMP, quantity::INT, product FROM 's3://user-c-lestic-ece/silver/dbt/stg_orders.parquet'
   EXCEPT SELECT order_id::VARCHAR, user_id::VARCHAR, ordered_at::TIMESTAMP, quantity::INT, product FROM 's3://user-c-lestic-ece/silver/spark/orders.parquet/*.parquet')
  UNION ALL
  (SELECT order_id::VARCHAR, user_id::VARCHAR, ordered_at::TIMESTAMP, quantity::INT, product FROM 's3://user-c-lestic-ece/silver/spark/orders.parquet/*.parquet'
   EXCEPT SELECT order_id::VARCHAR, user_id::VARCHAR, ordered_at::TIMESTAMP, quantity::INT, product FROM 's3://user-c-lestic-ece/silver/dbt/stg_orders.parquet'));
-- Exercise 1: birthdate_is_valid, dbt column vs Spark export
SELECT
  (SELECT count(*) FILTER (WHERE NOT birthdate_is_valid) FROM 's3://user-c-lestic-ece/silver/dbt/stg_users.parquet') AS dbt_invalid,
  (SELECT count(*) FILTER (WHERE NOT birthdate_is_valid) FROM 's3://user-c-lestic-ece/silver/spark/users_birthdate_check.parquet/*.parquet') AS spark_invalid;
