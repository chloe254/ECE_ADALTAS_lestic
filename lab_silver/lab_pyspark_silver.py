# %% [markdown]
# Lab: Bronze to silver with Spark DataFrames
# Run from the repo root (LAB_BUCKET_NAME / KUBERNETES_NAMESPACE and AWS_* set by Onyxia):
#   spark-submit --driver-memory 1g \
#     --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
#     lab_silver/lab_pyspark_silver.py > lab_silver/output.txt
# Cells (# %%) can also be run one by one in VS Code or pasted in the pyspark shell.

# %% SparkSession with S3 access
import json
import os
import time
import timeit
import urllib.request

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Spark 4.1.1 / Scala 2.13 in this image -> runtime 4.1_2.13 (the 4.0 artifact of the lab page
# does not match this Spark version)
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

T0 = time.perf_counter()
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print(spark)
print("Spark", spark.version, "| session timezone:", spark.conf.get("spark.sql.session.timeZone"),
      "| startup s:", round(time.perf_counter() - T0, 1))


def title(t):
    print("\n" + "=" * 78 + "\n" + t + "\n" + "=" * 78)


def ui(path):
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())


def stage_ids():
    return {s["stageId"] for s in ui("stages")}


def show_new_stages(before):
    for s in sorted(ui("stages"), key=lambda s: s["stageId"]):
        if s["stageId"] not in before and s["status"] == "COMPLETE":
            print(f"    stage {s['stageId']:>3} | tasks={s['numTasks']:>3} | input={s['inputBytes']:>7} B "
                  f"({s['inputRecords']} rec) | shuffleRead={s['shuffleReadBytes']:>6} B | {s['name'][:60]}")


# %% Bronze layer
title("Bronze layer")
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
users_bronze.printSchema()
orders_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)
print("users_bronze:", users_bronze.count(), "| orders_bronze:", orders_bronze.count())

# %% Silver: users, DataFrame API
title("Silver users (DataFrame API)")
T_SILVER = time.perf_counter()
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)
stg_users.show(5, truncate=False)

# Same pitfall check on state / zip_code (not handled by the lab code)
print("empty strings left -> state:", stg_users.filter(F.col("state") == "").count(),
      "| zip_code:", stg_users.filter(F.col("zip_code") == "").count(),
      "| city NULL:", stg_users.filter(F.col("city").isNull()).count())

# %% Silver: orders, window deduplication
title("Silver orders (DataFrame API + window dedup)")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())

duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()
print("--- physical plan of stg_orders ---")
stg_orders.explain()

# %% Spark SQL
title("Same transformation with Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql('''
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
''')
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("asserts OK")
print("--- physical plan of stg_orders_sql ---")
stg_orders_sql.explain()

stg_users.createOrReplaceTempView("stg_users")
spark.sql('''
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
''').show(10)
spark.sql('''
    SELECT count(DISTINCT state) AS states, sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city,
           collect_set(CASE WHEN city IS NULL THEN state END) AS states_without_city
    FROM stg_users
''').show(truncate=False)

# %% Write Parquet
title("Write silver as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("silver build time (users+orders, transformations + Parquet write) s:",
      round(time.perf_counter() - T_SILVER, 1))

jvm = spark._jvm
hconf = sc._jsc.hadoopConfiguration()


def list_dir(path):
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(hconf)
    it = fs.listFiles(p, True)
    rows = []
    while it.hasNext():
        st = it.next()
        rows.append((str(st.getPath()).replace(f"s3a://{BUCKET}/", ""), st.getLen()))
    return rows


for d in ["silver/spark/users.parquet", "silver/spark/orders.parquet"]:
    files = list_dir(f"s3a://{BUCKET}/{d}")
    print(f"{d}: {len(files)} objects")
    for f, size in files:
        print(f"    {size:>8}  {f}")
print("stg_users partitions:", stg_users.rdd.getNumPartitions(),
      "| stg_orders partitions:", stg_orders.rdd.getNumPartitions())

users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet read-back asserts OK:", users_silver.count(), orders_silver.count())

# %% Iceberg
title("Write silver as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()

for f, size in list_dir(f"s3a://{BUCKET}/warehouse/silver/orders"):
    print(f"    {size:>8}  {f}")

spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)

users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("Iceberg read-back asserts OK:", users_iceberg.count(), orders_iceberg.count())

# %% Exercise 1: birthdate_is_valid
title("Exercise 1: birthdate_is_valid (DataFrame vs SQL)")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
stg_users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        # users without any order are considered valid, like in the dbt lab
        F.coalesce(F.col("birthdate") <= F.to_date("first_ordered_at"), F.lit(True)),
    )
    .drop("first_ordered_at")
)

stg_orders.createOrReplaceTempView("stg_orders")
stg_users_valid_sql = spark.sql("""
    WITH first_orders AS (
        SELECT user_id, min(ordered_at) AS first_ordered_at
        FROM stg_orders
        GROUP BY user_id
    )
    SELECT u.*,
           coalesce(u.birthdate <= to_date(f.first_ordered_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN first_orders f ON u.user_id = f.user_id
""")
diff = stg_users_valid_df.exceptAll(stg_users_valid_sql).count() + \
    stg_users_valid_sql.exceptAll(stg_users_valid_df).count()
print("rows differing between DataFrame and SQL versions:", diff)
stg_users_valid_df.groupBy("birthdate_is_valid").count().show()
stg_users_valid_df.filter(~F.col("birthdate_is_valid")) \
    .join(first_orders, "user_id").select("username", "birthdate", "first_ordered_at").show(5, truncate=False)
# Export the invalid user ids, to compare with dbt in DuckDB
stg_users_valid_df.select("user_id", "birthdate_is_valid") \
    .write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users_birthdate_check.parquet")

# %% Exercise 2: UDF vs pandas_udf vs regexp_extract
title("Exercise 2: Python UDF vs pandas_udf vs regexp_extract")
import re

from pyspark.sql.functions import pandas_udf, udf

ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@udf("string")
def zip_udf(address):
    if address is None:
        return None
    lines = address.split("\n")
    m = ZIP_RE.search(lines[1]) if len(lines) > 1 else None
    return m.group(2) if m else ""


@pandas_udf("string")
def zip_pandas_udf(address: pd.Series) -> pd.Series:
    return address.str.split("\n").str[1].str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")


zip_native = F.regexp_extract(F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2)

users_cached = users_bronze.cache()
users_cached.count()  # materialize so S3 + CSV parsing is not timed

variants = {
    "regexp_extract": zip_native,
    "python udf": zip_udf("address"),
    "pandas_udf": zip_pandas_udf("address"),
}
ref = sorted(r[0] for r in users_cached.select(zip_native).collect())
for name, col in variants.items():
    same = sorted(r[0] for r in users_cached.select(col).collect()) == ref
    # equivalent of %%timeit: best of 5 repeats of 3 runs, noop sink forces full evaluation
    runs = timeit.repeat(lambda: users_cached.select(col.alias("z")).write.format("noop").mode("overwrite").save(),
                         number=3, repeat=5)
    print(f"  {name:<15} best {min(runs) / 3 * 1000:7.1f} ms/run | same result as regexp_extract: {same}")
users_cached.unpersist()

# %% Exercise 3: cache the bronze DataFrames
title("Exercise 3: cache bronze and look at later stages")


def silver_counts():
    return stg_users.count(), stg_orders.count()


for label in ["no cache, run 1", "no cache, run 2"]:
    before = stage_ids()
    t = time.perf_counter()
    silver_counts()
    print(f"  {label}: {round((time.perf_counter() - t) * 1000)} ms")
    show_new_stages(before)

users_bronze.cache()
orders_bronze.cache()
users_bronze.count(); orders_bronze.count()   # first action fills the cache
for label in ["cache, run 1", "cache, run 2"]:
    before = stage_ids()
    t = time.perf_counter()
    silver_counts()
    print(f"  {label}: {round((time.perf_counter() - t) * 1000)} ms")
    show_new_stages(before)
print("  Storage tab:", [(r["name"][:50], r["numCachedPartitions"], r["memoryUsed"]) for r in ui("storage/rdd")])
users_bronze.unpersist(); orders_bronze.unpersist()

# %% Exercise 4: Iceberg append + time travel, vs Parquet directory
title("Exercise 4: append + VERSION AS OF (Iceberg) vs Parquet directory")
snap_before = spark.sql(
    "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 1"
).first()[0]
new_order = spark.createDataFrame(
    [("00000000-0000-0000-0000-000000000001", stg_users.first()["user_id"], 2, "croissant")],
    "order_id string, user_id string, quantity int, product string",
).select("order_id", "user_id", F.current_timestamp().alias("ordered_at"), "quantity", "product")

new_order.writeTo("lab.silver.orders").append()
spark.sql("SELECT snapshot_id, parent_id, operation, committed_at FROM lab.silver.orders.snapshots "
          "ORDER BY committed_at").show(truncate=False)
print("current rows:", spark.table("lab.silver.orders").count())
print(f"VERSION AS OF {snap_before}:",
      spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {snap_before}").first()[0])
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM lab.silver.orders.history").show(truncate=False)

# Parquet: same append on a copy of the directory
pq = f"s3a://{BUCKET}/silver/spark/orders_append_demo.parquet"
stg_orders.write.mode("overwrite").parquet(pq)
print("parquet before append:", spark.read.parquet(pq).count())
new_order.write.mode("append").parquet(pq)
print("parquet after append :", spark.read.parquet(pq).count(),
      "| objects:", len(list_dir(pq)), "(no snapshot, no history: the previous state is gone)")
try:
    spark.sql(f"SELECT count(*) FROM parquet.`{pq}` VERSION AS OF 1").show()
except Exception as e:
    print("VERSION AS OF on Parquet ->", type(e).__name__, str(e).split("\n")[0][:150])

print("\nTOTAL runtime s:", round(time.perf_counter() - T0, 1))

# %% Cleanup (session only: S3 cleanup is a separate, explicit bash step)
spark.stop()
