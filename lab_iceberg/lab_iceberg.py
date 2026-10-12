# %% [markdown]
# Lab: Explore Iceberg tables with Spark
# Input = silver layer of the Spark DataFrame lab (silver/spark/*.parquet).
# Run from the repo root (AWS_* and KUBERNETES_NAMESPACE set by Onyxia):
#   spark-submit --driver-memory 2g \
#     --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
#     lab_iceberg/lab_iceberg.py > lab_iceberg/output.txt
# S3 cleanup (rm warehouse/) is NOT done on purpose: instructor's request.

# %% SparkSession with S3 + Iceberg catalog "lab"
import json
import os
from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def title(text):
    print("\n" + "=" * 78 + "\n" + text + "\n" + "=" * 78)


def list_s3(prefix):
    """Recursive listing through the Hadoop S3A filesystem: [(key, size)]."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/{prefix}")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    it = fs.listFiles(path, True)
    out = []
    while it.hasNext():
        st = it.next()
        out.append((st.getPath().toString().split(f"{BUCKET}/", 1)[1], st.getLen()))
    return sorted(out)


def kind(key):
    name = key.rsplit("/", 1)[-1]
    if "/data/" in key and name.endswith(".parquet"):
        return "data file (Parquet)"
    if name.endswith(".parquet"):
        return "Parquet part-file"
    if name == "_SUCCESS":
        return "_SUCCESS marker"
    if name.endswith(".metadata.json"):
        return "table metadata (vN.metadata.json)"
    if name.startswith("snap-") and name.endswith(".avro"):
        return "manifest list (snap-*.avro)"
    if name.endswith(".avro"):
        return "manifest (*-mN.avro)"
    if name == "version-hint.text":
        return "version-hint.text"
    return "other"


def show_tree(prefix):
    files = list_s3(prefix)
    for key, size in files:
        print(f"  {size:>9}  {key}")
    print("  -> by kind:", dict(Counter(kind(k) for k, _ in files)))
    return files


def data_files(prefix):
    return {k for k, _ in list_s3(prefix) if kind(k) == "data file (Parquet)"}


def snapshots(table):
    spark.sql(f"""
        SELECT committed_at, snapshot_id, parent_id, operation,
               summary['added-records'] AS added_records,
               summary['total-records'] AS total_records,
               summary['total-data-files'] AS total_data_files
        FROM {table}.snapshots ORDER BY committed_at
    """).show(truncate=False)


def current_schema(table):
    """Read the newest vN.metadata.json and print the schemas it records."""
    last = spark.sql(f"SELECT file FROM {table}.metadata_log_entries "
                     "ORDER BY timestamp DESC LIMIT 1").first()[0]
    meta = json.loads(spark.read.text(last, wholetext=True).first()[0])
    print("  newest metadata file:", last.split(f"{BUCKET}/", 1)[1])
    print("  current-schema-id   :", meta["current-schema-id"])
    for s in meta["schemas"]:
        print(f"  schema {s['schema-id']}:", [f["name"] for f in s["fields"]])


print("Spark", spark.version, "| Iceberg runtime", ICEBERG_PACKAGE.split(":")[-1], "| bucket", BUCKET)
spark.sql("SHOW NAMESPACES IN lab").show()

# %% Inputs: silver DataFrames of the previous lab
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
stg_users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
print("stg_orders:", stg_orders.count(), "rows | stg_users:", stg_users.count(), "rows")
stg_orders.printSchema()

# %% 1. Create the Iceberg table (fresh: the previous lab left 3 snapshots on it)
title("1. Create the Iceberg table lab.silver.orders")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
spark.sql("DROP TABLE IF EXISTS lab.silver.orders PURGE")
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

# %% 2. Inspect the files
title("2. Files under warehouse/silver/orders/")
show_tree("warehouse/silver/orders/")

# %% 3. Iceberg metadata tables
title("3. Metadata tables: snapshots and files")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("""SELECT file_path, file_format, record_count, file_size_in_bytes
             FROM lab.silver.orders.files""").show(truncate=False)
print("snapshots:", spark.table("lab.silver.orders.snapshots").count(),
      "| data files in current table:", spark.table("lab.silver.orders.files").count())

# %% 4. Second snapshot: append 3 rows with new order ids
title("4. Second snapshot (append)")
files_before = data_files("warehouse/silver/orders/")
orders = spark.table("lab.silver.orders")
orders.show(5, truncate=False)
new_rows = orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_rows.writeTo("lab.silver.orders").append()
snapshots("lab.silver.orders")
print("rows now:", spark.table("lab.silver.orders").count())
files_after = data_files("warehouse/silver/orders/")
print("original data files still on S3:", files_before <= files_after,
      "| data files before/after:", len(files_before), "/", len(files_after))

# %% 5. Time travel
title("5. Time travel")
snap_ids = [r.snapshot_id for r in spark.sql(
    "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()]
print("snapshot ids (oldest first):", snap_ids)
print(f"VERSION AS OF {snap_ids[0]}:",
      spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {snap_ids[0]}").first()[0])
print("current table          :", spark.sql("SELECT count(*) FROM lab.silver.orders").first()[0])
try:
    print("data files read by the first snapshot:",
          spark.sql(f"SELECT count(*) FROM lab.silver.orders.files VERSION AS OF {snap_ids[0]}").first()[0],
          "| by the current one:", spark.table("lab.silver.orders.files").count())
except Exception as e:  # metadata-table time travel depends on the Spark/Iceberg versions
    print("files VERSION AS OF not supported here:", str(e).splitlines()[0][:150])

# %% 6. Schema evolution
title("6. Schema evolution: ADD COLUMN source")
files_before = data_files("warehouse/silver/orders/")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show(truncate=False)
snapshots("lab.silver.orders")  # ALTER TABLE adds no snapshot: it is a metadata-only change
files_mid = data_files("warehouse/silver/orders/")
print("data files rewritten by ALTER TABLE:", len(files_before - files_mid),
      "| new data files:", len(files_mid - files_before))
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark' AS source
    FROM lab.silver.orders LIMIT 1
""")
spark.sql("SELECT source, count(*) AS n FROM lab.silver.orders GROUP BY source ORDER BY source").show()
files_after = data_files("warehouse/silver/orders/")
print("old data files kept after INSERT:", files_before <= files_after,
      "| data files:", len(files_after))
spark.sql("""SELECT timestamp, latest_snapshot_id, latest_schema_id
             FROM lab.silver.orders.metadata_log_entries ORDER BY timestamp""").show(truncate=False)
current_schema("lab.silver.orders")

# %% 7. Iceberg vs plain Parquet directory
title("7. Parquet directory vs Iceberg table")
print("silver/spark/orders.parquet/ (plain Parquet):")
show_tree("silver/spark/orders.parquet/")
print("warehouse/silver/orders/ (Iceberg):")
show_tree("warehouse/silver/orders/")

# %% Exercise 1: users as an Iceberg table
title("Exercise 1: lab.silver.users from stg_users")
spark.sql("DROP TABLE IF EXISTS lab.silver.users PURGE")
stg_users.writeTo("lab.silver.users").createOrReplace()
print("users:", spark.table("lab.silver.users").count())

# %% Exercise 2: append a new batch of users
title("Exercise 2: append a batch of 5 users")
batch = (stg_users.orderBy("username").limit(5)
         .withColumn("user_id", F.expr("uuid()"))
         .withColumn("username", F.concat(F.col("username"), F.lit("_b2"))))
batch.writeTo("lab.silver.users").append()
snapshots("lab.silver.users")

# %% Exercise 3: time travel on users
title("Exercise 3: users before / after the append")
u_snaps = [r.snapshot_id for r in spark.sql(
    "SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()]
print(f"VERSION AS OF {u_snaps[0]} (before):",
      spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {u_snaps[0]}").first()[0])
print(f"VERSION AS OF {u_snaps[-1]} (after) :",
      spark.sql(f"SELECT count(*) FROM lab.silver.users VERSION AS OF {u_snaps[-1]}").first()[0])
spark.sql(f"""
    SELECT username FROM lab.silver.users VERSION AS OF {u_snaps[-1]}
    EXCEPT
    SELECT username FROM lab.silver.users VERSION AS OF {u_snaps[0]}
""").show()

# %% Exercise 4: schema evolution on users (country)
title("Exercise 4: ADD COLUMN country, write rows with the new schema")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
cols = spark.table("lab.silver.users").columns
new_users = (stg_users.orderBy(F.col("username").desc()).limit(3)
             .withColumn("user_id", F.expr("uuid()"))
             .withColumn("username", F.concat(F.col("username"), F.lit("_b3")))
             .withColumn("country", F.lit("US"))   # every address is a US one (state + zip)
             .select(*cols))
new_users.writeTo("lab.silver.users").append()
spark.sql("SELECT country, count(*) AS n FROM lab.silver.users GROUP BY country ORDER BY country").show()
snapshots("lab.silver.users")
current_schema("lab.silver.users")

# %% Exercise 5: S3 layout of users vs orders
title("Exercise 5: S3 layout, users vs orders")
for t in ["users", "orders"]:
    files = list_s3(f"warehouse/silver/{t}/")
    print(f"  {t:<6}:", dict(Counter(kind(k) for k, _ in files)),
          "| bytes:", sum(s for _, s in files))

# %% Session only: S3 cleanup intentionally skipped
spark.stop()
