# %% [markdown]
# Lab: Introduction to Spark's RDD API
# Run (from the repo root, users.csv / orders.csv downloaded):
#   spark-submit lab_rdd/lab_pyspark_rdd.py > lab_rdd/output.txt
# The session uses local[*]; the JVM is cgroup-aware, so on this 2 vCPU pod local[*] = 2 cores.
# Can also be run cell by cell (# %%) in VS Code, or pasted in `pyspark --master local[*]`.

# %% SparkContext
import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


def title(t):
    print("\n" + "=" * 70 + "\n" + t + "\n" + "=" * 70)


def ui(path):
    """Spark UI REST API (same data as http://localhost:4040)."""
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url) as r:
        return json.loads(r.read())


def completed_stages():
    return {s["stageId"]: s for s in ui("stages") if s["status"] == "COMPLETE"}


def new_stages(before):
    return [s for sid, s in sorted(completed_stages().items()) if sid not in before]


def show_stages(stages):
    for s in stages:
        print(f"  stage {s['stageId']:>3} | tasks={s['numTasks']:>2} | "
              f"shuffleWrite={s['shuffleWriteBytes']:>7} B ({s['shuffleWriteRecords']} rec) | "
              f"shuffleRead={s['shuffleReadBytes']:>7} B ({s['shuffleReadRecords']} rec) | {s['name']}")


print("Spark", spark.version, "| UI:", sc.uiWebUrl, "| defaultParallelism:", sc.defaultParallelism,
      "| defaultMinPartitions:", sc.defaultMinPartitions)

# %% RDD creation and partitions
title("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users_lines partitions :", users_lines.getNumPartitions())
print("orders_lines partitions:", orders_lines.getNumPartitions())

orders_repartitioned = orders_lines.repartition(8)
print("after repartition(8)   :", orders_repartitioned.getNumPartitions())
print("after coalesce(1)      :", orders_lines.coalesce(1).getNumPartitions())
print("--- lineage repartition(8) ---")
print(orders_repartitioned.toDebugString().decode())
print("--- lineage coalesce(1) ---")
print(orders_lines.coalesce(1).toDebugString().decode())

# %% Lazy evaluation and lineage
title("Lazy evaluation and lineage")
header = orders_lines.first()
print("header:", header)
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())

before = completed_stages()
print("count #1:", orders_data.count())
print("count #2:", orders_data.count())
print("stages created by the 2 counts (no cache):")
show_stages(new_stages(before))

# %% The cost of no schema: the multiline address
title("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(repr(line))
print("users_lines.count():", users_lines.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("users_records.count():", users_records.count())
print("users via spark.read.csv(multiLine=True):",
      spark.read.csv("users.csv", header=True, multiLine=True).count())

# %% Manual parsing of orders
title("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
for o in orders_rdd.take(3):
    print(o)

# Question: what if `product` contained an extra comma?
try:
    parse_order("x,y,2020-01-01,3,bread, with butter")
except ValueError as e:
    print("extra comma ->", type(e).__name__, ":", e)

# %% Key-value RDDs: total quantity per user
title("Key-value RDDs: reduceByKey vs groupByKey")
before = completed_stages()
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print("reduceByKey:", user_quantity.take(5))
print("stages (reduceByKey):")
show_stages(new_stages(before))

before = completed_stages()
user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print("groupByKey :", user_quantity_grouped.take(5))
print("stages (groupByKey):")
show_stages(new_stages(before))
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

# %% Manual deduplication
title("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print("orders_rdd.count(), orders_dedup.count():", orders_rdd.count(), orders_dedup.count())
dups = (orders_rdd.map(lambda o: (o["order_id"], 1)).reduceByKey(add)
        .filter(lambda kv: kv[1] > 1))
print("order_ids appearing more than once:", dups.count(), dups.take(3))

# %% Joining orders with users
title("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
for e in enriched.take(3):
    print(e)
print("enriched.count():", enriched.count())
print(enriched.toDebugString().decode())

left = orders_kv.leftOuterJoin(users_kv)
print("leftOuterJoin count:", left.count(),
      "| orders without user:", left.filter(lambda kv: kv[1][1] is None).count())

# Co-partitioned join: same partitioner on both sides -> no extra shuffle at join time
users_p = users_kv.partitionBy(4).cache()
orders_p = orders_kv.partitionBy(4).cache()
users_p.count(); orders_p.count()
before = completed_stages()
print("co-partitioned join count:", orders_p.join(users_p).count())
print("stages for co-partitioned join (expect a single stage, no shuffle):")
show_stages(new_stages(before))
users_p.unpersist(); orders_p.unpersist()

# %% Exercise 1: aggregateByKey
title("Exercise 1: aggregateByKey -> (order_count, total_quantity), average")
user_stats = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),                                       # zero value: (count, total)
        lambda acc, q: (acc[0] + 1, acc[1] + q),      # seqOp: within a partition
        lambda a, b: (a[0] + b[0], a[1] + b[1]),      # combOp: across partitions
    )
)
user_avg = user_stats.mapValues(lambda ct: round(ct[1] / ct[0], 3))
for k, v in user_stats.take(5):
    print(k, v, "avg =", round(v[1] / v[0], 3))
print("check totals match reduceByKey:",
      sorted(user_stats.mapValues(lambda ct: ct[1]).collect()) == sorted(user_quantity.collect()))
print("top 3 average quantity:", user_avg.takeOrdered(3, key=lambda kv: -kv[1]))

# %% Exercise 2: fake user_id + leftOuterJoin
title("Exercise 2: fake user_id + leftOuterJoin")
FAKE_USER = "00000000-0000-0000-0000-000000000000"
fake_order = {"order_id": "fake-order-1", "user_id": FAKE_USER,
              "date": "2026-10-07 00:00:00+00:00", "quantity": 1, "product": "ghost"}
orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))
orphans = (
    orders_with_fake
    .map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda kv: kv[1][1] is None)
)
print("orders with no matching user:", orphans.count())
print(orphans.take(1))

# %% Exercise 3: DataFrame equivalent
title("Exercise 3: DataFrame API equivalent")
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
user_quantity_df.show(5, truncate=False)
user_quantity_df.explain()
print(user_quantity.toDebugString().decode())
df_rows = sorted((r[0], r[1]) for r in user_quantity_df.collect())
print("DataFrame == RDD result:", df_rows == sorted(user_quantity.collect()))

# %% Exercise 4: cache vs no cache timing
title("Exercise 4: cache vs no cache")


def bench(rdd_parsed, label):
    uq = rdd_parsed.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    def ms(fn):
        t0 = time.perf_counter()
        fn()
        return round((time.perf_counter() - t0) * 1000, 1)

    res = {"orders.count 1st": ms(rdd_parsed.count),
           "orders.count median(5)": sorted(ms(rdd_parsed.count) for _ in range(5))[2],
           "user_quantity.count median(5)": sorted(ms(uq.count) for _ in range(5))[2]}
    storage = [(r["name"], r["numCachedPartitions"], r["memoryUsed"]) for r in ui("storage/rdd")]
    print(f"{label:<10} times (ms): {res} | Storage tab: {storage}")


orders_rdd.unpersist()
spark.catalog.clearCache()
no_cache = orders_data.map(parse_order).setName("orders_no_cache")
bench(no_cache, "no cache")
cached = orders_data.map(parse_order).setName("orders_cached").cache()
bench(cached, "cache")
cached.unpersist()

# %% Cleanup
spark.stop()
