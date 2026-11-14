"""Partie 1 — Vérification du cluster Spark.

Exécution (depuis l'hôte) :
    docker compose exec jupyter python3 jobs/part1_cluster_check.py

Affiche les informations demandées par le sujet (version, master, appName,
defaultParallelism) et prouve que :
  - les deux workers exécutent réellement des tâches ;
  - S3A (MinIO) est accessible en lecture/écriture ;
  - les connecteurs Kafka, Delta et GraphFrames sont présents dans le classpath.
"""
import socket
import time

from common.spark_session import get_spark, lake_path

spark = get_spark("part1-cluster-check", cores_max=8)
sc = spark.sparkContext

# Laisser le temps aux executors de s'enregistrer auprès du driver
def n_executors() -> int:
    # getExecutorMemoryStatus contient un BlockManager par executor + celui du driver
    return sc._jsc.sc().getExecutorMemoryStatus().size() - 1


deadline = time.time() + 60
while n_executors() < 4 and time.time() < deadline:  # 8 cœurs / 2 cœurs par executor
    time.sleep(2)

print("=" * 70)
print("PARTIE 1 — INFORMATIONS SPARKSESSION")
print("=" * 70)
print(f"Version Spark          : {spark.version}")
print(f"Master                 : {sc.master}")
print(f"appName                : {sc.appName}")
print(f"applicationId          : {sc.applicationId}")
print(f"defaultParallelism     : {sc.defaultParallelism}")
print(f"Driver (hôte)          : {socket.gethostname()}")
print(f"Spark UI (driver)      : {sc.uiWebUrl}")

print(f"Executors enregistrés  : {n_executors()}")

print("\n--- Exécution distribuée : quels workers traitent les partitions ? ---")
t0 = time.time()
rdd = sc.parallelize(range(1_000_000), 16)
per_host = (
    rdd.mapPartitions(lambda it: [(socket.gethostname(), sum(1 for _ in it))])
    .reduceByKey(lambda a, b: a + b)
    .collect()
)
for host, n in sorted(per_host):
    print(f"  {host:<20} {n:>9} éléments")
print(f"  Somme des carrés = {rdd.map(lambda x: x * x).sum()}  ({time.time() - t0:.2f}s)")
workers_used = {h for h, _ in per_host}

print("\n--- Accès S3A / MinIO ---")
probe = lake_path("_smoke_tests", "part1")
spark.range(10).withColumnRenamed("id", "n").write.mode("overwrite").parquet(probe)
print(f"  Écriture + relecture {probe} : {spark.read.parquet(probe).count()} lignes")

print("\n--- Connecteurs présents dans le classpath ---")
for label, cls in [
    ("Kafka source", "org.apache.spark.sql.kafka010.KafkaSourceProvider"),
    ("Delta Lake", "io.delta.tables.DeltaTable"),
    ("S3A", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
    ("GraphFrames", "org.graphframes.GraphFrame"),
    ("GraphX", "org.apache.spark.graphx.Graph"),
]:
    try:
        spark._jvm.java.lang.Class.forName(cls)
        print(f"  [OK] {label:<13} {cls}")
    except Exception as exc:  # noqa: BLE001
        print(f"  [KO] {label:<13} {cls} -> {exc}")

import delta  # noqa: E402,F401
import graphframes  # noqa: E402,F401
print("  [OK] Modules Python : delta, graphframes")

print("\n" + "=" * 70)
verdict = "OK" if len(workers_used) == 2 else "KO"
print(f"VERDICT : {len(workers_used)} worker(s) ont exécuté des tâches -> {verdict}")
print("=" * 70)

spark.stop()
