"""Fabrique de SparkSession partagée par les notebooks et les jobs.

La configuration "statique" (S3A/MinIO, Delta, event log) vit dans
conf/spark-defaults.conf, montée dans tous les conteneurs. Ce module ne
fait qu'ajouter les réglages propres à chaque application.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lake")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def lake_path(*parts: str) -> str:
    """lake_path("bronze", "orders_raw") -> s3a://lake/bronze/orders_raw"""
    return "/".join([LAKE_ROOT.rstrip("/"), *[p.strip("/") for p in parts]])


def get_spark(app_name: str, cores_max: int | None = None, **conf: str) -> SparkSession:
    """Crée (ou récupère) une SparkSession connectée au master standalone.

    cores_max : plafond de cœurs pour l'application (défaut cluster = 4,
    voir spark-defaults.conf) ; 8 = tout le cluster (2 workers x 4 cœurs).
    """
    builder = SparkSession.builder.appName(app_name).master(SPARK_MASTER_URL)
    if cores_max is not None:
        builder = builder.config("spark.cores.max", str(cores_max))
    for key, value in conf.items():
        builder = builder.config(key.replace("__", "."), value)
    return builder.getOrCreate()
