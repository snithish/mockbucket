"""PySpark helpers for optional compatibility checks."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

DEFAULT_S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4"
DEFAULT_GCS_JARS = (
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/"
    "hadoop3-2.2.26/gcs-connector-hadoop3-2.2.26-shaded.jar"
)


def _base_builder(app_name: str) -> "SparkSession.Builder":
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.master(os.environ.get("MOCKBUCKET_SPARK_MASTER", "local[2]"))
    builder = builder.appName(app_name)
    builder = builder.config("spark.ui.enabled", "false")
    builder = builder.config("spark.sql.shuffle.partitions", "2")
    builder = builder.config("spark.driver.host", "127.0.0.1")
    builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
    builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")
    ivy_dir = os.environ.get("MOCKBUCKET_SPARK_IVY")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)
    ivy_settings = os.environ.get("MOCKBUCKET_SPARK_IVY_SETTINGS")
    if ivy_settings:
        builder = builder.config("spark.jars.ivySettings", ivy_settings)
    return builder


def _configured_builder(app_name: str, packages: str, jars: str = "") -> "SparkSession.Builder":
    builder = _base_builder(app_name)
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    if jars:
        builder = builder.config("spark.jars", jars)
    return builder


def s3a_roundtrip(
    *,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    bucket: str,
    key_prefix: str,
) -> int:
    """Write and read a parquet dataset through s3a:// and return the row count."""
    with _spark_session(
        app_name="mockbucket-compat-s3a",
        packages=os.environ.get("MOCKBUCKET_SPARK_S3_PACKAGES", DEFAULT_S3_PACKAGES),
        jars="",
        configs={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.endpoint.region": region,
            "spark.hadoop.fs.s3a.access.key": access_key,
            "spark.hadoop.fs.s3a.secret.key": secret_key,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.change.detection.mode": "none",
            "spark.hadoop.fs.s3a.impl.disable.cache": "true",
        },
    ) as spark:
        return _write_read_verify(
            spark,
            f"s3a://{bucket}/{key_prefix}",
        )


def gcs_roundtrip(
    *,
    endpoint: str,
    service_account_info: dict[str, Any],
    bucket: str,
    key_prefix: str,
) -> int:
    """Write and read a parquet dataset through gs:// and return the row count."""
    with _spark_session(
        app_name="mockbucket-compat-gcs",
        packages=os.environ.get("MOCKBUCKET_SPARK_GCS_PACKAGES", ""),
        jars=os.environ.get("MOCKBUCKET_SPARK_GCS_JARS", DEFAULT_GCS_JARS),
        configs={
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.fs.gs.project.id": "mockbucket",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.service.account.email": service_account_info["client_email"],
            "spark.hadoop.google.cloud.auth.service.account.private.key.id": service_account_info["private_key_id"],
            "spark.hadoop.google.cloud.auth.service.account.private.key": service_account_info["private_key"],
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "",
            "spark.hadoop.google.cloud.auth.service.account.keyfile": "",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.token.server.url": service_account_info["token_uri"],
            "spark.hadoop.fs.gs.storage.root.url": endpoint,
            "spark.hadoop.fs.gs.storage.service.path": "/storage/v1/",
            "spark.hadoop.fs.gs.storage.download.url": f"{endpoint}/download/storage/v1/",
            "spark.hadoop.fs.gs.storage.upload.url": f"{endpoint}/upload/storage/v1/",
            "spark.hadoop.fs.gs.impl.disable.cache": "true",
        },
    ) as spark:
        return _write_read_verify(
            spark,
            f"gs://{bucket}/{key_prefix}",
        )


@contextmanager
def _spark_session(*, app_name: str, packages: str, jars: str, configs: dict[str, str]):
    builder = _configured_builder(app_name, packages, jars)
    for key, value in configs.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()


def _write_read_verify(spark: "SparkSession", base_path: str) -> int:
    frame = spark.createDataFrame(
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
        ["id", "bucket_partition", "value"],
    )

    regular_path = f"{base_path}/regular"
    partitioned_path = f"{base_path}/partitioned"

    frame.repartition(2).write.mode("overwrite").parquet(regular_path)
    regular_count = spark.read.parquet(regular_path).count()
    if regular_count != 3:
        raise RuntimeError(f"regular parquet count={regular_count}, want 3")

    frame.repartition(2, "bucket_partition").write.mode("overwrite").partitionBy("bucket_partition").parquet(
        partitioned_path
    )
    partitioned_count = spark.read.parquet(partitioned_path).count()
    if partitioned_count != 3:
        raise RuntimeError(f"partitioned parquet count={partitioned_count}, want 3")

    return partitioned_count
