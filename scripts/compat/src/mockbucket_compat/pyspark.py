"""PySpark helpers for optional compatibility checks."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from typing import Any

DEFAULT_S3_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4"
DEFAULT_GCS_PACKAGES = "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26"


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
    """Run PySpark compatibility checks through s3a:// and return the scenario count."""
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
        return _run_compat_matrix(
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
    """Run PySpark compatibility checks through gs:// and return the scenario count."""
    with _spark_session(
        app_name="mockbucket-compat-gcs",
        packages=os.environ.get("MOCKBUCKET_SPARK_GCS_PACKAGES", DEFAULT_GCS_PACKAGES),
        jars=os.environ.get("MOCKBUCKET_SPARK_GCS_JARS", ""),
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
        return _run_compat_matrix(
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


def _run_compat_matrix(spark: "SparkSession", base_path: str) -> int:
    base_path = f"{base_path.rstrip('/')}/run-{uuid.uuid4().hex}"
    frame = spark.createDataFrame(
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
        ["id", "bucket_partition", "value"],
    )
    append_frame = spark.createDataFrame(
        [(4, "group-c", "delta"), (5, "group-c", "epsilon")],
        ["id", "bucket_partition", "value"],
    )
    overwrite_frame = spark.createDataFrame(
        [(10, "group-a", "updated")],
        ["id", "bucket_partition", "value"],
    )
    text_frame = spark.createDataFrame([("alpha",), ("beta",), ("gamma",)], ["value"])

    checks = 0
    checks += _verify_parquet_write_modes(spark, frame, append_frame, overwrite_frame, f"{base_path}/parquet")
    checks += _verify_partition_overwrite(spark, frame, overwrite_frame, f"{base_path}/partitioned")
    checks += _verify_delimited_formats(spark, frame, text_frame, f"{base_path}/formats")
    checks += _verify_single_file_outputs(spark, text_frame, f"{base_path}/single-file")
    checks += _verify_success_markers(spark, text_frame, f"{base_path}/markers")
    checks += _verify_filesystem_ops(spark, text_frame, f"{base_path}/filesystem")

    return checks


def _verify_parquet_write_modes(
    spark: "SparkSession",
    frame: "DataFrame",
    append_frame: "DataFrame",
    overwrite_frame: "DataFrame",
    base_path: str,
) -> int:
    regular_path = f"{base_path}/regular"
    ignore_path = f"{base_path}/ignore"
    error_path = f"{base_path}/error"

    frame.repartition(2).write.mode("overwrite").parquet(regular_path)
    _assert_rows_equal(
        spark.read.parquet(regular_path),
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
    )

    append_frame.repartition(1).write.mode("append").parquet(regular_path)
    _assert_rows_equal(
        spark.read.parquet(regular_path),
        [
            (1, "group-a", "alpha"),
            (2, "group-a", "beta"),
            (3, "group-b", "gamma"),
            (4, "group-c", "delta"),
            (5, "group-c", "epsilon"),
        ],
    )

    frame.write.mode("overwrite").parquet(ignore_path)
    overwrite_frame.write.mode("ignore").parquet(ignore_path)
    _assert_rows_equal(
        spark.read.parquet(ignore_path),
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
    )

    frame.write.mode("overwrite").parquet(error_path)
    try:
        overwrite_frame.write.mode("errorifexists").parquet(error_path)
    except Exception:
        pass
    else:
        raise RuntimeError("parquet errorifexists write unexpectedly succeeded")

    return 4


def _verify_partition_overwrite(
    spark: "SparkSession",
    frame: "DataFrame",
    overwrite_frame: "DataFrame",
    base_path: str,
) -> int:
    partitioned_path = f"{base_path}/parquet"

    frame.repartition(2, "bucket_partition").write.mode("overwrite").partitionBy("bucket_partition").parquet(
        partitioned_path
    )
    overwrite_frame.repartition(1).write.mode("overwrite").partitionBy("bucket_partition").parquet(partitioned_path)

    _assert_rows_equal(
        spark.read.parquet(partitioned_path),
        [(10, "group-a", "updated")],
    )

    fs = _filesystem_for_path(spark, partitioned_path)
    stale_partition_path = f"{partitioned_path}/bucket_partition=group-b"
    if _path_exists(fs, stale_partition_path):
        raise RuntimeError(f"partition overwrite left stale partition behind at {stale_partition_path}")

    return 2


def _verify_delimited_formats(
    spark: "SparkSession",
    frame: "DataFrame",
    text_frame: "DataFrame",
    base_path: str,
) -> int:
    csv_path = f"{base_path}/csv"
    json_path = f"{base_path}/json"
    text_path = f"{base_path}/text"

    frame.write.mode("overwrite").option("header", "true").csv(csv_path)
    _assert_rows_equal(
        spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path),
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
    )

    frame.write.mode("overwrite").json(json_path)
    _assert_rows_equal(
        spark.read.json(json_path),
        [(1, "group-a", "alpha"), (2, "group-a", "beta"), (3, "group-b", "gamma")],
    )

    text_frame.write.mode("overwrite").text(text_path)
    _assert_single_column_rows(
        spark.read.text(text_path),
        ["alpha", "beta", "gamma"],
    )

    return 3


def _verify_filesystem_ops(spark: "SparkSession", text_frame: "DataFrame", base_path: str) -> int:
    source_path = f"{base_path}/source"
    target_path = f"{base_path}/renamed"

    text_frame.write.mode("overwrite").text(source_path)

    fs = _filesystem_for_path(spark, source_path)
    if not _path_exists(fs, source_path):
        raise RuntimeError(f"filesystem source path is missing: {source_path}")

    statuses = fs.listStatus(_path_for(source_path))
    if len(statuses) == 0:
        raise RuntimeError(f"filesystem listStatus returned no entries for {source_path}")

    globbed = fs.globStatus(_path_for(f"{source_path}/part-*"))
    if not globbed:
        raise RuntimeError(f"filesystem globStatus returned no matches for {source_path}/part-*")

    if not fs.rename(_path_for(source_path), _path_for(target_path)):
        raise RuntimeError(f"filesystem rename failed: {source_path} -> {target_path}")
    if _path_exists(fs, source_path):
        raise RuntimeError(f"filesystem source path still exists after rename: {source_path}")
    if not _path_exists(fs, target_path):
        raise RuntimeError(f"filesystem target path is missing after rename: {target_path}")

    _assert_single_column_rows(
        spark.read.text(f"{target_path}/part-*"),
        ["alpha", "beta", "gamma"],
    )

    if not fs.delete(_path_for(target_path), True):
        raise RuntimeError(f"filesystem recursive delete failed for {target_path}")
    if _path_exists(fs, target_path):
        raise RuntimeError(f"filesystem target path still exists after delete: {target_path}")

    return 5


def _verify_single_file_outputs(spark: "SparkSession", text_frame: "DataFrame", base_path: str) -> int:
    csv_path = f"{base_path}/csv"

    text_frame.coalesce(1).write.mode("overwrite").option("header", "false").csv(csv_path)

    fs = _filesystem_for_path(spark, csv_path)
    part_files = fs.globStatus(_path_for(f"{csv_path}/part-*"))
    if len(part_files) != 1:
        raise RuntimeError(f"single-file output produced {len(part_files)} part files, want 1")

    actual = sorted(row._c0 for row in spark.read.csv(csv_path).collect())
    if actual != ["alpha", "beta", "gamma"]:
        raise RuntimeError(f"single-file csv rows={actual}, want ['alpha', 'beta', 'gamma']")

    return 2


def _verify_success_markers(spark: "SparkSession", text_frame: "DataFrame", base_path: str) -> int:
    text_path = f"{base_path}/text"

    text_frame.write.mode("overwrite").text(text_path)

    fs = _filesystem_for_path(spark, text_path)
    success_path = f"{text_path}/_SUCCESS"
    if not _path_exists(fs, success_path):
        raise RuntimeError(f"missing success marker at {success_path}")

    temporary_path = f"{text_path}/_temporary"
    if _path_exists(fs, temporary_path):
        raise RuntimeError(f"temporary path still exists after commit: {temporary_path}")

    return 2


def _assert_rows_equal(frame: "DataFrame", expected: list[tuple[int, str, str]]) -> None:
    actual = sorted((int(row.id), str(row.bucket_partition), str(row.value)) for row in frame.collect())
    if actual != sorted(expected):
        raise RuntimeError(f"rows={actual}, want {sorted(expected)}")


def _assert_single_column_rows(frame: "DataFrame", expected: list[str]) -> None:
    actual = sorted(str(row.value) for row in frame.collect())
    if actual != sorted(expected):
        raise RuntimeError(f"rows={actual}, want {sorted(expected)}")


def _filesystem_for_path(spark: "SparkSession", path: str):
    return _path_for(path).getFileSystem(spark._jsc.hadoopConfiguration())


def _path_for(path: str):
    return _spark_jvm().org.apache.hadoop.fs.Path(path)


def _spark_jvm():
    from pyspark import SparkContext

    sc = SparkContext._active_spark_context
    if sc is None:
        raise RuntimeError("SparkContext is not initialized")
    return sc._jvm


def _path_exists(fs: Any, path: str) -> bool:
    return bool(fs.exists(_path_for(path)))
