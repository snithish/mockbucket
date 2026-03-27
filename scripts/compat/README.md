# MockBucket compatibility suite

This project contains the Python-based compatibility runner and SDK checks for
MockBucket.

Run it from the repository root with:

```sh
uv run --project scripts/compat mockbucket-compat test
```

Optional PySpark parquet roundtrips for S3 and GCS can be run on demand:

```sh
uv run --project scripts/compat mockbucket-compat test --with-pyspark aws gcs
```

Those checks require a local Java runtime plus Spark connector packages for S3A and
GCS. In GitHub Actions they run only from a manually triggered `ci` workflow run.
