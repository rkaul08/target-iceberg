"""Iceberg target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_iceberg.sinks import (
    IcebergSink,
)

class TargetIceberg(Target):
    name = "target-iceberg"

    config_jsonschema = th.PropertiesList(
        th.Property("add_record_metadata", th.BooleanType, default=False),
        th.Property( # temporary until pyiceberg fix is released
            "aws_access_key_id",
            th.StringType,
            required=True,
            description="AWS access key id",
        ),
        th.Property( # temporary until pyiceberg fix is released
            "max_batch_size",
            th.IntegerType,
            default=100000,
            description="Maximum number of records to process in one batch",
        ),
        th.Property( # temporary until pyiceberg fix is released
            "aws_secret_access_key",
            th.StringType,
            required=True,
            description="AWS secret access key",
        ),
        th.Property( # temporary until pyiceberg fix is released
            "s3_endpoint",
            th.StringType,
            required=True,
            description="S3 endpoint - e.g. http://localhost:9000",
        ),
        th.Property( # temporary until pyiceberg fix is released
            "s3_bucket",
            th.StringType,
            default="lakehouse",
            description="Name of the s3 bucket where Iceberg catalog is stored",
        ),
        th.Property( # temporary until pyiceberg fix is released
            "iceberg_rest_uri",
            th.StringType,
            required=True,
            description="Name of the Iceberg catalog",
        ),
        th.Property(
            "iceberg_catalog_name",
            th.StringType,
            required=True,
            description="Name of the Iceberg catalog",
        ),
        th.Property(
            "iceberg_catalog_namespace_name",
            th.StringType,
            required=True,
            description="Name of the Iceberg catalog namespace",
        ),
        th.Property(
            "table_name",
            th.StringType,
            default=None,
            description="Name of the Iceberg Table",
        ),
        th.Property(
             "schema_evolution_flag",
             th.BooleanType,
             default=False,
             description="Add new fields in existing table automatically",
         ),
        th.Property(
             "partition_date",
             th.StringType,
             default=None,
             description="Add custom partition date value, instead of current date partition",
         )
    ).to_dict()

    default_sink_class = IcebergSink


if __name__ == "__main__":
    TargetIceberg.cli()
