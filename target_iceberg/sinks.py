"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from typing import cast, Any
from singer_sdk.sinks import BatchSink
import pyarrow as pa  # type: ignore
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchNamespaceError, NoSuchTableError
from pyarrow import fs
from datetime import datetime
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import json
from .iceberg import singer_to_pyarrow_schema, pyarrow_to_pyiceberg_schema


class IcebergSink(BatchSink):
    """Iceberg target sink class."""

    @property
    def max_size(self) -> int:
        """Return configured batch size or default."""
        return self.config.get("max_batch_size", 100000)


    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.stream_name = stream_name
        self.schema = schema
        self.is_first_batch = True

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """

        write_properties = {
            "write.format.default": "iceberg",
            "write.target-file-size-bytes": "268435456",
            "write.parquet.row-group-size-bytes": "67108864",
            "write.parquet.page-size-bytes": "1048576",
            "write.parquet.compression-codec": "zstd",
            "write.parquet.compression-level": 1,
            "write.parquet.bloom-filter-enabled": "true",
            "write.metadata.metrics.default": "truncate(16)",
            "write.distribution-mode": "hash"
            #"write.target-file-size-bytes": "536870912",
        }


        # Load the Iceberg catalog
        region = 'eu-west-1'
        # region = fs.resolve_s3_region(self.config.get("s3_bucket"))
        # self.logger.info(f"AWS Region: {region}")

        catalog_name = self.config.get("iceberg_catalog_name")
        self.logger.info(f"Catalog name: {catalog_name}")

        s3_endpoint = self.config.get("s3_endpoint")
        self.logger.info(f"S3 endpoint: {s3_endpoint}")

        iceberg_rest_uri = self.config.get("iceberg_rest_uri")
        self.logger.info(f"Iceberg REST URI: {iceberg_rest_uri}")

        catalog = load_catalog(
            catalog_name,
            **{
                "uri": iceberg_rest_uri,
                "s3.endpoint": s3_endpoint,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": region,
                "s3.access-key-id": self.config.get("aws_access_key_id"),
                "s3.secret-access-key": self.config.get("aws_secret_access_key"),
                "s3.upload.thread-pool-size":"4",
                "s3.multipart.threshold": "104857600", # Only files greater than 100MB will be using multipart upload/parallel writes
                "s3.multipart.part-size-bytes": "104857600" # files will be divided on 100MB, 500MB file will have 5 parts
            },
        )

        nss = catalog.list_namespaces()
        self.logger.info(f"Namespaces: {nss}")

        # Create a namespace if it doesn't exist
        ns_name: str = cast(str, self.config.get("iceberg_catalog_namespace_name"))
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            # NoSuchNamespaceError is also raised for some reason (probably a bug - but needs to be handled anyway)
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Create pyarrow df
        singer_schema = self.schema

        for record in context["records"]:
            if "_sdc_batched_at" in record:
                record["partition_date"] = datetime.now().strftime("%Y-%m-%d")
                self.logger.info(f"The test record is : {record}")
                try:
                    batched_at = datetime.strptime(record["_sdc_batched_at"].split('.')[0], "%Y-%m-%d %H:%M:%S")
                    record["partition_date"] = batched_at.strftime("%Y-%m-%d")
                    self.logger.info(f"batched_at : {batched_at.strftime("%Y-%m-%d")}")
                    self.logger.info(f"The test record After addition is : {record}")
                except (ValueError, AttributeError):
                    record["partition_date"] = datetime.now().strftime("%Y-%m-%d")
                    self.logger.info(f"Except addition is : {record}")
            else:
                self.logger.info(f"Else addition is : {record}")
                record["partition_date"] = datetime.now().strftime("%Y-%m-%d")

        
        singer_schema["properties"]["partition_date"] = {
            "type": ["string", "null"],
            "format": "date"
        }
        self.logger.info(f"Singer Schema: {json.dumps(singer_schema, indent=2)}")

        pa_schema = singer_to_pyarrow_schema(self, singer_schema)

        self.logger.info(f"PyArrow Schema: {pa_schema}")

        df = pa.Table.from_pylist(context["records"], schema=pa_schema)

        # Create a table if it doesn't exist
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

            # TODO: Handle schema evolution - compare existing table schema with singer schema (converted to pyiceberg schema)
        except NoSuchTableError as e:
            # Table doesn't exist, so create it
            pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)

                # Create partition spec
            partition_spec = PartitionSpec(
            PartitionField(
                source_id=pyiceberg_schema.find_field("partition_date").field_id,
                transform=DayTransform(),
                name="partition_date"
                )
            )   

            table = catalog.create_table(
                identifier=table_id,
                schema=pyiceberg_schema,
                partition_spec=partition_spec,
                properties=write_properties)
            self.logger.info(f"Table '{table_id}' created")


        columns = {col: [record[col] for record in context["records"]] for col in pa_schema.names}
        df = pa.Table.from_pydict(columns, schema=pa_schema)

        # Add data to the table
        
        if self.is_first_batch:
            table.overwrite(df)
            self.is_first_batch = False
        else:
            table.append(df)
