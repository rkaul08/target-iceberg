"""Iceberg target sink class, which handles writing streams."""

from __future__ import annotations
import os
from typing import cast, Any
from datetime import datetime
import json

import pyarrow as pa  # type: ignore
from pyarrow import fs
from singer_sdk.sinks import BatchSink
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

from .iceberg import singer_to_pyarrow_schema, pyarrow_to_pyiceberg_schema


class IcebergSink(BatchSink):
    """Iceberg target sink class for writing data streams to Apache Iceberg tables."""

    # Default Iceberg write properties
    WRITE_PROPERTIES = {
        "write.format.default": "parquet",
        "write.target-file-size-bytes": "268435456",  # 256MB
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "1",
        "write.parquet.bloom-filter-enabled": "true",
        "write.metadata.metrics.default": "truncate(16)",
        "write.distribution-mode": "hash",
        "format-version": "2"
    }

    # S3 configuration
    S3_CONFIG = {
        "s3.upload.thread-pool-size": "4",
        "s3.multipart.threshold": "104857600",  # 100MB threshold for multipart upload
        "s3.multipart.part-size-bytes": "104857600"  # 100MB part size
    }

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
        """Initialize the Iceberg sink.

        Args:
            target: The target object.
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: Key properties for the stream.
        """
        super().__init__(
            target=target,
            schema=schema,
            stream_name=stream_name,
            key_properties=key_properties,
        )
        self.stream_name = stream_name
        self.schema = schema
        self.is_first_batch = True
        self.schema_evolution_flag = self.config.get("schema_evolution_flag", False)

    def _get_catalog(self) -> Any:
        """Initialize and return the Iceberg catalog."""
        region = 'eu-west-1'
        catalog_name = self.config.get("iceberg_catalog_name")
        s3_endpoint = self.config.get("s3_endpoint")
        iceberg_rest_uri = self.config.get("iceberg_rest_uri")

        self.logger.info(f"Catalog name: {catalog_name}")
        self.logger.info(f"S3 endpoint: {s3_endpoint}")
        self.logger.info(f"Iceberg REST URI: {iceberg_rest_uri}")

        catalog_config = {
            "uri": iceberg_rest_uri,
            "s3.endpoint": s3_endpoint,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.region": region,
            "s3.access-key-id": self.config.get("aws_access_key_id"),
            "s3.secret-access-key": self.config.get("aws_secret_access_key"),
            **self.S3_CONFIG
        }

        return load_catalog(catalog_name, **catalog_config)

    def _prepare_schema(self) -> tuple[pa.Schema, datetime]:
        """Prepare the PyArrow schema with partition date field.
        
        Returns:
            Tuple of (PyArrow schema, partition date value)
        """
        # Add partition_date to schema
        singer_schema = self.schema.copy()
        singer_schema["properties"]["partition_date"] = {
            "type": ["string", "null"],
            "format": "date"
        }
        
        # Convert to PyArrow schema
        original_pa_schema = singer_to_pyarrow_schema(self, singer_schema)

        return original_pa_schema, datetime.now().date()

    def process_batch(self, context: dict) -> None:
        """Process a batch of records.

        Args:
            context: Stream partition or context dictionary.
        """
        # Initialize catalog and create namespace
        catalog = self._get_catalog()
        ns_name: str = cast(str, self.config.get("iceberg_catalog_namespace_name"))
        
        try:
            catalog.create_namespace(ns_name)
            self.logger.info(f"Namespace '{ns_name}' created")
        except (NamespaceAlreadyExistsError, NoSuchNamespaceError):
            self.logger.info(f"Namespace '{ns_name}' already exists")

        # Prepare schema and data
        pa_schema, partition_date_value = self._prepare_schema()
        
        # Handle table creation or loading
        table_name = self.stream_name
        table_id = f"{ns_name}.{table_name}"

        try:
            table = catalog.load_table(table_id)
            self.logger.info(f"Table '{table_id}' loaded")

            table_schema = table.schema()
            table_field_ids = {field.name: field.field_id for field in table_schema.fields}

            new_fields = []
            additional_fields = []
            for field in pa_schema:
                if field.name in table_field_ids:
                    # Use existing field ID from table
                    field_with_metadata = field.with_metadata({"PARQUET:field_id": f"{table_field_ids[field.name]}"})
                    new_fields.append(field_with_metadata)
                else:
                     additional_fields.append(field)

            if additional_fields:
                if not self.schema_evolution_flag:
                    # If schema evolution is disabled and we found new fields, raise error
                    field_names = [field.name for field in additional_fields]
                    raise ValueError(f"New fields found in DataFrame that don't exist in table: {', '.join(field_names)}. Add them in the table Manually or Add < schema_evolution_flag = True > to meltano target")
                
                # Schema evolution is enabled, add new fields
                self.logger.info(f"Adding new fields: {[field.name for field in additional_fields]}")
                
                try:
                    with table.update_schema() as update:
                        for field in additional_fields:
                            # Convert PyArrow type to Iceberg type
                            iceberg_schema = pyarrow_to_pyiceberg_schema(self, pa.schema([field]))
                            iceberg_field = iceberg_schema.fields[0]
                            
                            # Add the new column
                            update.add_column(field.name, iceberg_field.field_type)
                    
                    # After schema update, get new field IDs and update PyArrow schema
                    updated_schema = table.schema()
                    for field in additional_fields:
                        field_id = updated_schema.find_field(field.name).field_id
                        field_with_metadata = field.with_metadata({"PARQUET:field_id": f"{field_id}"})
                        new_fields.append(field_with_metadata)
                        
                    self.logger.info("Schema updated successfully")
                    
                except Exception as e:
                    self.logger.error(f"Schema evolution failed: {str(e)}")
                    raise


                
            pa_schema = pa.schema(new_fields)
        except NoSuchTableError:
            # Create new table with partition spec
            pyiceberg_schema = pyarrow_to_pyiceberg_schema(self, pa_schema)
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=pyiceberg_schema.find_field("partition_date").field_id,
                    transform=DayTransform(),
                    name="partition_date",
                    field_id=1000
                )
            )

            table = catalog.create_table(
                identifier=table_id,
                schema=pyiceberg_schema,
                partition_spec=partition_spec,
                properties=self.WRITE_PROPERTIES
            )
            self.logger.info(f"Table '{table_id}' created")

        # Add partition_date to records
        for record in context["records"]:
            record["partition_date"] = partition_date_value

        # Create DataFrame
        df = pa.Table.from_pylist(context["records"], schema=pa_schema)

        # Write data
        if self.is_first_batch:
            partition_filter = f"partition_date = '{partition_date_value}'"
            existing_files = table.scan(row_filter=partition_filter).plan_files()

            if existing_files:
                # Drop the specific partition if it exists
                self.logger.info(f"Dropping existing partition for date {partition_date_value}")
                table.delete(partition_filter)

            self.logger.info(f"Overwriting partition for date {partition_date_value}")
            table.overwrite(
                df,
                overwrite_filter=f"partition_date = '{partition_date_value}'"
            )
            self.is_first_batch = False
        else:
            table.append(df)
