from typing import cast, Any, List, Tuple, Union
import pyarrow as pa  # type: ignore
from pyarrow import Schema as PyarrowSchema, Field as PyarrowField
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema
from decimal import Decimal
import decimal


# Borrowed from https://github.com/crowemi/target-s3/blob/main/target_s3/formats/format_parquet.py
def singer_to_pyarrow_schema_without_field_ids(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""

    def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
        """This function takes in original array of anyOf's schema detected
        and reduces it to the detected schema, based on rules, right now
        just detects whether it is string or not.
        """
        types, formats = [], []
        for val in anyOf:
            typ = val.get("type")
            if val.get("format"):
                formats.append(val["format"])
            if type(typ) is not list:
                types.append(typ)
            else:
                types.extend(typ)
        types = list(set(types))
        formats = list(set(formats))
        ret_type = []
        if "string" in types:
            ret_type.append("string")
        if "null" in types:
            ret_type.append("null")
        return ret_type, formats[0] if formats else None

    def get_pyarrow_schema_from_array(items: dict, level: int = 0):
        type = cast(list[Any], items.get("type"))
        any_of_types = items.get("anyOf")
        format_type = items.get("format")

        if any_of_types:
            self.logger.info("array with anyof type schema detected.")
            type, _ = process_anyof_schema(anyOf=any_of_types)

        if "string" in type:
            if format_type == "date":
                return pa.date32()
            return pa.string()
        elif "integer" in type:
            return pa.int64()
        elif "number" in type:
            return pa.float64()
        elif "boolean" in type:
            return pa.bool_()
        elif "array" in type:
            subitems = cast(dict, items.get("items"))
            return pa.list_(get_pyarrow_schema_from_array(items=subitems, level=level))
        elif "object" in type:
            subproperties = cast(dict, items.get("properties"))
            return pa.struct(get_pyarrow_schema_from_object(properties=subproperties, level=level + 1))
        else:
            return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0):
        """
        Returns schema for an object.
        """
        fields = []

        for key, val in properties.items():
            if "type" in val.keys():
                type = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                type, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.warning("type information not given")
                type = ["string", "null"]

            if "integer" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.int64(), nullable=nullable))
            elif "number" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.decimal128(38,10), nullable=nullable))
            elif "boolean" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.bool_(), nullable=nullable))
            elif "string" in type:
                nullable = "null" in type
                if format and level == 0:
                    # this is done to handle explicit datetime conversion
                    # which happens only at level 1 of a record
                    if format == "date":
                        fields.append(pa.field(key, pa.date32(), nullable=nullable))
                    elif format == "time":
                        fields.append(pa.field(key, pa.time64(), nullable=nullable))
                    else:
                        fields.append(pa.field(key, pa.timestamp("us", tz="UTC"), nullable=nullable))
                else:
                    fields.append(pa.field(key, pa.string(), nullable=nullable))
            elif "array" in type:
                nullable = "null" in type
                items = val.get("items")
                if items:
                    item_type = get_pyarrow_schema_from_array(items=items, level=level)
                    if item_type == pa.null():
                        self.logger.warn(
                            f"""key: {key} is defined as list of null, while this would be
                                correct for list of all null but it is better to define
                                exact item types for the list, if not null."""
                        )
                    fields.append(pa.field(key, pa.list_(item_type), nullable=nullable))
                else:
                    self.logger.warn(
                        f"""key: {key} is defined as list of null, while this would be
                            correct for list of all null but it is better to define
                            exact item types for the list, if not null."""
                    )
                    fields.append(pa.field(key, pa.list_(pa.null()), nullable=nullable))
            elif "object" in type:
                nullable = "null" in type
                prop = val.get("properties")
                inner_fields = get_pyarrow_schema_from_object(properties=prop, level=level + 1)
                if not inner_fields:
                    self.logger.warn(
                        f"""key: {key} has no fields defined, this may cause
                            saving parquet failure as parquet doesn't support
                            empty/null complex types [array, structs] """
                    )
                fields.append(pa.field(key, pa.struct(inner_fields), nullable=nullable))

        return fields

    properties = singer_schema["properties"]
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def assign_pyarrow_field_ids(self, pa_fields: list[PyarrowField], field_id: int = 0) -> Tuple[list[PyarrowField], int]:
    """Assign field ids to the schema."""
    new_fields = []
    for field in pa_fields:
        if isinstance(field.type, pa.StructType):
            field_indices = list(range(field.type.num_fields))
            struct_fields = [field.type.field(field_i) for field_i in field_indices]
            nested_pa_fields, field_id = assign_pyarrow_field_ids(self, struct_fields, field_id)
            new_fields.append(
                pa.field(field.name, pa.struct(nested_pa_fields), nullable=field.nullable, metadata=field.metadata)
            )
        else:
            field_id += 1
            field_with_metadata = field.with_metadata({"PARQUET:field_id": f"{field_id}"})
            new_fields.append(field_with_metadata)
    
    return new_fields, field_id


def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""
    pa_schema = singer_to_pyarrow_schema_without_field_ids(self, singer_schema)
    pa_fields_with_field_ids, _ = assign_pyarrow_field_ids(self, pa_schema)
    return pa.schema(pa_fields_with_field_ids)


def pyarrow_to_pyiceberg_schema(self, pa_schema: PyarrowSchema) -> PyicebergSchema:
    """Convert pyarrow schema to pyiceberg schema."""
    # Ensure all fields have proper metadata before conversion
    fields = []
    for field in pa_schema:
        field_id = field.metadata.get(b'field_id')
        if field_id is None:
            # If no field_id in metadata, create one
            field_id = str(1000).encode()  # default field ID
        
        # Create new metadata with PARQUET:field_id
        new_metadata = {
            b'PARQUET:field_id': field_id,
            b'field_id': field_id
        }

        field_type = field.type
        if pa.types.is_date64(field_type):
            field_type = pa.date32()

        field = pa.field(field.name, field_type, field.nullable, new_metadata)
        fields.append(field)

    
    # Create new schema with updated fields
    updated_schema = pa.schema(fields)
    
    # Convert to pyiceberg schema
    pyiceberg_schema = pyarrow_to_schema(updated_schema)
    return pyiceberg_schema
