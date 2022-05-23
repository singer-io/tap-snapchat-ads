import os
import json
import singer
from singer import metadata
from tap_snapchat_ads.streams import STREAMS

LOGGER = singer.get_logger()

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    schemas = {}
    field_metadata = {}

    for stream_name, stream_class in STREAMS.items():
        base_schema_path = 'schemas/{}.json'.format(stream_name)
        schema_file_path = stream_class.json_schema or base_schema_path
        schema_path = get_abs_path(schema_file_path)
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_class.key_properties or None,
            valid_replication_keys=stream_class.replication_keys or None,
            replication_method=stream_class.replication_method or None
        )

        mdata_map = metadata.to_map(mdata)
        # update inclusion of "replication keys" as "automatic"
        for replication_key in (stream_class.replication_keys or []):
            mdata_map[('properties', replication_key)]['inclusion'] = 'automatic'

        field_metadata[stream_name] = metadata.to_list(mdata_map)

    return schemas, field_metadata
