#!/usr/bin/env python3
import os
from typing import List

import json

import singer
from singer import utils, metadata

from tap_circle_ci.streams import (TOP_LEVEL_STREAM_ID_TO_FUNCTION,
                                                            STREAM_ID_TO_SUB_STREAM_IDS,
                                                            validate_stream_dependencies)
from tap_circle_ci.client import add_authorization_header

REQUIRED_CONFIG_KEYS = ["token", "project_slugs"]
LOGGER = singer.get_logger()

def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas() -> dict:
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def discover() -> singer.catalog.Catalog:
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return singer.catalog.Catalog.from_dict({'streams': streams})

def get_selected_streams(catalog: singer.catalog.Catalog) -> list:
    '''
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog.streams:
        if getattr(stream.schema, 'selected', False):
            selected_streams.append(stream.tap_stream_id)
        else:
            stream_metadata = metadata.to_map(stream.metadata)
            # stream metadata will have an empty breadcrumb
            if metadata.get(stream_metadata, (), "selected"):
                selected_streams.append(stream.tap_stream_id)

    return selected_streams


def extract_sub_stream_ids(stream_id: str) -> List[str]:
    """
    Get all children, grandchildren, etc.
    """
    if stream_id in STREAM_ID_TO_SUB_STREAM_IDS:
        next_level_streams = [sub_id for sub_id in STREAM_ID_TO_SUB_STREAM_IDS[stream_id]]
        # Recurse
        lowel_level_streams = []
        for sub_id in next_level_streams:
            lowel_level_streams += extract_sub_stream_ids(sub_id)
        return next_level_streams + lowel_level_streams
    else:
        return []


def sync(config: dict, state: dict, catalog: dict) -> None:
    """
    Syncs all projects
    """
    projects = list(filter(None, config['project_slugs'].split(' ')))
    add_authorization_header(config['token'])
    for project in projects:
        LOGGER.info(f'Syncing project {project}')
        sync_single_project(project, state, catalog)


def sync_single_project(project: str, state: dict, catalog: singer.catalog.Catalog) -> None:
    """
    Sync a single project's streams
    """
    selected_stream_ids = get_selected_streams(catalog)
    validate_stream_dependencies(selected_stream_ids)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        if stream_id in selected_stream_ids:
            # if it is a "sub_stream", it will be sync'd by its parent
            if TOP_LEVEL_STREAM_ID_TO_FUNCTION.get(stream_id) is None:
                continue
            LOGGER.info(f'Syncing stream: {stream_id}')

            # if stream is selected, write schema and sync
            stream_schema = stream.schema
            all_metadata = {stream_id: stream.metadata}
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema.to_dict(), stream.key_properties)

                # get sync function and any sub streams
                sync_func = TOP_LEVEL_STREAM_ID_TO_FUNCTION[stream_id]
                sub_stream_ids = extract_sub_stream_ids(stream_id)

                # handle streams with sub streams
                if len(sub_stream_ids) > 0:
                    stream_schemas = {stream_id: stream_schema}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            LOGGER.info(f'Syncing substream: {sub_stream_id} (descendent of {stream_id})')
                            sub_stream = next(s for s in catalog.streams if s.tap_stream_id == sub_stream_id)
                            stream_schemas[sub_stream_id] = sub_stream.schema
                            all_metadata[sub_stream_id] = sub_stream.metadata
                            singer.write_schema(sub_stream_id, sub_stream.schema.to_dict(),
                                                sub_stream.key_properties)

                # sync stream and it's sub streams
                state = sync_func(stream_schemas, project, state, all_metadata)
                singer.write_state(state)


@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog.to_dict(), indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog =  discover()

        sync(args.config, args.state, catalog)

if __name__ == "__main__":
    main()
