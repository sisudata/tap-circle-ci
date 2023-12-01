import copy
import datetime
import random
import string
from typing import Union

import mock
import pytest
import requests
import singer

from tap_circle_ci.streams import *
from tap_circle_ci.tap_circle_ci import load_schemas, discover


def test_validate_stream_dependencies():
    valid_stream_ids = ['pipelines', 'workflows', 'jobs']
    validate_stream_dependencies(valid_stream_ids)
    with pytest.raises(ValueError):
        validate_stream_dependencies(['pipelines', 'jobs'])


def _generate_random_field(field: str) -> Union[str, int, bool]:
    if field == 'boolean':
        return bool(random.randint(0,1))
    elif field == 'string':
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    elif field == 'integer':
        return random.randint(0, 10000)
    else:
        raise TypeError('Unexpected field type')


def generate_data_from_schema(schema: dict, num_shots: int, start_time: datetime.datetime) -> List[dict]:
    time_copy = copy.deepcopy(start_time)
    return [_generate_single_data_point(schema, time_copy + datetime.timedelta(hours=i)) for i in range(num_shots)][::-1]

def non_null_schema_types(schema):
    types = None
    if isinstance(schema['type'], str):
        types = [schema['type']]
    else:
        types = schema['type']

    return [t for t in types if t != 'null']

def _generate_single_data_point(schema: dict, time: datetime.datetime) -> Union[dict, int, bool, str]:
    types = non_null_schema_types(schema)
    if 'object' in types:
        return {k: _generate_single_data_point(v, time) for k, v in schema['properties'].items()}
    elif 'string' in types and schema.get('format') == 'date-time':
        return singer.utils.strftime(time)
    else:
        return _generate_random_field(types[0])


def pageify_data_points(data_points: List[dict], page_limit: int) -> List[dict]:
    page_data = {"items": [], "next_page_token": None}
    pages = []
    for i, data_point in enumerate(data_points):
        if (i + 1) % (page_limit + 1)== 0:
            page_data["next_page_token"] = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            pages.append(page_data)
            page_data = {"items": [], "next_page_token": None}
        page_data["items"].append(data_point)
    if len(data_points) % (page_limit + 1) != 0:
        pages.append(page_data)
    return pages


def pageify_sub_stream_data_points(num_parent_streams: int, sub_stream_data_points: List[dict], page_limit: int) -> List[List[dict]]:
    num_sub_points_per_parent = int(len(sub_stream_data_points) // num_parent_streams)
    pages = []
    for i in range(num_parent_streams):
        end_slice =  (i + 1) * num_sub_points_per_parent if i < num_parent_streams - 1 else len(sub_stream_data_points)
        pages += pageify_data_points(sub_stream_data_points[i * num_sub_points_per_parent:end_slice], page_limit)
    return pages


def turn_pages_into_mocks(pages: List[dict]) -> List[mock.Mock]:
    resps = []
    for page in pages:
        resp = mock.Mock()
        resp.status_code = 200
        resp.text = 'success'
        resp.json.return_value = page
        resps.append(resp)
    return resps


def test_get_pipelines_without_cutoff():
    raw_schemas = load_schemas()
    raw_schema = raw_schemas['pipelines']
    catalog = discover()
    schema = {'pipelines': next(s.schema for s in catalog.streams if s.tap_stream_id == 'pipelines')}
    metadata = {'pipelines': next(s.metadata for s in catalog.streams if s.tap_stream_id == 'pipelines')}
    fake_pipelines = generate_data_from_schema(
        raw_schema,
        50,
        singer.utils.now()
    )
    fake_workflows = generate_data_from_schema(
        raw_schemas['workflows'],
        300,
        singer.utils.now()
    )
    pipeline_pages = pageify_data_points(fake_pipelines, 30)
    workflow_pages = pageify_sub_stream_data_points(50, fake_workflows, 3)
    all_responses = turn_pages_into_mocks(pipeline_pages + workflow_pages)

    with mock.patch('tap_circle_ci.client.get_session') as gs, \
            mock.patch('singer.metrics.log'), \
            mock.patch('singer.metrics.record_counter') as rc:
            fake_session = mock.create_autospec(requests.Session())
            fake_session.headers = {}
            fake_session.request.side_effect = all_responses
            gs.return_value = fake_session
            mock_rc = mock.Mock()
            mock_rc.increment.return_value = None
            rc.return_value = mock_rc
            get_all_pipelines(schema, "fake_project", {}, metadata)
    assert mock_rc.increment.call_count == 50


def test_get_pipelines_with_bookmark():
    raw_schemas = load_schemas()
    raw_schema = raw_schemas['pipelines']
    catalog = discover()
    schema = {'pipelines': next(s.schema for s in catalog.streams if s.tap_stream_id == 'pipelines')}
    metadata = {'pipelines': next(s.metadata for s in catalog.streams if s.tap_stream_id == 'pipelines')}
    fake_pipelines = generate_data_from_schema(
        raw_schema,
        50,
        singer.utils.now() - datetime.timedelta(hours=24)
    )
    fake_workflows = generate_data_from_schema(
        raw_schemas['workflows'],
        300,
        singer.utils.now()
    )
    pipeline_pages = pageify_data_points(fake_pipelines, 100)
    workflow_pages = pageify_sub_stream_data_points(50, fake_workflows, 3)
    all_responses = turn_pages_into_mocks(pipeline_pages + workflow_pages)

    with mock.patch('tap_circle_ci.client.get_session') as gs, \
            mock.patch('singer.metrics.log'), \
            mock.patch('singer.metrics.record_counter') as rc:
            fake_session = mock.create_autospec(requests.Session())
            fake_session.headers = {}
            fake_session.request.side_effect = all_responses
            gs.return_value = fake_session
            mock_rc = mock.Mock()
            mock_rc.increment.return_value = None
            rc.return_value = mock_rc
            get_all_pipelines(
                schema,
                "fake_project",
                {"bookmarks": {"fake_project": {"pipelines": {"since": singer.utils.strftime(singer.utils.now())}}}},
                metadata
            )
    assert mock_rc.increment.call_count == 25


def test_get_pipelines_and_workflows():
    raw_schemas = load_schemas()
    catalog = discover()
    schemas = {
        'pipelines': next(s.schema for s in catalog.streams if s.tap_stream_id == 'pipelines'),
        'workflows': next(s.schema for s in catalog.streams if s.tap_stream_id == 'workflows')
    }
    metadata = {
        'pipelines': next(s.metadata for s in catalog.streams if s.tap_stream_id == 'pipelines'),
        'workflows': next(s.metadata for s in catalog.streams if s.tap_stream_id == 'workflows')
    }
    fake_pipelines = generate_data_from_schema(
        raw_schemas['pipelines'],
        50,
        singer.utils.now() - datetime.timedelta(hours=50)
    )
    fake_workflows = generate_data_from_schema(
        raw_schemas['workflows'],
        300,
        singer.utils.now() - datetime.timedelta(hours=50)
    )
    pipeline_pages = pageify_data_points(fake_pipelines, 30)
    workflow_pages = pageify_sub_stream_data_points(50, fake_workflows, 3)
    all_responses = turn_pages_into_mocks(pipeline_pages + workflow_pages)

    with mock.patch('tap_circle_ci.client.get_session') as gs, \
        mock.patch('singer.metrics.log'), \
        mock.patch('singer.metrics.record_counter') as rc:
            fake_session = mock.create_autospec(requests.Session())
            fake_session.headers = {}
            fake_session.request.side_effect = all_responses
            gs.return_value = fake_session
            mock_rc_1 = mock.Mock()
            mock_rc_1.increment.return_value = None
            mock_rc_2 = mock.Mock()
            mock_rc_2.increment.return_value = None
            rc.side_effect = [mock_rc_1, mock_rc_2]
            get_all_pipelines(schemas, "fake_project", {}, metadata)
    assert mock_rc_1.increment.call_count == 50
    assert mock_rc_2.increment.call_count == 300
