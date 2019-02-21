import json

from influxdb import InfluxDBClient
from tsdb_wrapper.impl.influx import InfluxDB

import pytest

test_db = InfluxDB('localhost', 8086, 'root', 'root', 'tsdb_test')


@pytest.fixture(scope='function', autouse=True)
def scope_function():
    def get_points(fname: str):
        with open(fname, 'r') as f:
            points = json.load(f)
            for point in points:
                measurement = point['category']
                point['measurement'] = measurement
                del(point['category'])
            return points

    db = InfluxDBClient('localhost', 8086, 'root', 'root', 'tsdb_test')
    db.write_points(get_points('tests/data/simple.json'))
    db.write_points(get_points('tests/data/special_character.json'))

    yield

    db.query('DROP DATABASE "tsdb_test"')
    db.query('CREATE DATABASE "tsdb_test"')


def test_get_schema():
    expected = ['simple', ' ,-_/.\'"']

    ret = test_db.get_categories()

    assert len(expected) == len(ret)
    assert set(expected) == set(ret)


def test_get_tags():
    expected = {
        'simple': {
            'tk1': ['tv1a', 'tv1b'],
            'tk2': ['tv2a', 'tv2b'],
            'tk3': ['tv3a'],
        },
        ' ,-_/.\'"': {
            'tk ,-_/.\'"1': ['tv ,-_/.\'"1'],
            'tk ,-_/.\'"2': ['tv ,-_/.\'"2']
        },
    }

    for category in expected:
        expected_tag_keys = list(expected[category].keys())
        ret_tag_keys = test_db.get_tag_keys_of_category(category)

        assert len(expected_tag_keys) == len(ret_tag_keys)
        assert set(expected_tag_keys) == set(ret_tag_keys)

        for tag_key in ret_tag_keys:
            expected_tag_values = expected[category][tag_key]
            ret_tag_values = test_db.get_tag_values_by_key_of_category(category, tag_key)  # NOQA

            assert len(expected_tag_values) == len(ret_tag_values)
            assert set(expected_tag_values) == set(ret_tag_values)


def test_get_value_keys():
    expected = {
        'simple': ['fk1', 'fk2'],
        ' ,-_/.\'"': ['fk ,-_/.\'"1', 'fk ,-_/.\'"2'],
    }

    for category in expected:
        expected_field_keys = list(expected[category])
        ret_field_keys = test_db.get_value_columns_of_category(category)

        assert len(expected_field_keys) == len(ret_field_keys)
        assert set(expected_field_keys) == set(ret_field_keys)


@pytest.mark.parametrize('query,expected', [
    # get all
    (
        {'category': 'simple'},
        [
            {
                'time': '2050-09-10T11:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1a', 'tk2': 'tv2a', 'tk3': 'tv3a',
            },
            {
                'time': '2050-09-10T12:00:00Z',
                'fk1': 'fv1a', 'fk2': 10,
                'tk1': 'tv1a', 'tk2': 'tv2a', 'tk3': 'tv3a',
            },
            {
                'time': '2050-09-10T13:00:00Z',
                'fk1': 'fv1b', 'fk2': 10,
                'tk1': 'tv1a', 'tk2': 'tv2a', 'tk3': 'tv3a'
            },
            {
                'time': '2050-09-10T14:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2a', 'tk3': 'tv3a'
            },
            {
                'time': '2050-09-10T15:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2b', 'tk3': 'tv3a'
            },
        ],
    ),
    # time filter
    (
        {
            'category': 'simple',
            'end_timestamp': '2050-09-10T10:59:59Z',
        },
        []
    ),
    (
        {
            'category': 'simple',
            'end_timestamp': '2050-09-10T11:00:00Z',
        },
        [
            {
                'time': '2050-09-10T11:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1a', 'tk2': 'tv2a', 'tk3': 'tv3a',
            },
        ],
    ),
    (
        {
            'category': 'simple',
            'begin_timestamp': '2050-09-10T15:00:00Z'
        },
        [
            {
                'time': '2050-09-10T15:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2b', 'tk3': 'tv3a'
            },
        ],
    ),
    (
        {
            'category': 'simple',
            'begin_timestamp': '2050-09-10T15:00:01Z'
        },
        [],
    ),
    # select fields
    (
        {
            'category': 'simple',
            'begin_timestamp': '2050-09-10T12:00:00Z',
            'end_timestamp': '2050-09-10T12:00:00Z',
            'selects': ['fk1']
        },
        [
            {
                'time': '2050-09-10T12:00:00Z',
                'fk1': 'fv1a',
            },
        ]
    ),
    (
        {
            'category': 'simple',
            'begin_timestamp': '2050-09-10T12:00:00Z',
            'end_timestamp': '2050-09-10T12:00:00Z',
            'selects': ['time', 'fk1']
        },
        [
            {
                'time': '2050-09-10T12:00:00Z',
                'fk1': 'fv1a',
            },
        ]
    ),
    # tag filter
    (
        {
            'category': 'simple',
            'tags': {
                'tk1': 'tv1b',
            },
        },
        [
            {
                'time': '2050-09-10T14:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2a', 'tk3': 'tv3a'
            },
            {
                'time': '2050-09-10T15:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2b', 'tk3': 'tv3a'
            }
        ],
    ),
    (
        {
            'category': 'simple',
            'tags': {
                'tk1': 'tv1b',
                'tk2': 'tv2b',
            },
        },
        [
            {
                'time': '2050-09-10T15:00:00Z',
                'fk1': 'fv1a', 'fk2': 1,
                'tk1': 'tv1b', 'tk2': 'tv2b', 'tk3': 'tv3a'
            }
        ],
    ),
])
def test_query(query, expected):
    ret = test_db.query(**query)
    assert ret == expected


@pytest.mark.parametrize('query,expected', [
    # get all
    (
        {'category': ' ,-_/.\'"'},
        [
            {
                'time': '2050-09-10T11:00:00Z',
                'tk ,-_/.\'"1': 'tv ,-_/.\'"1', 'tk ,-_/.\'"2': 'tv ,-_/.\'"2',
                'fk ,-_/.\'"1': 'fv ,-_/.\'"1', 'fk ,-_/.\'"2': 'fv ,-_/.\'"2',
            },
        ],
    ),
    # select fields
    (
        {
            'category': ' ,-_/.\'"',
            'selects': ['tk ,-_/.\'"1', 'fk ,-_/.\'"2'],
        },
        [
            {
                'time': '2050-09-10T11:00:00Z',
                'tk ,-_/.\'"1': 'tv ,-_/.\'"1',
                'fk ,-_/.\'"2': 'fv ,-_/.\'"2',
            },
        ]
    ),
    # tag filter
    (
        {
            'category': ' ,-_/.\'"',
            'tags': {
                'tk ,-_/.\'"1': 'tv ,-_/.\'"1',
            },
        },
        [
            {
                'time': '2050-09-10T11:00:00Z',
                'tk ,-_/.\'"1': 'tv ,-_/.\'"1', 'tk ,-_/.\'"2': 'tv ,-_/.\'"2',
                'fk ,-_/.\'"1': 'fv ,-_/.\'"1', 'fk ,-_/.\'"2': 'fv ,-_/.\'"2',
            },
        ],
    ),
    (
        {
            'category': ' ,-_/.\'"',
            'tags': {
                'tk ,-_/.\'"1': 'tv ,-_/.\'"1',
                'tk ,-_/.\'"2': 'tv ,-_/.\'"INVALID',
            },
        },
        [],
    ),
])
def test_special_character_query(query, expected):
    ret = test_db.query(**query)
    assert ret == expected
