from tsdb_wrapper import TimeseriesDatabaseConfig, timeseries_database_factory
from tsdb_wrapper.impl.influx import InfluxDB

import pytest


def test_factory():
    config = TimeseriesDatabaseConfig(
        'influx',
        'localhost',
        8086,
        'root',
        'root',
        'test')
    db = timeseries_database_factory(config)
    assert isinstance(db, InfluxDB)


def test_factory_value_error():
    with pytest.raises(ValueError):
        config = TimeseriesDatabaseConfig(
            'awesome',
            'localhost',
            8086,
            'root',
            'root',
            'test')
        timeseries_database_factory(config)
