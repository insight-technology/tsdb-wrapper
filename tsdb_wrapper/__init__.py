from .base import (TimeseriesDatabase, TimeseriesDatabaseConfig,
                   TimeseriesDataSchema)
from .impl.influx import InfluxDB
from ._meta import __version__


def timeseries_database_factory(
        config: TimeseriesDatabaseConfig) -> TimeseriesDatabase:
    if config.database_type == 'influx':
        return InfluxDB(
            config.host,
            config.port,
            config.username,
            config.password,
            config.database
        )
    else:
        raise ValueError


__all__ = [
    'timeseries_database_factory',
    'TimeseriesDatabase',
    'TimeseriesDatabaseConfig',
    'TimeseriesDataSchema',
    '__version__',
    ]
