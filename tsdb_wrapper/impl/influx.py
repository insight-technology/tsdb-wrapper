from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import influxdb

from tsdb_wrapper.base import TimeseriesDatabase, TimeseriesDataSchema


class InfluxDB(TimeseriesDatabase):
    def __init__(
            self,
            host: str,
            port: int,
            username: str,
            password: str,
            database: str):

        self.client = influxdb.InfluxDBClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )

    @TimeseriesDatabase.schema.getter  # type: ignore
    def schema(self):
        ret = TimeseriesDataSchema()

        categories = self.get_categories()
        for m in categories:
            tags = {}
            for k in self.get_tag_keys_of_category(m):
                tags[k] = self.get_tag_values_by_key_of_category(m, k)
            ret[m] = tags

        return ret

    def query(
            self,
            category: str,
            tags: Dict[str, str] = {},
            begin_timestamp: Union[int, str, datetime] = None,
            end_timestamp: Union[int, str, datetime] = None,
            selects: List[str] = []):
        category = self._quote(category, '"')

        where_query = ''
        for tag, value in tags.items():
            tag = self._quote(tag, '"')
            value = self._quote(value, "'")

            where_query += f'{tag} = {value} and '

        begin = self.__convert_to_influx_timequery(begin_timestamp)
        end = self.__convert_to_influx_timequery(end_timestamp)

        if begin is not None:
            where_query += f' time >= {begin} and '

        if end is not None:
            where_query += f' time <= {end} and '

        if where_query != '':
            where_query = where_query[:-5]

        select_query = ''
        if len(selects) == 0:
            select_query = '*'
        else:
            for elm in selects:
                elm = self._quote(elm, '"')
                select_query += f'{elm},'
            select_query = select_query[:-1]

        query = f'select {select_query} from {category}'
        if where_query != '':
            query += f' where {where_query}'

        result = self.client.query(query)

        return list(result.get_points())

    def raw_query(self, query: str):
        ret = self.client.query(query)
        return ret

    def get_categories(self):
        query = "show measurements"
        ret = self.client.query(query)
        values = [value.get('name') for value in ret.get_points()]
        return values

    def get_tag_keys_of_category(self, category: str):
        category = self._quote(category, '"')

        query = f'show tag keys from {category}'
        ret = self.client.query(query)
        values = [value.get('tagKey') for value in ret.get_points()]
        return values

    def get_tag_values_by_key_of_category(
            self,
            category: str,
            key: str) -> List:
        category = self._quote(category, '"')
        key = self._quote(key, '"')

        query = f'show tag values from {category} with key = {key}'
        ret = self.client.query(query)
        values = [value.get('value') for value in ret.get_points()]
        return values

    def get_value_columns_of_category(self, category: str):
        category = self._quote(category, '"')

        query = f'show field keys from {category}'
        ret = self.client.query(query)
        values = [elm.get('fieldKey') for elm in ret.get_points()]
        return values

    def __convert_to_influx_timequery(self, value: Any) -> Optional[str]:
        if value is None:
            return None

        if type(value) == int:
            return str(value)

        if type(value) == str:
            return f'\'{value}\''

        if type(value) == datetime:
            return str(int(value.timestamp() * 1000 * 1000 * 1000))  # ナノ秒

        raise ValueError()

    def _quote(self, val: str, quote_char: str):
        temp = val.replace(quote_char, f'\\{quote_char}')
        return f'{quote_char}{temp}{quote_char}'

    def __repr__(self):
        return f'InfluxDB_{self.client._host}_{self.client._port}_{self.client._database}'  # NOQA
