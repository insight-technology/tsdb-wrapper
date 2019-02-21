from datetime import datetime
from typing import Dict, List, Union


class TimeseriesDatabaseConfig:
    __slots__ = [
        'database_type',
        'host',
        'port',
        'username',
        'password',
        'database',
        'kwargs',
    ]

    def __init__(
            self,
            database_type: str,
            host: str,
            port: int,
            username: str,
            password: str,
            database: str,
            **kwargs):
        self.database_type = database_type
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.kwargs = kwargs

    def build_masked_str(self) -> str:
        return f'Username and password are Hidden. {self.host}:{self.port}/{self.database}'  # NOQA

    def build_raw_str(self) -> str:
        return f'{self.host}:{self.port}/{self.database}, user: {self.username}, password: {self.password}'  # NOQA

    def __str__(self):
        return self.build_masked_str()


CategorySchema = Dict[str, List[str]]


class TimeseriesDataSchema:
    """
    時系列DB内のカテゴリとカテゴリ毎のタグとキーの組み合わせ
    TODO: Fieldの取扱
    """
    __slots__ = ['_schema']

    def __init__(self):
        self._schema = {}  # type: Dict[str, CategorySchema]

    @property
    def categories(self) -> List[str]:
        return list(self._schema.keys())

    def __getitem__(self, category: str):
        return self._schema[category]

    def __setitem__(self, category: str, tags: CategorySchema):
        self._schema[category] = tags

    def __contains__(self, category: str):
        return category in self._schema

    def to_dict(self):
        return self._schema


class TimeseriesDatabase:
    def __init__(
            self,
            host: str,
            port: int,
            username: str,
            password: str,
            database: str):
        raise NotImplementedError

    def query(
            self,
            category: str,
            tags: Dict[str, str] = {},
            begin_timestamp: Union[int, str, datetime] = None,
            end_timestamp: Union[int, str, datetime] = None,
            selects: List[str] = []):
        raise NotImplementedError

    def raw_query(self, query: str):
        raise NotImplementedError

    @property
    def schema(self) -> TimeseriesDataSchema:
        raise NotImplementedError

    def get_categories(self):
        raise NotImplementedError

    def get_value_columns_of_category(self, category: str):
        raise NotImplementedError
