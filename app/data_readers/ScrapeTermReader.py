from typing import Union

import pandas as pd

from common.lib.Logger import ClassLogger, StandardLogger


class ScrapeTermReader(object):
    logger = ClassLogger.get_cls_logger()

    def __init__(self, location, logger: Union[ClassLogger, StandardLogger]):
        self.location = location
        self.set_logger(logger)

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    def extract(self) -> pd.DataFrame:
        data = pd.read_csv(self.location)
        if data is None or data.empty:
            self.logger.log_info('ScrapeTermReader', 'Returned Null result')
            return pd.DataFrame(columns=self.source_columns()).astype(self.source_types())

        _result = (data
                   .filter(self.source_columns())
                   .astype(self.source_types()))

        valid_filter = (_result['num'] > 0) & (~_result['term'].isnull())

        self.flag_invalid_data(_result[~valid_filter])

        result = _result[valid_filter]
        count = 0 if result.empty else result.shape[0]
        self.logger.log_info('ScrapeTermReader',
                             f'Read {count} record(s)')
        return result

    @staticmethod
    def source_columns() -> list:
        return ['term', 'num']

    @staticmethod
    def source_types() -> dict:
        return {'term': 'str', 'num': 'int64'}

    def flag_invalid_data(self, data: pd.DataFrame):
        try:
            if data.empty:
                return
            message = (f'Flag {data.shape[0]} term record(s) with invalid data \n '
                       f'{data}')
            self.logger.log_info('ScrapeTermReader', message)

        except Exception as e:
            self.logger.log_info('ScrapeTermReader',
                                 f'Error raised while flagging invalid term data')
            raise e
