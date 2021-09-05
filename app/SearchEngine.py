import time
from typing import Union

import googlesearch
import numpy as np
import pandas as pd

from app.data_readers.ScrapeTermReader import ScrapeTermReader
from common.config.EngineConfig import EngineConfig
from common.lib import io_lib
from common.lib.Logger import ClassLogger, StandardLogger
from googleapi import googleapi


class SearchEngine(object):
    logger = ClassLogger.get_cls_logger()

    def __init__(self, logger: Union[ClassLogger, StandardLogger], engine_config: EngineConfig):
        self.lang = engine_config.lang
        self.set_logger(logger)

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    # TODO: leave below for now for single run testing - LS
    # TODO: use threading for term search - LS
    @classmethod
    def run_single(cls, term: str, num: int) -> list:
        cls.logger.log_info('SearchEngine', 'Starting search engine')
        try:
            result = googlesearch.search(term=term,
                                         num_results=num,
                                         lang='en')
        except Exception as e:
            cls.logger.log_info('SearchEngine', {e})
            raise
        return result

    # TODO: seems the max num of results by googlesearch is 100, try google-search-api in run_new() - LS
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.log_info('SearchEngine',
                             'Starting search engine')
        if data.empty or data is None or data['term'].isnull().all() or data['num'].isnull().all():
            return pd.DataFrame(columns=['term', 'url']).astype({'term': 'str', 'url': 'str'})
        try:
            t0 = time.time()
            url = data.assign(url=lambda df: [googlesearch.search(term=row.term,
                                                                  num_results=row.num,
                                                                  lang=self.lang) for row in df.itertuples()])
            t1 = time.time()
            self.logger.log_info('SearchEngine',
                                 f'Took {t1 - t0} seconds to complete {data.shape[0]} searches')
            result = self.transform(url)
            return result

        except Exception as e:
            self.logger.log_info('SearchEngine', f'Error during searching - {e}')
            raise

    @classmethod
    def transform(cls, data: pd.DataFrame) -> pd.DataFrame:
        result = (data
                  .filter(['term', 'url'])
                  .explode('url', ignore_index=True)
                  .drop_duplicates(subset=['url']))

        count = 0 if result.empty else result.shape[0]
        cls.logger.log_info('SearchEngine',
                            f'post transform {count} record(s)')
        return result

    # TODO: below become slow when num is > thousands. add threading and sleep to avoid google bot ban - LS
    # TODO: simplify standard_search.search() and remove unneeded attributes - LS
    def run_new(self, data: pd.DataFrame) -> pd.DataFrame:
        self.logger.log_info('SearchEngine',
                             'Starting search engine')
        if data.empty or data is None or data['term'].isnull().all() or data['num'].isnull().all():
            return pd.DataFrame(columns=['term', 'url']).astype({'term': 'str', 'url': 'str'})
        try:
            t0 = time.time()

            url = (data
                   .assign(page_num=lambda df: np.where(df['num'] % 10 == 0, df['num'] // 10, df['num'] // 10 + 1),
                           response=lambda df: [googleapi.standard_search.search(query=row.term,
                                                                                 pages=row.page_num,
                                                                                 lang=self.lang)
                                                for row in df.itertuples()],
                           url=lambda df: [[r.link for r in response] for response in df['response']])
                   .filter(['term', 'url']))

            t1 = time.time()
            self.logger.log_info('SearchEngine',
                                 f'Took {t1 - t0} seconds to complete {data.shape[0]} searches')
            result = self.transform(url)
            return result

        except Exception as e:
            self.logger.log_info('SearchEngine', f'Error during searching - {e}')
            raise


if __name__ == '__main__':
    # Dev testing
    app_location = io_lib.app_location_path('app')
    _term_data_location = io_lib.get_source_data_file(app_location, 'term_data.csv')

    log_file = io_lib.get_log_file(app_location, ending='scrape_service_logger.txt')
    _logger = StandardLogger('app', output_filename=log_file)

    data_reader = ScrapeTermReader(_term_data_location, _logger)
    term_data = data_reader.extract()

    config_file = io_lib.get_config_file(app_location, ending='yaml/engine_config.yml')
    _engine_config = EngineConfig(config_file)

    search_engine = SearchEngine(_logger, _engine_config)
    _url = search_engine.run(term_data)
    print(_url.shape)
