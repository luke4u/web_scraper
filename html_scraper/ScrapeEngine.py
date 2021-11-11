import concurrent.futures
import time

import pandas as pd
import requests

from html_scraper.config.EngineConfig import EngineConfig
from html_scraper.lib import io_lib
from html_scraper.lib.Logger import ClassLogger


class ScrapeEngine(object):
    logger = ClassLogger.get_cls_logger()

    def __init__(self, logger: ClassLogger, engine_config: EngineConfig):
        self.set_logger(logger)
        self.timeout = float(engine_config.timeout)

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    def run_engine_single(self, url: str):
        self.logger.log_info('ScraperEngine', f'Scrapping {url}')
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code != requests.codes.ok:
                self.logger.log_info('ScraperEngine ', f'bad response received!')
                return None

            return response.text
        except Exception as e:
            self.logger.log_info('ScraperEngine', f'Error during scrapping {url}')
            return None

    def run_engine_old(self, url: pd.DataFrame):
        self.logger.log_info('ScraperEngine', 'Starting ScraperEngine - Requests module')
        if url.empty or url is None:
            return pd.DataFrame(columns=self.get_default_cols()).astype(self.get_default_types())

        try:
            t0 = time.time()
            result = (url.groupby(['term'], as_index=False)
                      .apply(self.try_get_html)
                      .filter(self.get_default_cols()))
            t1 = time.time()
            self.logger.log_info('ScrapeEngine', f'Took {t1 - t0} seconds to complete {result.shape[0]} scrapping')

            return result

        except Exception as e:
            self.logger.log_info('ScraperEngine', f'{e}')
            return pd.DataFrame(columns=self.get_default_cols()).astype(self.get_default_types())

    def try_get_html(self, data: pd.DataFrame) -> pd.DataFrame:
        _data = data.filter(['url']).set_index('url')
        for row in _data.itertuples():
            url = row.Index
            try:
                response = requests.get(url, timeout=self.timeout)
                _data.at[url, 'html'] = response.text

            except Exception as e:
                self.logger.log_info('ScraperEngine', f'{e}')
        result = _data.reset_index(drop=False)
        return result

    def run_engine(self, url: pd.DataFrame):
        self.logger.log_info('ScraperEngine', 'Starting ScraperEngine')
        if url.empty or url is None:
            return pd.DataFrame(columns=self.get_default_cols()).astype(self.get_default_types())

        try:
            t0 = time.time()
            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                response = [executor.submit(self.run_engine_single, url) for url in url['url'].values]
                html = [r.result() for r in response]
            t1 = time.time()

            result = (url
                      .assign(html=html)
                      .filter(self.get_default_cols()))

            self.logger.log_info('ScrapeEngine',
                                 f'Took {t1 - t0} seconds to complete {result.shape[0]} scrapping')

            return result

        except Exception as e:
            self.logger.log_info('ScraperEngine',
                                 f'Error during scrapping - {e}')
            return pd.DataFrame(columns=self.get_default_cols()).astype(self.get_default_types())

    @staticmethod
    def get_default_cols():
        return ['url', 'html']

    @staticmethod
    def get_default_types():
        return {'url': 'str', 'html': 'str'}


if __name__ == '__main__':
    # Dev testing
    app_location = io_lib.app_location_path('app')
    config_file = io_lib.get_config_file(app_location, ending='yaml/engine_config.yml')

    _engine_config = EngineConfig(config_file)

    _logger = ClassLogger.get_cls_logger()
    _scraper_engine = ScrapeEngine(_logger, _engine_config)

    _html = 'https://www.bbc.com/weather'
    _text = _scraper_engine.run_engine_single(_html)
