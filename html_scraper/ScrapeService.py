import time
from datetime import datetime
from typing import Union

import pandas as pd

from html_scraper.ScrapeEngine import ScrapeEngine
from html_scraper.SearchEngine import SearchEngine
from html_scraper.data_io.FileDataArchiveIO import FileDataArchiveIO
from html_scraper.data_readers.ScrapeTermReader import ScrapeTermReader
from html_scraper.config.EngineConfig import EngineConfig
from html_scraper.lib import io_lib, data_lib
from html_scraper.lib.Logger import ClassLogger, StandardLogger


class ScrapeService(object):
    logger = ClassLogger.get_cls_logger()

    def __init__(self, engine_config: EngineConfig, logger: Union[ClassLogger, StandardLogger]):
        self.set_logger(logger)
        self.engine_config = engine_config

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    def run(self):
        t0 = time.time()
        self.logger.log_info('ScrapeService', f'Starting ScrapeService'
                                              f'in {self.engine_config.get_mode().upper()} mode')
        app_location = io_lib.app_location_path('html_scraper')
        term_data_location = io_lib.get_source_data_file(app_location, 'term_data.csv')

        self.logger.log_info('ScrapeService', f'Starting ScrapeTermReader')
        data_reader = ScrapeTermReader(term_data_location, self.logger)
        term_data = data_reader.extract()

        self.logger.log_info('ScrapeService', f'Starting SearchEngine')
        search_engine = SearchEngine(self.logger, self.engine_config)
        url = search_engine.run(term_data)

        self.logger.log_info('ScrapeService', f'Starting ScrapeEngine')
        scrape_engine = ScrapeEngine(self.logger, self.engine_config)
        html = scrape_engine.run_engine(url)

        result = self.aggregate_instance_data(url, html)

        self.logger.log_info('ScrapeService', f'Starting FileDataArchiveIO')
        snap_time = datetime.now().strftime(io_lib.time_format())
        file_tag = f'snap_data_{snap_time}'
        archive_tag = f'instance1'
        archive_path = io_lib.get_archive_path(app_location, archive_tag)
        file_data_data_io = FileDataArchiveIO(self.engine_config, archive_path, self.logger)
        file_data_data_io.write_archive_data(result, file_tag=file_tag)

        t1 = time.time()
        self.logger.log_info('ScrapeService', f'Took {t1 - t0} seconds to complete')
        self.logger.log_info('ScrapeService', f'Ending ScrapeService ...')

    def aggregate_instance_data(self, url, html):
        if url.empty or url is None:
            return data_lib.get_default_instance_data()

        result = (url
                  .merge(html, on=['url'], how='outer', indicator=True))

        self.flag_terms_with_url_but_no_html(result[(result['html'].isna()) | (result['_merge'] == 'left_only')])

        return result.filter(data_lib.instance_data_columns())

    def flag_terms_with_url_but_no_html(self, data: pd.DataFrame):
        try:
            if data.empty:
                return
            message = (f'Flag {data.shape[0]} term record(s) with url but no html \n '
                       f'{data}')
            self.logger.log_info('ScrapeService', message)

        except Exception as e:
            self.logger.log_info('ScrapeService',
                                 f'Error raised while flagging term data with no html')
            raise e
