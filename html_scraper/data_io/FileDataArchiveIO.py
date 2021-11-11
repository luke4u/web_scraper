import pandas as pd

from html_scraper.data_io.DataArchiveIOBase import DataArchiveIOBase
from html_scraper.config.EngineConfig import EngineConfig
from html_scraper.lib import io_lib
from html_scraper.lib.Logger import ClassLogger


class FileDataArchiveIO(DataArchiveIOBase):
    def __init__(self, engine_config: EngineConfig, archive_path, logger: ClassLogger):
        super().__init__(logger)
        self.archive_path = archive_path
        self.engine_config = engine_config

    def write_archive_data(self, data, file_tag):
        try:
            self.write_generic_archive_data(self.archive_path, data, file_tag)
        except Exception as e:
            self.logger.log_info('FileDataArchiveIO',
                                 f'Error writing archive data file {file_tag}: {e}')
            raise e

    def write_generic_archive_data(self, archive_path, data, file_tag):
        try:
            archive_file = self.get_archive_file_name(archive_path, file_tag)
            if self.engine_config.is_dev():
                data.to_csv(archive_file[:-10] + '.csv', index=False)
            else:
                io_lib.to_parquet(data, archive_file)

        except Exception as e:
            self.logger.log_info('FileDataArchiveIO',
                                 f'Error writing archive data file {file_tag}: {e}')
            raise e

    @staticmethod
    def get_archive_file_name(archive_path, file_tag):
        archive_file = f'{file_tag}.parg.gzip'
        file_name = io_lib.get_path(archive_path, archive_file)
        return file_name

    def get_archive_data(self, file_tag):
        try:
            archive_file_name = self.get_archive_file_name(self.archive_path, file_tag)
            if self.engine_config.is_dev:
                result = pd.read_csv(archive_file_name[:-9] + '.csv', index=False)
            else:
                result = io_lib.from_parquet(archive_file_name)
            return result
        except Exception as e:
            self.logger.log_info('FileDataArchiveIO',
                                 f'Error reading archive data file {file_tag}: {e}')
            raise e
