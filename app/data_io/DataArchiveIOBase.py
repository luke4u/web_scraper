from abc import ABC, abstractmethod

from common.lib.Logger import ClassLogger


class DataArchiveIOBase(ABC):
    logger = ClassLogger.get_cls_logger()

    def __init__(self, logger):
        self.set_logger(logger)

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    @abstractmethod
    def write_archive_data(self, data, data_tag):
        pass

    @abstractmethod
    def get_archive_data(self, data_tag):
        pass
