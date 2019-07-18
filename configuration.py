from configparser import ConfigParser
from logger import logger


class Config(object):
    """
    配置文件类
    """
    def __init__(self):
        self.cp = ConfigParser()
        self.cp.read("config.cfg")
        self.logger = logger
        self.logger.info("Config file is loaded")

    def csv_zone(self):
        return self.cp.sections()[0]

    def zip_zone(self):
        return self.cp.sections()[1]

    @property
    def csv_model_path(self):
        path = self.cp.get(self.csv_zone(), "csv_model_path")
        return path

    @property
    def new_csv_dir(self):
        path = self.cp.get(self.csv_zone(), "new_csv_dir")
        return path

    @property
    def resource_csv(self):
        path = self.cp.get(self.csv_zone(), "resource_csv")
        return path

    @property
    def partNos_count(self):
        count = self.cp.get(self.csv_zone(), "partNos_count")
        return count

    @property
    def asc_codes_count(self):
        count = self.cp.get(self.csv_zone(), "asc_codes_count")
        return count



