import csv
import os
import random
import sys
import threading
import traceback
import time
from logger import ErrorInfo
from logger import logger
from configuration import Config
from zip import ZIP


class CSVJob(object):

    _csv_files = set()
    """待处理csv文件"""

    _csv_datas = []
    """csv行列表"""

    _zip_files = set()
    """待压缩文件"""

    _asc_codes = []
    """"ASC_Code"""

    _partNos = []
    """配件代码"""

    _already_zipped = set()
    """已压缩文件"""

    def __init__(self):
        cnf = Config()
        self.csv_model_path = cnf.csv_model_path
        self.new_file_dir = cnf.new_csv_dir
        self.resource_csv = cnf.resource_csv
        self.partNos_count = cnf.partNos_count
        self.asc_codes_count = cnf.asc_codes_count
        self.logger = logger
        self.csv_model_name = None
        self.start_time = time.time()

        self.load_resource_csv()
        self.load_csv_model()

    @ErrorInfo
    def load_resource_csv(self):
        """
        加载资源文件，读取ASC_Code，partNo
        :return: None
        """
        self.logger.info("load resource csv")
        resource_files = os.listdir(self.resource_csv)
        for file in resource_files:
            if "asc" in file:
                with open(os.path.join(self.resource_csv, file), 'r', newline='') as csvfile:
                    csv_reader = csv.reader(csvfile, dialect='excel')
                    for row in csv_reader:
                        if csv_reader.line_num > 1 and int(row[1]) > 11:
                            self._asc_codes.append(row[0])

            elif "partno" in file:
                with open(os.path.join(self.resource_csv, file), 'r', newline='', encoding='utf-8') as csvfile:
                    csv_reader = csv.reader(csvfile, dialect='excel')
                    for row in csv_reader:
                        if csv_reader.line_num > 1:
                            self._partNos.append(row[0])

    @ErrorInfo
    def load_csv_model(self):
        """
        加载模板文件
        :return: None
        """
        self.csv_model_name = os.listdir(self.csv_model_path)[0]
        filepath = os.path.join(self.csv_model_path, self.csv_model_name)
        self.logger.info("load csv model")
        with open(filepath, 'r', newline='') as csvfile:
            csv_reader = csv.reader(csvfile, dialect='excel')
            for row in csv_reader:
                self._csv_datas.append(row)

    @ErrorInfo
    def generate_new_csv(self):
        """
        将模板数据的ASC_Code和partNo使用资源数据进行替换（partNo从资源中随机抽取），
        生成以ASC——Code为前缀的CSV文件
        :return: None
        """
        print("------partNo个数：{}".format(len(self._partNos)))
        print("------asc_code: {}".format(len(self._asc_codes)))
        if not os.path.exists(self.new_file_dir):
            os.mkdir(self.new_file_dir)
        if int(self.asc_codes_count) <= 400:
            asc_count = 0

            for asc_code in self._asc_codes[::-1]:

                if asc_count >= int(self.asc_codes_count):
                    break

                csv_datas = self._csv_datas.copy()
                csv_name = asc_code + self.csv_model_name

                with open(os.path.join(self.new_file_dir, csv_name), 'w+', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    lines = 0
                    for count, row in enumerate(csv_datas):
                        if count == 1:
                            row[0] = asc_code
                        elif count > 2:
                            row[0] = self._partNos[count]
                        if lines >= int(self.partNos_count) + 3:
                            break
                        writer.writerow(row)
                        lines += 1

                asc_count += 1
                self.logger.info("{} is done".format(csv_name))

        else:
            asc_count = 0
            for asc_code in self._asc_codes:
                csv_name = asc_code + self.csv_model_name

                if asc_count >= int(self.asc_codes_count):
                    break

                with open(os.path.join(self.new_file_dir, csv_name), 'w+', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    lines = 0
                    for count, row in enumerate(self._csv_datas):
                        if count == 1:
                            row[0] = asc_code
                        elif count > 2:
                            row[0] = self._partNos[count]
                        if lines >= int(self.partNos_count) + 3:
                            break
                        writer.writerow(row)
                asc_count += 1
                self.logger.info("{} is done".format(csv_name))
        self.logger.info("{} thread is done".format(threading.currentThread().getName()))

    """
    def search_csvfile(self, dirpath):
        for root, dirs, files in os.walk(dirpath):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    full_path_name = os.path.join(root, file)
                    self._csv_files.add(full_path_name)
    """

    def add_zipfiles(self):
        """
        添加待压缩CSV文件
        """
        files = os.listdir(self.new_file_dir)
        for file in files:
            full_filepath = os.path.join(self.new_file_dir, file)
            if full_filepath not in self._already_zipped:  # 若已经压缩，则不加入集合
                self._zip_files.add(full_filepath)

    def zip(self, thread):
        """
        压缩文件，如果thread线程未结束，等待1s继续查找添加CSV文件
        :param thread: CSV文件处理线程
        :return: None
        """
        zip = ZIP()
        self.logger.info("zip thread start")
        while True:
            self.add_zipfiles()
            try:
                if len(self._zip_files) == 0:
                    if threading.Thread.isAlive(thread):
                        time.sleep(1)
                    else:
                        break
                tozip_file = self._zip_files.pop()
                if tozip_file not in self._already_zipped:
                    zip(tozip_file)
                    self._already_zipped.add(tozip_file)
            except Exception:
                print("{}\n".format("zip") + traceback.format_exc())
        self.logger.info("all tasks done")
        time_used = int((time.time() - self.start_time))
        self.logger.info("spend time: {}s".format(time_used))
        time.sleep(10)

    def start(self):
        threads = []
        t1 = threading.Thread(target=self.generate_new_csv, name="generate_new_csv")
        t2 = threading.Thread(target=self.zip, args=(t1,), name="zip")
        threads.append(t1)
        threads.append(t2)
        for thread in threads:
            thread.start()


if __name__ == '__main__':
    csv_job = CSVJob()
    csv_job.start()








