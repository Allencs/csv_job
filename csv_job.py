import csv
import os
import threading
import traceback
import time
from logger import ErrorInfo
from logger import logger
from configuration import Config
from zip import ZIP
import queue


class CSVJob(object):
    _csv_files = set()
    """待处理csv文件"""

    _new_csv_files = set()
    """新生成csv文件"""

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

    tozip_file_queue = queue.Queue(maxsize=-1)
    """待压缩文件队列"""

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
        self.clear_dirs()

        print("".join(["partNos count:".ljust(20, ".") + str(len(self._partNos)).rjust(8) + "\n",
                       "asc_code count:".ljust(20, ".") + str(len(self._asc_codes)).rjust(8) + "\n",
                       "csv model lines:".ljust(20, ".") + str(len(self._csv_datas)).rjust(8)]))

        if not os.path.exists(self.new_file_dir):
            os.mkdir(self.new_file_dir)
        if int(self.asc_codes_count) <= 400:
            asc_count = 0

            for asc_code in self._asc_codes[::-1]:

                if asc_count >= int(self.asc_codes_count):
                    break

                csv_datas = self._csv_datas.copy()
                csv_name = asc_code + self.csv_model_name
                fullpath_csv_name = os.path.join(self.new_file_dir, csv_name)

                with open(fullpath_csv_name, 'w+', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    lines = 0
                    for count, row in enumerate(csv_datas):
                        if count == 1:
                            row[0] = asc_code
                        elif count > 2:
                            row[0] = self._partNos[lines]
                        if lines >= int(self.partNos_count) + 3:
                            break
                        writer.writerow(row)
                        lines += 1

                self._new_csv_files.add(fullpath_csv_name)
                asc_count += 1
                csv_datas.clear()

                self.logger.info("{} is done".format(csv_name))

        else:
            _asc_count = 0
            for _asc_code_ in self._asc_codes:

                _csv_name = _asc_code_ + self.csv_model_name
                _csv_datas_ = self._csv_datas.copy()

                if _asc_count >= int(self.asc_codes_count):
                    break

                _fullpath_csv_name = os.path.join(self.new_file_dir, _csv_name)

                with open(os.path.join(self.new_file_dir, _csv_name), 'w+', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    lines_2 = 0
                    for count, row in enumerate(_csv_datas_):
                        if count == 1:
                            row[0] = _asc_code_
                        elif count > 2:
                            row[0] = self._partNos[lines_2]
                        if lines_2 >= int(self.partNos_count) + 3:
                            break
                        writer.writerow(row)
                        lines_2 += 1

                self._new_csv_files.add(_fullpath_csv_name)
                _asc_count += 1
                _csv_datas_.clear()
                self.logger.info("{} is done".format(_csv_name))

        self.logger.info("{} thread is done".format(threading.currentThread().getName()))

    """
    def search_csvfile(self, dirpath):
        for root, dirs, files in os.walk(dirpath):
            for file in files:
                if os.path.splitext(file)[1] == '.csv':
                    full_path_name = os.path.join(root, file)
                    self._csv_files.add(full_path_name)
    """

    @ErrorInfo
    def clear_dirs(self):
        """
        删除自动生成的文件中的内容，避免出现脏文件
        :return: None
        """
        if os.path.exists(self.new_file_dir):
            for file in os.listdir(self.new_file_dir):
                os.remove(os.path.join(self.new_file_dir, file))
            self.logger.info("directory <new_csv> is cleared")

        if os.path.exists(ZIP.default_dir):
            for zip_file in os.listdir(ZIP.default_dir):
                os.remove(os.path.join(ZIP.default_dir, zip_file))
            self.logger.info("directory <CSV_ZIP> is cleared")

    def add_zipfiles(self):
        """
        添加待压缩CSV文件(该方法用于非同步队列)
        """
        files = os.listdir(self.new_file_dir)
        for file in files:
            full_filepath = os.path.join(self.new_file_dir, file)
            if full_filepath not in self._already_zipped:  # 若已经压缩，则不加入集合
                self._zip_files.add(full_filepath)

    @ErrorInfo
    def check_line(self, thread):
        """
        检查生成的csv文件行数，将正确的csv文件放入待压缩队列
        :param thread: generate_new_csv线程
        :return: None
        """
        check_list = []
        while True:
            if len(self._new_csv_files) == 0:
                if threading.Thread.isAlive(thread):
                    time.sleep(1)
                else:
                    break
            else:
                fullpath_new_csv = self._new_csv_files.pop()
                with open(fullpath_new_csv, 'r', newline='', encoding='utf-8') as csvfile:
                    csv_reader = csv.reader(csvfile, dialect='excel')
                    line_num = 0
                    while True:
                        try:
                            csv_reader.__next__()
                            line_num += 1
                        except StopIteration:
                            break

                    if not line_num == int(self.partNos_count) + 3:
                        check_list.append(fullpath_new_csv)
                        print("******ERROR CSVFIE FOUND******")
                        with open("error_csvfiles.log", "a+", encoding='utf-8') as f:
                            f.write(fullpath_new_csv + "\n")
                    else:
                        self.tozip_file_queue.put(fullpath_new_csv)

        if len(check_list) == 0:
            self.logger.info("csv files are checked, no errors")

    def zip(self):
        """
        压缩文件，从队列中取出检查无误的csv文件进行压缩处理
        :return: None
        """
        zip = ZIP()
        self.logger.info("zip thread start")

        while True:
            try:
                tozip_file = self.tozip_file_queue.get(True, 2)
            except queue.Empty:
                break
            else:
                if tozip_file not in self._already_zipped:
                    zip(tozip_file)
                    self._already_zipped.add(tozip_file)

        """
        # 用于非同步队列               
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
        """

        self.logger.info("all tasks done")
        time_used = int((time.time() - self.start_time))
        self.logger.info("spend time: {}s".format(time_used))
        time.sleep(10)

    def start(self):
        threads = []
        t1 = threading.Thread(target=self.generate_new_csv, name="generate_new_csv")
        t2 = threading.Thread(target=self.zip, name="zip")
        t3 = threading.Thread(target=self.check_line, args=(t1,), name="check_line")
        threads.append(t1)
        threads.append(t2)
        threads.append(t3)
        for thread in threads:
            thread.start()


if __name__ == '__main__':
    csv_job = CSVJob()
    csv_job.start()
