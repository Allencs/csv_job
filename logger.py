import logging
import traceback


class Logger(object):
    def __init__(self, name, clevel=logging.DEBUG, flevel=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_name = "{}.log".format(name)

        sh = logging.StreamHandler()
        sh.setLevel(clevel)
        sh.setFormatter(fmt)

        fh = logging.FileHandler(log_name)
        fh.setFormatter(fmt)
        fh.setLevel(flevel)

        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.error(message)

    def warning(self, message):
        self.warning(message)


logger = Logger("csv_job")


def error_log(func):
    def inner_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            with open("error_info.log", 'a+', encoding='utf-8') as f:
                f.write("{}\n".format(func.__name__) + traceback.format_exc() + "\n\t")
    return inner_func






