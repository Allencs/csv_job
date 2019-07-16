import os
import zipfile


class ZIP(object):

    desktop = os.path.join(os.path.expanduser("~"), 'Desktop')
    default_dir = os.path.join(desktop, "CSV_ZIP")

    def __init__(self):
        if not os.path.exists(self.default_dir):
            os.mkdir(self.default_dir)
        self.dirpath = self.default_dir

    def __call__(self, *args):
        """
        此处参数须输入绝对路径
        """
        filename = os.path.split(*args)[1]  # 文件名（带后缀）
        zip_filename = str(filename.split('.')[0]) + ".zip"
        f = zipfile.ZipFile(os.path.join(self.dirpath, zip_filename), 'w', zipfile.ZIP_DEFLATED)
        f.write(*args, filename)
        f.close()


