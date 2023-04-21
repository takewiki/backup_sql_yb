import pymssql
import datetime
import oss2
import os


class SqlServer():

    def __init__(self, host, port, userName, password, databaseName, ossKey, ossSecret, ossEntry, bucketName):
        """
        初始化
        :param host: 数据库地址
        :param port: 数据库端口号
        :param userName: 数据库用户名
        :param password: 数据库密码
        :param databaseName: 数据库名称
        :param ossKey: 阿里云KEY
        :param ossSecret: 阿里云Secret
        :param ossEntry: 阿里云入库
        :param bucketName: 阿里云仓储
        """
        self.time1 = str(datetime.datetime.now())
        self.time2 = self.time1.split('.')[0]
        self.time3 = self.time2.split(' ')[0]
        self.timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
        self.database_host = host  # 得到hose
        self.database_port = port  # 得到端口号
        self.database_user = userName  # 得到登录数据的用户名
        self.database_password = password  # 得到登录数据库的密码
        self.databaseName = databaseName  # 得到你想要备份的数据库名称
        self.con = pymssql.connect(host=self.database_host, database=self.databaseName, user=self.database_user,
                                   port=self.database_port,
                                   password=self.database_password, charset='utf8')
        self.cursor = self.con.cursor(as_dict=True)
        self.auth = oss2.Auth(ossKey, ossSecret)
        self.bucket = oss2.Bucket(self.auth, ossEntry, bucketName)
        self.folder_name = 'backup_{}'.format(self.time3)

    def sql_backupDiff(self, dirPath, date, time):
        """
        增量还原
        :param dirPath: 备份文件路劲
        :param date: 日期
        :param time: 时间
        :return:
        """
        try:

            day = date[:4] + date[5:7] + date[8:10]
            sql = r"BACKUP DATABASE  {} TO DISK='{}\metadata_{}_{}.bak' WITH DIFFERENTIAL".format(
                self.databaseName, dirPath, day, time)
            self.con.autocommit(True)
            self.cursor.execute(sql)
            self.con.commit()
            self.con.autocommit(False)

            # 填写本地文件的完整路径。如果未指定本地路径，则默认从示例程序所属项目对应本地路径中上传文件。
            with open('{}\metadata_{}_{}.bak'.format(dirPath, day, time), 'rb') as fileobj:
                res = self.bucket.put_object("{}/".format(self.time3) + 'metadata_{}_{}.bak'.format(day, time),
                                             fileobj)
                fileobj.close()
                return {"status": True, "message": "OK",
                        "result": [{
                            "fileName": "{}/".format(self.time3) + 'metadata_{}_{}.bak'.format(day, time),
                            "ETag": res.etag,
                            "URL": res.resp.response.url,
                        }]}
        except Exception as e:
            return {"status": False, "message": "ERROR", "result": str(e)}

    def sql_backupAll(self, dirPath, date, time):
        """
        全量备份-备份到数据库本地服务器
        :param databaseName: 数据库名称
        :param path: 备份文件路劲
        :param time3: 时间 年-月-日
        :return:
        """
        try:
            day = date[:4] + date[5:7] + date[8:10]
            sql_master = 'use master'
            sql = r"BACKUP DATABASE {} TO DISK='{}\metadata_{}_{}.bak'".format(self.databaseName, dirPath, day, time)
            self.con.autocommit(True)
            self.cursor.execute(sql_master)
            self.cursor.execute(sql)
            self.con.commit()
            self.con.autocommit(False)

            with open('{}\metadata_{}_{}.bak'.format(dirPath, day, time), 'rb') as fileobj:
                res = self.bucket.put_object("{}/".format(self.time3) + 'metadata_{}.bak'.format(day), fileobj)
                fileobj.close()

                return {"status": True, "message": "OK",
                        "result": [{
                            "fileName": "{}/".format(self.time3) + 'metadata_{}.bak'.format(day),
                            "ETag": res.etag,
                            "URL": res.resp.response.url,
                        }]}
                # return {"message": "OK", "res": "backup_{}/".format(self.time3) + 'metadata_{}.bak'.format(day)}
        except Exception as e:
            return {"status": False, "message": "ERROR", "result": str(e)}

    def sql_restoreAll(self, oss_fileName, fileName):
        """
        全量还原
        :param oss_fileName: oss文件路劲
        :param fileName: 全量还原文件
        :return:
        """
        try:
            self.load_fileName(oss_fileName, fileName)
            sql_master = 'use master'
            sql = r"RESTORE DATABASE {} FROM DISK = '{}' WITH  RECOVERY, REPLACE".format(
                self.databaseName, fileName)
            self.con.autocommit(True)
            self.cursor.execute(sql_master)
            self.cursor.execute(sql)
            self.con.commit()
            self.con.autocommit(False)
            return {"status": True, "message": "OK",
                    "result": "数据库{}全量还原成功".format(self.databaseName)}
        except Exception as e:
            return {"status": False, "message": "ERROR", "result": str(e)}

    def sql_restoreDiff(self, oss_fileName, fileNameAll, fileNameDiff, overWrite=True):
        """
        增量还原
        :param oss_fileName: oss文件路劲
        :param file: 全量还原bak文件
        :param fileName: 增量还原bak文件
        :param overWrite: 是否覆盖
        :return:
        """

        try:
            self.load_fileName(oss_fileName, fileNameAll)
            self.load_fileName(oss_fileName, fileNameDiff)
            sql_master = 'use master'

            sql_total = r"RESTORE DATABASE {} FROM DISK = '{}' WITH  NORECOVERY, REPLACE".format(
                self.databaseName, fileNameAll)
            if overWrite:
                sql_add = r"RESTORE DATABASE {} FROM DISK = '{}' WITH RECOVERY".format(
                    self.databaseName, fileNameDiff)
            else:
                sql_add = r"RESTORE DATABASE {} FROM DISK = '{}' WITH NORECOVERY".format(
                    self.databaseName, fileNameDiff)
            self.con.autocommit(True)
            self.cursor.execute(sql_master)
            self.cursor.execute(sql_total)
            self.cursor.execute(sql_add)
            self.con.commit()
            self.con.autocommit(False)
            # os.remove('{}'.format(fileNameDiff))
            return {"status": True, "message": "OK",
                    "result": "数据库{}增量还原成功".format(self.databaseName)}
        except Exception as e:
            return {"status": False, "message": "ERROR", "result": str(e)}

    def load_fileName(self, fileName, load_path):
        """
        从oss2下载文件
        :param fileName: 文件路劲
        :param load_path: 下载到本地路劲
        :return:
        """
        dir_path = ''
        path = load_path.split('\\')
        for i in path[:-1]:
            dir_path += i + '\\'
        print(dir_path)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)
        try:
            res = self.bucket.get_object_to_file(fileName, load_path)  # 从 oss2 下载文件
            return {"message": "OK", 'result': res.headers}
        except Exception as e:
            return {"status": False, "message": "ERROR", "result": str(e)}
