import configparser
import datetime
import os

# 读取配置文件
def cf(section, option):
    """
    :param filename 文件名称
    :param section: 服务
    :param option: 配置参数
    :return:返回配置信息
    """# 获取当前目录路径
    proDir = os.path.split(os.path.realpath(__file__))[0]
    # print(proDir)

    # 拼接路径获取完整路径
    configPath = os.path.join(proDir, 'config.ini')
    # print(configPath)

    # 创建ConfigParser对象
    conf = configparser.ConfigParser(allow_no_value=True)

    # 读取文件内容
    conf.read(configPath)
    config = conf.get(section, option)
    return config

def dic_config():
    dic = {
        "time1":str(datetime.datetime.now()),
    'time2' :str(datetime.datetime.now()).split('.')[0],
    'time3' : str(datetime.datetime.now()).split('.')[0].split(' ')[0]
    }

    # 获取当前路劲

    current_path = os.getcwd()
    current_config_path = current_path + '\\' + 'config.ini'
    current_log_path = current_path + '\\' + 'log.txt'
    current_database_backup_path = current_path + '\\' + 'database_backup'