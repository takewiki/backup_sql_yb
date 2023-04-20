from setuptools import setup, find_packages

setup(name='backup',
      version='1.0.6',
      description='test for zip',
      author='yb',
      # package_data={'': ['config.ini']},
      include_package_data=True,
      author_email='623374149@qq.com',
      packages=['backup'],
      install_requires=[
          'oss2', 'pymssql'
      ]
      # packages=crmMaterial()可替换上个packages,可自动寻找到所有文件，当promax下有多文件夹时
      # 可用
      )
