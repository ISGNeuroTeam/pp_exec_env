from setuptools import setup

with open("requirements.txt", "r") as file:
    deps = file.readlines()

setup(name='pp_exec_env',
      version='0.1.0',
      description='Postprocessing Execution Environment',
      author='Alexander Lakeev',
      author_email='alakeev@isgneuro.com',
      packages=['pp_exec_env'],
      package_dir={'pp_exec_env': 'pp_exec_env'},
      zip_safe=False,
      install_requires=deps
      )
