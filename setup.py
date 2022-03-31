from setuptools import setup

#
# pp_exec_env is not supposed to be distributed as a library, thus use this only if needed during development.
#

with open("requirements.txt", "r") as file:
    deps = file.readlines()

setup(name='pp_exec_env',
      version='1.0.3',
      description='Postprocessing Execution Environment',
      author='Alexander Lakeev',
      author_email='alakeev@isgneuro.com',
      packages=['pp_exec_env'],
      package_dir={'pp_exec_env': 'pp_exec_env'},
      zip_safe=False,
      install_requires=deps
      )
