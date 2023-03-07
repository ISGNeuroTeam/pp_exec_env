from setuptools import setup


try:
    with open('requirements.txt') as f:
        requirements = f.readlines()
except OSError:
    requirements = []

setup(name='pp_exec_env',
      version='1.4.7',
      description='Postprocessing Execution Environment',
      author='Alexander Lakeev',
      author_email='alakeev@isgneuro.com',
      packages=['pp_exec_env'],
      package_dir={'pp_exec_env': 'pp_exec_env'},
      zip_safe=False,
      install_requires=requirements,
      )
