from setuptools import setup, find_packages

setup(name='pp_exec_env',
      version='0.1',
      description='Postprocessing Execution Environment',
      author='Alexander Lakeev',
      author_email='alakeev@isgneuro.com',
      packages=find_packages(),
      package_dir={'pp_exec_env': 'pp_exec_env'},
      zip_safe=False,
      install_requires=[
          "execution_environment @ git+ssh://git@github.com/ISGNeuroTeam/execution_environment.git"],
      )
