from setuptools import setup

setup(name='gdelt_util',
      version='0.11',
      description='Utilites exploring GDELT data set',
      url='http://github.com/davefol/gdelt_util',
      author='Dave Fol',
      author_email='dof5@cornell.edu',
      license='GNU',
      packages=['gdelt_util'],
      install_requires=['pandas','numpy','scipy','matplotlib','seaborn'],
      zip_safe=False)