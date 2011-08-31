
import os, sys, platform
import doze
from setuptools import setup, find_packages

requires = ['setuptools']

setup(
    name='Doze',
    version=doze.__version__,
    description='A lightweight SQL wrapper, ideal for import scripts',
    author='Cameron Eure',
    author_email='cleure@websprockets.com',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requires)
