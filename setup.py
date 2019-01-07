import os
from setuptools import setup, find_packages

base_dir = os.path.dirname(__file__)
install_requires = [line.rstrip() for line in open(os.path.join(base_dir, 'requirements.txt'))]

setup(
    name = 'hca_ingest',
    version = '0.6.2',
    packages = find_packages(exclude=['tests', 'tests.*']),
    install_requires = install_requires,
    include_package_data = True
)
