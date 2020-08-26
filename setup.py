import os
from setuptools import find_packages, setup

where = os.path.join("src", "main", "python")
setup(
    name='filegdb',
    version='0.38',  # TODO Read it from pom.xml
    description='FileGDB',
    author='Mansour Raad',
    author_email='mraad@esri.com',
    python_requires='>=3.6',
    packages=find_packages(where=where),
    package_dir={'': where}
)
