import os
import untangle  # conda install -c conda-forge untangle
from setuptools import find_packages, setup

pom = untangle.parse("pom.xml")
version = pom.project.version.cdata

where = os.path.join("src", "main", "python")
setup(
    name='filegdb',
    version=version,
    description='FileGDB',
    author='Mansour Raad',
    author_email='mraad@esri.com',
    python_requires='>=3.6',
    packages=find_packages(where=where),
    package_dir={'': where}
)
