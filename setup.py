import os
from xml.dom.minidom import parse
from setuptools import find_packages, setup

doc = parse("pom.xml")
ele = doc.getElementsByTagName("version")[0]
ver = ele.firstChild.nodeValue

where = os.path.join("src", "main", "python")

setup(
    name='filegdb',
    version=ver,
    description='FileGDB',
    author='Mansour Raad',
    author_email='mraad@esri.com',
    python_requires='>=3.6',
    packages=find_packages(where=where),
    package_dir={'': where}
)
