"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from codecs import open
from os import path

from setuptools import find_packages, setup

import radish


REQUIREMENTS_FILE_PATH = path.join(path.abspath(path.dirname(__file__)), "requirements.txt")

with open(REQUIREMENTS_FILE_PATH, "r") as f:
    REQUIREMENTS_FILE = [
        line
        for line in f.read().splitlines()
        if not line.startswith("#") and not line.startswith("--")
    ]

with open("README.md", "r") as readme_file:
    LONG_DESCRIPTION = readme_file.read()

setup(
    name="radish",
    version=radish.__version__,
    description="Pythonic Redis interface with support for asyncio (PEP 3156) and type annotations (PEP 484)",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/jacksmith15/radish",
    author="Jack Smith",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.6",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=REQUIREMENTS_FILE,
    dependency_links=[],
)
