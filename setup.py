# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from actree import __NAME__, __VERSION__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name=__NAME__,
    version=__VERSION__,
    packages=find_packages(),
    test_suite='tests',
    url='https://github.com/bharathra/actee',
    project_urls={
        "Bug Tracker": "https://github.com/bharathra/actee/issues",
    },
    license='MIT',
    author='Bharath Achyutha Rao',
    author_email='bharath.rao@hotmail.com',
    description='Autonomous agent for task/action planning and execution',
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ],
)
