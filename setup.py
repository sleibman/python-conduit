from setuptools import setup
import os

with open('README.md') as file:
    long_description = file.read()

setup(
    name = "conduit",
    version = "0.0.8",
    author = "Steve Leibman",
    author_email = "sleibman@alum.mit.edu",
    description = ("Framework for dataflow-style python programming"),
    license = "MIT",
    keywords = "dataflow distributed pipe flow programming",
    url = "https://github.com/sleibman/python-conduit",
    packages = ['conduit',
                'conduit.util',
                'conduit.test'],
    include_package_data = True,
    package_data = {'conduit': ['README.md', 'LICENSE.txt']},
    long_description = long_description,
    test_suite = 'conduit.test',
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities",
    ],
)
