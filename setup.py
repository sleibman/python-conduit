from setuptools import setup
import os

setup(
    name = "conduit",
    version = "0.0.3",
    author = "Steve Leibman",
    author_email = "sleibman@alum.mit.edu",
    description = ("Framework for dataflow-style python programming"),
    license = "MIT",
    keywords = "dataflow distributed pipe flow programming",
    url = "https://github.com/sleibman/python-conduit",
    packages = ['conduit',
                'conduit.util',
                'conduit.test'],
    long_description = """ 
python-conduit
==============

Python framework for dataflow-style programs.

Users of this framework structure their code into blocks with named inputs and outputs that are connected
by channels. A typical application will have one or more data generator/importer blocks which then pass 
their data through various blocks which apply filters or tranforms to operate on the data.

For other similar projects, see: http://wiki.python.org/moin/FlowBasedProgramming

License
-------

conduit is free software and is released under the terms
of the MIT license (<http://opensource.org/licenses/mit-license.php>),
as specified in the accompanying LICENSE.txt file.
    """,
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
