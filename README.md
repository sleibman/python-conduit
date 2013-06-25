python-conduit
==============

Python framework for dataflow-style programs.

Users of this framework structure their code into blocks with named inputs and outputs that are connected
by channels. A typical application will have one or more data generator/importer blocks which then pass 
their data through various blocks which apply filters or tranforms to operate on the data.

For other similar projects, see: http://wiki.python.org/moin/FlowBasedProgramming

# License #

conduit is free software and is released under the terms
of the MIT license (<http://opensource.org/licenses/mit-license.php>),
as specified in the accompanying LICENSE.txt file.

# Procedures #

Contributors: Submit pull requests on github to request that your changes be added to the repository.

Maintainers: To release a new version:
1. Increment release number in setup.py
2. Run 'python setup.py sdist' to create a new distribution.
3. Using a virtualenv, run 'pip install dist/conduit-<version>.tar.gz' to locally install for testing purposes.
4. Run the nose tests via:
     % cd <virtualenv_dir>/lib/python2.7/site-packages/conduit/test
     % nosetests
5. Upload new package via:
     % python setup.py sdist upload 
