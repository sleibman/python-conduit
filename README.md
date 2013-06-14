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

