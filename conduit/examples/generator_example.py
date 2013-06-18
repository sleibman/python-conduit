from conduit import *

"""
This example demonstrates the use of a python generator function wrapped in a Conduit block.
It will print out numbers 0 through 4.
In this particular case, there is only one block, not connected to any others.
"""

def incremental_generator():
    value = 0
    while value < 5:
        print value
        value += 1
        yield

print "Generator block printing numbers 0-4:"
increment_block = GeneratorBlock(incremental_generator)
graph = Graph(increment_block)
graph.run()

"""
A second example that shows a generator block providing data to a downstream block.
This time, it prints numbers 5 through 9.
"""

def incremental_generator_with_output():
    value = 5
    while value < 10:
        yield {'value': value}
        value += 1

def printer(value):
    print value

print "Generator block passing numbers 5-9 to printer block:"
increment_block = GeneratorBlock(incremental_generator_with_output)
printer_block = PassThrough(printer)
increment_block('value') >> printer_block('value')
graph = Graph(increment_block)
graph.run()
