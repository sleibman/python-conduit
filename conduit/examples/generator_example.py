from conduit import *

"""
This example demonstrates the use of a python generator function wrapped in a Conduit block.
It will print out numbers 0 through 9.
In this particular case, there is only one block, not connected to any others.
"""

def incremental_generator():
    value = 0
    while value < 10:
        print value
        value += 1
        yield

increment_block = GeneratorBlock(incremental_generator)
graph = Graph(increment_block)
graph.run()

