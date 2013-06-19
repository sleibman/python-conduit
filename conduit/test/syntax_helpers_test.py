from conduit import *
import nose


def verify_array(value):
    inputs = {'value': value}
    array = [0,1,2,3]
    for element in array:
        nose.tools.assert_equal(inputs['value'], element)
        inputs = yield


def incremental_generator_with_output():
    array = [0,1,2,3]
    for value in array:
        yield {'value': value}


def printer(value):
    print value


class TestGreaterThanGreaterThan(object):

    def test_one_channel(self):
        """
        Tests the ability to connect two blocks together in the form:
        block(channel) >> block(channel)
        """
        increment_block = GeneratorBlock(incremental_generator_with_output)
        verification_block = GeneratorBlock(verify_array)
        increment_block('value') >> verification_block('value')
        graph = Graph(increment_block)
        graph.run()