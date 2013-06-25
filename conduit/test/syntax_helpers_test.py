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
        clear_graph()
        increment_block = GeneratorBlock(incremental_generator_with_output)
        increment_block.set_debug_name('increment_block')
        verification_block = GeneratorBlock(verify_array)
        verification_block.set_debug_name('verification_block')
        increment_block('value') >> verification_block('value')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['increment_block','verification_block']))