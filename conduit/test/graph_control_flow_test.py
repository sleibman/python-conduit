
from conduit import *

import nose
import datetime
import isodate

DAY = datetime.timedelta(days=1)
MINUTE = datetime.timedelta(minutes=1)


class EmitIntegers(DataBlock):
    """
    Instantiate EmitIntegers with a maximum value, and it will act as a block that emits sequential integers
    from 0 to max, inclusive of both endpoints.
    """

    def __init__(self, max):
        super(EmitIntegers, self).__init__()
        # Intentionally making self.time a datetime.datetime object instead of datetime.date, in contrast with
        # other tests, in order to exercise different types.
        self.time = isodate.parse_datetime('1900-01-01T00:00:00')
        self.max = max
        self.value = 0

    def block_code(self):
        if self.value > self.max:
            self.terminate()
        else:
            self.set_output_data('value', self.value)
            self.value += 1
            self.time = self.time + MINUTE


class EmitTimeSeries(DataBlock):
    """
    Instantiate EmitTimeSeries with the entire timeseries as a list of (time, value) tuples.
    It will then act as a block that emits the next timeseries element on 'time' and 'value'
    channels for each iteration.
    """

    def __init__(self, timeseries):
        super(EmitTimeSeries, self).__init__()
        self.pointer = 0
        self.timeseries = timeseries

    def block_code(self):
        if self.pointer >= len(self.timeseries):
            self.terminate()
        else:
            timeseries_entry = self.timeseries[self.pointer]
            self.time = timeseries_entry[0]
            self.set_output_data('time', timeseries_entry[0])
            self.set_output_data('value', timeseries_entry[1])
            self.pointer += 1


def expected_fruit_quantity(fruit_name):
    """
    Returns an arbitrary (but consistent) number, given a string input.
    """
    return hash(fruit_name) % 100


def generate_fruit_inventory():
    fields = ['fruit', 'count']
    data = (('apple', expected_fruit_quantity('apple')),
            ('peach', expected_fruit_quantity('peach')),
            ('star fruit', expected_fruit_quantity('star fruit')))
    yield {'fields': fields}
    for item in data:
        yield {'data': item}

def confirm_fruit_delivery(fields, data):
    fruit_type = data[fields.index('fruit')]
    fruit_count = data[fields.index('count')]
    nose.tools.assert_equal(fruit_count, expected_fruit_quantity(fruit_type))


def passthrough_function(data):
    pass


class ConfirmSequence(DataBlock):
    """
    Helper class for other nose tests
    """
    def __init__(self, desired_sequence, comparison_function):
        super(ConfirmSequence, self).__init__()
        self.desired_sequence = desired_sequence
        self.comparison_function = comparison_function
        self.pointer = 0

    def block_code(self):
        value = self.get_input('value')
        self.comparison_function(value, self.desired_sequence[self.pointer])
        self.pointer += 1


class ConfirmMergedSequence(ConfirmSequence):
    """
    See nose test "test_two_merged_inputs" for a usage example.
    """

    def block_code(self):
        a = self.get_input('input_a')
        b = self.get_input('input_b')
        expectation = self.desired_sequence[self.pointer]
        self.comparison_function(self.time, expectation[0])
        self.comparison_function(a, expectation[1])
        self.comparison_function(b, expectation[2])
        self.pointer += 1


class TestConnectivity(object):

    def test_two_blocks(self):
        """
        +---------------------+        +-------------------------+
        | emit_data_block     |        | confirm_sequence_block  |
        |                     |        |                         |
        |   value [0,1,2,3,4] ===========> value [0,1,2,3,4]     |
        |                     |        |                         |
        +---------------------+        +-------------------------+
        """
        clear_graph()
        emit_data_block = EmitIntegers(4)
        emit_data_block.set_debug_name('EmitIntegers')
        confirm_sequence_block = ConfirmSequence([0, 1, 2, 3, 4], nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('ConfirmSequence')
        connect(emit_data_block, 'value', confirm_sequence_block, 'value')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['EmitIntegers', 'ConfirmSequence']))

    def test_two_blocks_via_timeseries(self):
        timeseries = [[isodate.parse_date('2000-01-01'), 1],
                      [isodate.parse_date('2000-01-02'), 3],
                      [isodate.parse_date('2000-01-03'), 5],
                      [isodate.parse_date('2000-01-04'), 9]]
        clear_graph()
        emit_data_block = EmitTimeSeries(timeseries)
        emit_data_block.set_debug_name('EmitTimeSeries')
        confirm_sequence_block = ConfirmSequence([1, 3, 5, 9], nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('ConfirmSequence')
        connect(emit_data_block, 'value', confirm_sequence_block, 'value')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['EmitTimeSeries', 'ConfirmSequence']))

    def test_two_merged_inputs(self):
        """
        +---------------------+
        | emit_data_block_1   |        +------------------------------------------------------+
        |                     |        |                                                      |
        |         value [1,3] ===========> input_a         confirm_sequence_block             |
        |                     |        |                                                      |
        +---------------------+        |             (a, b)                                   |
                                       |     time 1: (1, null) [insufficient data to execute] |
        +---------------------+        |     time 2: (1, 200)  [executes]                     |
        | emit_data_block_2   |        |     time 3: (3, 200)  [executes]                     |
        |                     |        |     time 4: (3, 400)  [executes]                     |
        |                     |        |                                                      |
        |    value [200,400] ===========> input_b                                             |
        +---------------------+        +------------------------------------------------------+
        """
        clear_graph()
        timeseries_1 = [[isodate.parse_date('2000-01-01'), 1],
                        [isodate.parse_date('2000-01-03'), 3]]

        timeseries_2 = [[isodate.parse_date('2000-01-02'), 200],
                        [isodate.parse_date('2000-01-04'), 400]]

        emit_data_block_1 = EmitTimeSeries(timeseries_1)
        emit_data_block_2 = EmitTimeSeries(timeseries_2)

        emit_data_block_1.set_debug_name('EmitTimeSeries_1')
        emit_data_block_2.set_debug_name('EmitTimeSeries_2')

        # We should see the sequence start on the first day for which we have data for BOTH inputs,
        # and we should see another call for every time that we have updated information for ANY input:
        expected_data = [[isodate.parse_date('2000-01-02'), 1, 200],
                         [isodate.parse_date('2000-01-03'), 3, 200],
                         [isodate.parse_date('2000-01-04'), 3, 400]]

        confirm_sequence_block = ConfirmMergedSequence(expected_data, nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('ConfirmMergedSequence')
        connect(emit_data_block_1, 'value', confirm_sequence_block, 'input_a')
        connect(emit_data_block_2, 'value', confirm_sequence_block, 'input_b')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['EmitTimeSeries_1', 'EmitTimeSeries_2', 'ConfirmMergedSequence']))

    def test_discarded_inputs(self):
        """
        Test discarded inputs.
        This test uses the same graph arrangement as test_two_merged_inputs(), but for this test case, one of the
        input data streams has several entries that will be discarded before the other input is ready.
        """
        clear_graph()
        timeseries_1 = [[isodate.parse_date('2000-01-01'), 1],
                        [isodate.parse_date('2000-01-02'), 2],
                        [isodate.parse_date('2000-01-03'), 3],
                        [isodate.parse_date('2000-01-04'), 4],
                        [isodate.parse_date('2000-01-05'), 5]]

        timeseries_2 = [[isodate.parse_date('2000-01-04'), 400],
                        [isodate.parse_date('2000-01-05'), 500]]

        emit_data_block_1 = EmitTimeSeries(timeseries_1)
        emit_data_block_2 = EmitTimeSeries(timeseries_2)

        emit_data_block_1.set_debug_name('emit_data_block_1')
        emit_data_block_2.set_debug_name('emit_data_block_2')

        # We should see the sequence start on the first day for which we have data for BOTH inputs,
        # and we should see another call for every time that we have updated information for ANY input:
        expected_data = [[isodate.parse_date('2000-01-04'), 4, 400],
                         [isodate.parse_date('2000-01-05'), 5, 500]]

        confirm_sequence_block = ConfirmMergedSequence(expected_data, nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('confirm_sequence_block')

        connect(emit_data_block_1, 'value', confirm_sequence_block, 'input_a')
        connect(emit_data_block_2, 'value', confirm_sequence_block, 'input_b')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set,
                                set(['emit_data_block_1', 'emit_data_block_2', 'confirm_sequence_block']))

    def test_offset_inputs(self):
        """
        Test offset inputs.
        This test uses the same graph arrangement as test_two_merged_inputs(), but for this test case, none of the
        inputs have perfect alignment.
        """
        clear_graph()
        timeseries_1 = [[isodate.parse_date('2000-01-01'), 1]]

        timeseries_2 = [[isodate.parse_date('2000-01-02'), 200],
                        [isodate.parse_date('2000-01-03'), 300]]

        emit_data_block_1 = EmitTimeSeries(timeseries_1)
        emit_data_block_2 = EmitTimeSeries(timeseries_2)

        emit_data_block_1.set_debug_name('emit_data_block_1')
        emit_data_block_2.set_debug_name('emit_data_block_2')

        # We should see the sequence start on the first day for which we have data for BOTH inputs,
        # and we should see another call for every time that we have updated information for ANY input:
        expected_data = [[isodate.parse_date('2000-01-02'), 1, 200],
                         [isodate.parse_date('2000-01-03'), 1, 300]]

        confirm_sequence_block = ConfirmMergedSequence(expected_data, nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('confirm_sequence_block')

        connect(emit_data_block_1, 'value', confirm_sequence_block, 'input_a')
        connect(emit_data_block_2, 'value', confirm_sequence_block, 'input_b')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set,
                                set(['emit_data_block_1', 'emit_data_block_2', 'confirm_sequence_block']))

    def test_time_range(self):
        """
        Tests the ability to specify start and end times, outside of which we do not execute any blocks downstream
        of the head(s).
        """
        clear_graph()
        timeseries = [[isodate.parse_date('2000-01-01'), 1],
                      [isodate.parse_date('2000-01-02'), 3],
                      [isodate.parse_date('2000-01-03'), 5],
                      [isodate.parse_date('2000-01-04'), 9]]
        emit_data_block = EmitTimeSeries(timeseries)
        emit_data_block.set_debug_name('EmitTimeSeries')
        confirm_sequence_block = ConfirmSequence([3, 5], nose.tools.assert_equal)
        confirm_sequence_block.set_debug_name('ConfirmSequence')
        connect(emit_data_block, 'value', confirm_sequence_block, 'value')
        trace = core.run(start=isodate.parse_date('2000-01-02'), end=isodate.parse_date('2000-01-03'))
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['EmitTimeSeries', 'ConfirmSequence']))

    def test_one_time_output_with_intermediaries(self):
        """
        In this test, one upstream blocks only provides data once, keeping it the same for the rest of time.
        The other block provides new data at each iteration.
        +----------------------+      +---------------------+       +-------------------------------------------------+
        |                      |      |  passthrough FIELDS |       |                                                 |
        |              fields ========>                     =========> fields [only has info on iteration 0]          |
        |                      |      |                     |       |                                                 |
        |                      |      +---------------------+       |                                                 |
        | data_generator_block |                                    |                                                 |
        |                      |      +---------------------+       |       verification_block                        |
        |                      |      | passthrough DATA    |       |                                                 |
        |                      |      |                     |       |                                                 |
        |                      |      |                     |       |                                                 |
        |                      |      |                     |       |                                                 |
        |                data ========>                    ==========> data [has info on iterations 1-4]              |
        +----------------------+      +---------------------+       +-------------------------------------------------+

        There are two tricky parts to this arrangement:
        1. The user cannot connect the data generator block directly to the verification block. The reason for this is
         that the data generator block only provides info for "fields" on iteration 0, and only provides info for "data"
         on iterations 1-n. Since the verification block has to wait for iteration 1 to have both of its inputs
         fulfilled, the "fields" channel would be full, and the data generator block will never be permitted to run.
         This could be solved by having a buffer in the channel larger than the 1 iteration it can currently hold.
        2. On the implementation side, the thing that originally tripped up the success of this test is that the
         passthrough for "fields" needs to update ist own timestamp every time the data generator block iterates, even
         though no new data is provided. Otherwise the logic for the execution of the verification block cannot be done
         correctly.

        In this example, the blocks must execute in topologically sorted order to ensure correctness. In most of the
        other tests, order of execution is unimportant.
        """
        clear_graph()
        data_generator_block = conduit.GeneratorBlock(generate_fruit_inventory)
        data_generator_block.set_debug_name('generate_fruit_inventory')
        verification_block = conduit.Block(confirm_fruit_delivery)
        verification_block.set_debug_name('confirm_fruit_delivery')
        passthrough_block_1 = conduit.PassThrough(passthrough_function)
        passthrough_block_1.set_debug_name('passthrough for DATA')
        passthrough_block_2 = conduit.PassThrough(passthrough_function)
        passthrough_block_2.set_debug_name('passthrough for FIELDS')
        data_generator_block('data') >> passthrough_block_1('data') >> verification_block('data')
        data_generator_block('fields') >> passthrough_block_2('data') >> verification_block('fields')
        trace = core.run()
        executed_set = executed_block_set(trace)
        nose.tools.assert_equal(executed_set, set(['passthrough for DATA', 'passthrough for FIELDS',
                                                   'generate_fruit_inventory', 'confirm_fruit_delivery']))
