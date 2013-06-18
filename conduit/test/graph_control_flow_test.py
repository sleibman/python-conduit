
from conduit import *

import nose
import datetime
import isodate

DAY = datetime.timedelta(days=1)


class EmitIntegers(DataBlock):
    """
    Instantiate EmitIntegers with a maximum value, and it will act as a block that emits sequential integers
    from 0 to max, inclusive of both endpoints.
    """

    def __init__(self, max):
        super(EmitIntegers, self).__init__()
        self.time = isodate.parse_date('1900-01-01')
        self.max = max
        self.value = 0

    def block_code(self):
        if self.value > self.max:
            self.terminate()
        else:
            self.set_output_data('value', self.value)
            self.value += 1
            self.time = self.time + DAY


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

    def test_sanity(self):
        nose.tools.assert_equal(2.0, 2.0)

    def test_two_blocks(self):
        """
        +---------------------+        +-------------------------+
        | emit_data_block     |        | confirm_sequence_block  |
        |                     |        |                         |
        |   value [0,1,2,3,4] ===========> value [0,1,2,3,4]     |
        |                     |        |                         |
        +---------------------+        +-------------------------+
        """
        emit_data_block = EmitIntegers(4)
        confirm_sequence_block = ConfirmSequence([0, 1, 2, 3, 4], nose.tools.assert_equal)
        connect(emit_data_block, 'value', confirm_sequence_block, 'value')
        graph = Graph(emit_data_block)
        graph.run()

    def test_two_blocks_via_timeseries(self):
        timeseries = [[isodate.parse_date('2000-01-01'), 1],
                      [isodate.parse_date('2000-01-02'), 3],
                      [isodate.parse_date('2000-01-03'), 5],
                      [isodate.parse_date('2000-01-04'), 9]]
        emit_data_block = EmitTimeSeries(timeseries)
        confirm_sequence_block = ConfirmSequence([1, 3, 5, 9], nose.tools.assert_equal)
        connect(emit_data_block, 'value', confirm_sequence_block, 'value')
        graph = Graph(emit_data_block)
        graph.run()

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
        timeseries_1 = [[isodate.parse_date('2000-01-01'), 1],
                        [isodate.parse_date('2000-01-03'), 3]]

        timeseries_2 = [[isodate.parse_date('2000-01-02'), 200],
                        [isodate.parse_date('2000-01-04'), 400]]

        emit_data_block_1 = EmitTimeSeries(timeseries_1)
        emit_data_block_2 = EmitTimeSeries(timeseries_2)

        # We should see the sequence start on the first day for which we have data for BOTH inputs,
        # and we should see another call for every time that we have updated information for ANY input:
        expected_data = [[isodate.parse_date('2000-01-02'), 1, 200],
                         [isodate.parse_date('2000-01-03'), 3, 200],
                         [isodate.parse_date('2000-01-04'), 3, 400]]

        confirm_sequence_block = ConfirmMergedSequence(expected_data, nose.tools.assert_equal)
        connect(emit_data_block_1, 'value', confirm_sequence_block, 'input_a')
        connect(emit_data_block_2, 'value', confirm_sequence_block, 'input_b')
        graph = Graph()
        graph.add_head(emit_data_block_1)
        graph.add_head(emit_data_block_2)
        graph.run()

    def test_discarded_inputs(self):
        """
        This test uses the same graph arrangement as test_two_merged_inputs(), but for this test case, one of the
        input data streams has several entries that will be discarded before the other input is ready.
        """
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
        graph = Graph()
        graph.add_head(emit_data_block_1)
        graph.add_head(emit_data_block_2)
        graph.run()


