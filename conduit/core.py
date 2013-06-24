#!/usr/bin/env python

"""
.. module:: core
   :synopsis: conduit core module

"""

from util import comparable_interface
import datetime
import random
import logging
import abc
import copy
import Queue
import isodate

random.seed(0)

START = isodate.parse_date('2010-03-01')
DAY = datetime.timedelta(days=1)
HOUR = datetime.timedelta(hours=1)

WEIGHT = 50000000

class Data(comparable_interface.Comparable):
    """
    The Data class acts as a struct containing user data and a timestamp. Data objects are passed from block to block
    by way of channels.
    """

    def __init__(self, time, data):
        self.time = time
        self.data = data

    def __nonzero__(self):
        """
        The validity of Data objects depends on them having a valid timestamp. This method defines the behavior
        when a Data object is referred to in a boolean context.
        """
        return bool(self.time)

    def __repr__(self):
        return "Data(time=" + str(self.time) + ", data=" + str(self.data) + ")"

    def _cmpkey(self):
        """
        Provided for the functionality in comparable_interface.
        Comparisons on Data objects will use the time field.
        So, for example, to get the Data object with the earliest time stamp from a list of Data objects,
        just call min(list_of_Data_objects).
        """
        return self.time


class Channel(comparable_interface.Comparable):
    """
    A channel must have exactly one input block (the producer) and may have zero or more output blocks (consumers).
    When the producer uses the is_open() method to ask the channel whether new data can be shoved into the channel,
    it is asking whether all consumers have pulled a copy of the data.
    """

    def __init__(self):
        self.value = None
        self.consumers = {}  # Key = Datablock object, Value = boolean indicating whether they're ready for more data
        self.producer = None
        self.debug_name_string = None
        self.active = False
        self.time = 0  # Channel time is always greater than or equal to the timestamp on the data in the channel

    def _cmpkey(self):
        """
        Provided for the functionality in comparable_interface.
        """
        return self.time

    def __repr__(self):
        """
        __repr__ is overloaded in order to simplify debugging.
        """
        return self.debug_name()

    def set_debug_name(self, name):
        """
        set_debug_name overrides the return value of str(self).
        To return this to what the user of a python debugger would normally expect, run:
        c.set_debug_name('<%s.%s object at %s>' % (c.__class__.__module__, c.__class__.__name__, hex(id(c))))
        assuming your channel object is referred to by variable c.
        """
        self.debug_name_string = name

    def debug_name(self):
        if self.debug_name_string:
            return self.debug_name_string
        else:
            return "Channel connecting " + str(self.producer) + " to " + str(self.consumers)

    def time(self):
        return self.time

    def touch(self, time):
        self.time = time

    def get_value(self, consumer=None):
        """
        If consumer is specified, the channel will record that consumer as having consumed the value.
        """
        if consumer:
            self.consumers[consumer] = True
        return self.value

    def set_value(self, value):
        self.value = value
        self.active = True
        logging.debug("# Setting value in <" + str(self) + ">: " + str(self.value))
        self.mark_consumer_plates_full()

    def activate(self):
        self.active = True

    def deactivate(self):
        self.active = False

    def has_data(self):
        return self.value is not None

    def is_open(self):
        for ready_for_more_data in self.consumers.values():
            if not ready_for_more_data:
                return False
        return True

    def mark_consumer_plates_full(self):
        self.set_consumer_status(False)

    def mark_consumers_hungry_for_more(self):
        self.set_consumer_status(True)

    def set_consumer_status(self, status, consumer=None):
        if consumer:
            self.consumers[consumer] = status
        else:
            for consumer in self.consumers.keys():
                self.consumers[consumer] = status

    def add_producer(self, producer):
        self.producer = producer

    def add_consumer(self, consumer):
        self.consumers[consumer] = True

    def get_consumers(self):
        """
        consumers are currently just DataBlock objects. There's still an open question as to whether we will need
        both the DataBlock and the channel name on which it is subscribing to this information.
        """
        return self.consumers.keys()


class Connectable():
    """
    Connectable objects are designed to enable syntax helpers for wiring blocks together.
    """
    def __init__(self, block, channel_name):
        self.block = block
        self.channel_name = channel_name

    def __rshift__(self, other):
        connect(self.block, self.channel_name, other.block, other.channel_name)
        return other


class DataBlock():
    """
    User implementations of data blocks should derive from the DataBlock class and implement the block_code()
    instance method. The block_code() method will be called after some preamble code whenever the block is driven
    by a call to its step() method.
    """

    __metaclass__ = abc.ABCMeta  # Defined so that we can mark block_code() as an abstract method

    def __init__(self):
        self.input_channels = {}   # Keys = channel name, Values = Channel objects
        self.input_data = {}       # Keys = channel name, Values = Data objects
        self.output_channels = {}  # Keys = channel name, Values = Channel objects
        self.priority = 100  # Lower numbers have higher priority
        # The execution of each iteration of a block has a specific definition of "now".
        self.time = 0
        self.termination_reached = False
        self.debug_name = '<%s.%s object at %s>' % (self.__class__.__module__, self.__class__.__name__, hex(id(self)))
        self.start_time = None
        self.end_time = None
        self.block_initialization()

    def __repr__(self):
        """
        __repr__ is overloaded in order to simplify debugging.
        self.debug_name defaults to what would ordinarily be seen as a response to self.__repr__ but user code
        can call set_debug_name() to override this, and we take advantage of it in the subclasses that enable
        syntax helpers for defining blocks.
        """
        return self.debug_name

    def __cmp__(self, other):
        """
        Comparison operator is provided so that blocks can have different priorities for execution order.
        TODO: Code that actually uses priorities has been removed, so we may consider getting rid of this.
        """
        return cmp(self.priority, other.priority)

    def __call__(self, *args, **kwargs):
        """
        Defining __call__ makes it possible to let users take advantage of this connection syntax:
        """
        return Connectable(self, args[0])

    def set_debug_name(self, name):
        self.debug_name = name

    def terminate(self):
        self.termination_reached = True
        for channel_name in self.output_channels.keys():
            self.output_channels[channel_name].deactivate()

    def terminated(self):
        return self.termination_reached

    def increment_time(self):
        if isinstance(self.time, int):
            # If time isn't an int, user has overridden it, and it will be up to them to increment time.
            self.time += 1

    def set_start(self, start):
        self.start_time = start

    def set_end(self, end):
        self.end_time = end

    def _before_valid_time_range(self):
        """
        In case of uncertainty (times not specified), we assume that we are in a valid range.
        """
        if self.start_time is not None:
            try:
                if self.time < self.start_time:
                    return True
            except TypeError:
                return False
        return False

    def _after_valid_time_range(self):
        """
        In case of uncertainty (times not specified), we assume that we are in a valid range.
        """
        if self.end_time is not None:
            try:
                if self.time > self.end_time:
                    return True
            except TypeError:
                return False
        return False

    def _in_valid_time_range(self):
        """
        If start_time or end_time is set and current time is outside the specified range, return False.
        If time window is not comparable with current time (as would be the case if user code specifies the time to
        be of a different type), we have not gotten far enough into the process to make a decision, and default to True.
        All other cases return True.
        """
        if self._before_valid_time_range() or self._after_valid_time_range():
            return False
        else:
            return True

    def set_input_data(self, key, value):
        """
        set_input_data will automatically create an input channel if necessary.
        Automatic channel creation is intended for the case where users are trying to set initial values on a block
        whose input channels aren't subscribed to anything in the graph.
        """
        if not key in self.input_channels.keys():
            self.set_input_channel(key, Channel())
        self.input_channels[key].set_value(Data(self.time, value))

    def _get_input_data_object(self, key):
        """
        _get_input_data_object() not typically used by user-defined subclasses, because it retrieves data objects from
        all input channels, as opposed to data objects that have already been pulled into block-local storage from those
        input channels.
        """
        return self.input_channels[key].get_value(self)

    def get_input(self, key):
        """
        get_input() only looks at data that has already been pulled from a channel.
        """
        return self.input_data[key].data

    def _get_all_input_data_objects(self):
        inputs = {}
        for input_name in self.input_channels.keys():
            inputs[input_name] = self.input_channels[input_name].get_value(self)
        return inputs

    def _get_all_input_values(self):
        input_values = {}
        for input_name in self.input_channels.keys():
            channel = self.input_channels[input_name]
            data_object = channel.get_value(self)
            input_values[input_name] = data_object.data
        return input_values

    def clear_inputs(self):
        for input_channel in self.input_channels.values():
            input_channel.set_readiness(False, self)

    def clear_outputs(self):
        for output_channel in self.output_channels.values():
            output_channel.set_readiness(False)

    def set_output_data(self, key, value):
        self.get_output_channel(key).set_value(Data(self.time, value))

    def read_output_data(self, key=None):
        if key:
            return self.output_channels[key].get_value()
        else:
            output_data = {}
            for channel_name in self.output_channels.keys():
                output_data[channel_name] = self.output_channels[channel_name].get_value()
            return output_data

    def get_output_channel(self, output_channel_name):
        """
        get_output_channel will create a new channel object if necessary.
        """
        if not output_channel_name in self.output_channels.keys():
            self.output_channels[output_channel_name] = Channel()
        self.output_channels[output_channel_name].add_producer(self)
        return self.output_channels[output_channel_name]

    def pass_data_through(self, data=None):
        if data:
            for channel_name in data.keys():
                self.set_output_data(channel_name, data[channel_name])
        else:
            for channel_name in self.input_channels.keys():
                self.set_output_data(channel_name, self.read_input_data(channel_name))

    def set_input_channel(self, input_channel_name, channel):
        self.input_channels[input_channel_name] = channel
        channel.add_consumer(self)

    def get_input_channel_names(self):
        return self.input_channels.keys()

    def get_output_channel_names(self):
        return self.output_channels.keys()

    def advance_self_to_latest_time_of_all_input_channels(self):
        channel = max(self.input_channels.values())
        self.time = channel.time
        """
        if not isinstance(data_object.time, int) and isinstance(self.time, int):
            # User has overridden time to be a new type. It would be cleaner to set a flag indicating
            # that a user has overridden it, and possibly store the value in a different variable, but
            # that approach has the cost that we would have to pass the flag through the channels with each
            # message.
            self.time = data_object.time
        else: data_object.time > self.time:
            self.time = data_object.time
        """

    def advance_self_to_latest_time_of_pulled_data(self):
        for data_object in self.input_data.values():
            if not isinstance(data_object.time, int) and isinstance(self.time, int):
                # User has overridden time to be a new type. It would be cleaner to set a flag indicating
                # that a user has overridden it, and possibly store the value in a different variable, but
                # that approach has the cost that we would have to pass the flag through the channels with each
                # message.
                self.time = data_object.time
                continue
            if data_object.time > self.time:
                self.time = data_object.time

    def advance_self_to_latest_time_of_data_in_channels(self):
        for data_object in self.input_channels.values():
            if not isinstance(data_object.time, int) and isinstance(self.time, int):
                self.time = data_object.time
                continue
            if data_object.time > self.time:
                self.time = data_object.time

    def step(self):
        """
        Returns a set of DataBlocks whose inputs were updated. This will be used by the Graph run() method to invoke
        the step() method of the next blocks in the chain (we don't invoke those call the step() method of those
        blocks directly from here, because then the stack could get quite large).
        """
        # This method will typically have been called by the scheduler as a result of either (A) this block
        #   is registered at the head of the graph, or (B) there is a state change in the inputs.
        #
        # 1. Check to see whether downstream (output) channels are accepting input (all of the channel's
        #    consumers have pulled the "next" value).
        #    If any output channel is not accepting input, abort.
        # 2. Find minimum time in the "next" fields of all of my own input channels. Pull that time
        #    (or times -- in a tie, everybody wins) into my own "current" fields and look for state changes.
        #    If no state changes, abort.
        # 3. Check whether all inputs are satisfied. If any are unsatisfied, abort.
        # 4. Execute user code (which fills downstream channels "next" fields)
        # 5. For each downstream channel, append all consumers to return value.
        # 6. Return set of downstream blocks that are candidates for execution.



        # REPLACEMENT FOR STEP 5:
        # Logic for execution flow WAS to:
        # Execute a block. Look at all output channels to which the block provided data on that iteration.
        # For each of those channels, nominate each of their consumers as candidates for running next.
        # Trigger the set of unique candidates.
        # The problem with this logic is that channels now buffer data, possibly for multiple iterations.
        # The downstream blocks aren't always eligible to execute right away, and we basically forget to execute
        # them on future iterations (because if they aren't provided new input, they won't be candidates, but
        # they may legitimately need to pull data from their input channel buffers and execute).
        #
        # So instead of all that fancy logic, we basically say screw it. Traverse the whole graph and offer
        # everybody the chance to run -- including blocks that are downstream of some that do not run on this iteration!

        downstream_blocks = []


        # Restrict the set of input channels we consider to those that are active (a channel is typically deactivated
        # before it starts producing useful data, and after it has reached the end of useful data)
        active_input_channels = {}
        active_input_channels_names = [channel_name for channel_name in self.input_channels.keys()
                                       if self.input_channels[channel_name].active]
        active_input_channels = {name:self.input_channels[name] for name in active_input_channels_names}

        # If this is a block that has one or more input channels but none of them are active, bail out:
        if self.input_channels and not active_input_channels:
            return downstream_blocks

        # 2. Pull data for earliest time. If there are no input channels, we just proceed:
        unprocessed_input_channels = {}
        if active_input_channels:

            for output_channel in self.output_channels.values():
                # Update timestamp unconditionally (whether the user code provided a new value or not).
                # This demonstrates that the block has provided everything it knows for the current time.
                output_channel.touch(self.time)

            # Get the collection of channels from which I have not already consumed data:
            unprocessed_input_channel_names = [channel_name for channel_name in active_input_channels.keys()
                                          if not active_input_channels[channel_name].consumers[self]]
            unprocessed_input_channels = {name:active_input_channels[name] for name in unprocessed_input_channel_names}
            channel_with_earliest_data = min(active_input_channels.values())
            if unprocessed_input_channels:
                unprocessed_channel_with_earliest_data = min(unprocessed_input_channels.values())
            else:
                unprocessed_channel_with_earliest_data = None

            if not unprocessed_input_channels:
                # If I have some input channels but none of them are unprocessed, then there is no data to be pulled.
                # In this case, we need to pass along the current time, and be done.
                self.advance_self_to_latest_time_of_data_in_channels()
                for output_channel in self.output_channels.values():
                    output_channel.touch(self.time)
                return downstream_blocks




        if unprocessed_input_channels:
            state_change = False
            for input_channel_name in unprocessed_input_channels.keys():
                # We compare against the timestamps for all channels because even if a given channel has provided us
                # valid data, if its timestamp is still earlier than what we believe to be the current timestamp, then
                # we need to provide it the opportunity to potentially give even more recent data.
                if unprocessed_input_channels[input_channel_name] <= channel_with_earliest_data:
                    new_data = unprocessed_input_channels[input_channel_name].get_value(self)  # gets value AND marks as consumed.
                    logging.debug("==> Pulling data (" + str(new_data.data) + ") from channel '" +
                                  input_channel_name + "' -- " + str(self))
                    if (not self.input_data) or \
                            (not input_channel_name in self.input_data) or \
                                    self.input_data[input_channel_name] != new_data:
                        state_change = True
                        self.input_data[input_channel_name] = new_data
            if not state_change:
                return downstream_blocks
            self.advance_self_to_latest_time_of_pulled_data()


        for input_name in self.input_data.keys():
            logging.debug("# BLOCK " + str(self) + ": time=" + str(self.time) + ", " + str(input_name) + " = " +
                          str(self.input_data[input_name]))

        # 3. Ensure inputs satisfied (note that we want input data for all channels, not just those currently active):
        if self.input_channels:
            for input_channel_name in self.input_channels.keys():
                if input_channel_name not in self.input_data.keys():
                    logging.debug("     Channel " + input_channel_name + " not satisfied. Bailing out.")
                    return downstream_blocks

        # Ensure that -- if there are any downstream channels -- they are all open.
        # Note that this has to happen after pulling data from the input channels in order to properly accommodate the
        # case in which a block consumes its own outputs.
        if self.output_channels:
            for output_channel in self.output_channels.values():
                if not output_channel.is_open():
                    return downstream_blocks

        # 4. Execute user code:
        logging.debug("Executing block code for: " + str(self))
        self.block_code()  # the block_code() method is responsible for setting new values in the output channels
        # logging.debug("After executing user code, block time is: " + str(self.time))

        for output_channel in self.output_channels.values():
            # Update timestamp unconditionally We do it again here (in addition to above), in case the user code updated
            # self.time. It needed to be done above in case we were to bail out before running user code.
            output_channel.touch(self.time)


        for output_channel in self.output_channels.values():
            if self._in_valid_time_range():
                for consumer in output_channel.get_consumers():
                    # logging.debug(str(self) + " is nominating block to append to run list: " + str(consumer))
                    downstream_blocks.append(consumer)
            else:
                output_channel.mark_consumers_hungry_for_more()
                if self._after_valid_time_range():
                    self.terminate()

        # 6. Return collection of downstream neighbors:
        return downstream_blocks

    def block_initialization(self):
        return

    def set_input_connection(self, channel_name, channel):
        self.input_channels[channel_name] = channel

    def set_output_connection(self, source_channel, destination_block, destination_channel):
        self.output_channels[source_channel] = [destination_block, destination_channel]
        # self.set_output_data(source_channel, None)

    @abc.abstractmethod
    def block_code(self):
        return


class BaseFilter(DataBlock):

    def __repr__(self):
        """
        __repr__ is overloaded in order to simplify debugging.
        """
        return "Filter that wraps predicate " + str(self.predicate)

    @abc.abstractmethod
    def predicate(self):
        return False

    def block_code(self):
        if self.predicate():
            self.pass_data_through()
        else:
            self.clear_outputs()
        self.clear_inputs()


class Block(DataBlock):
    """
    User function should return a map of output arg names and values.
    """

    def __init__(self, user_function):
        DataBlock.__init__(self)
        self.user_function = user_function
        self.clear_inputs()
        self.debug_name = "Block that wraps " + str(self.user_function)

        for argname in user_function.func_code.co_varnames:
            # We only need to handle the implicitly defined inputs. All others are defined by the graph connections:
            if argname == 'previous_outputs':
                # Blocks implicitly subscribe to their own outputs, but this information is clobbered if they also
                # subscribe to some other block's outputs on the same input channel. Note that this ordering guarantee
                # needs to be inspected more closely if we start doing things in parallel.
                self.set_input_channel(argname, self.get_output_channel(argname))
                self.set_input_data(argname, {})

    def block_code(self):
        inputs = self._get_all_input_values()
        outputs = self.user_function(**inputs)
        if outputs:
            for key in outputs.keys():
                self.set_output_data(key, outputs[key])
        if 'previous_outputs' in self.output_channels.keys():
            self.output_channels['previous_outputs'].set_value(Data(self.time, copy.deepcopy(outputs)))


class GeneratorBlock(Block):
    def __init__(self, user_function):
        Block.__init__(self, user_function)
        self.first_time = True

    def block_code(self):
        inputs = self._get_all_input_values()
        outputs = {}
        """
        self.f = self.user_function(**inputs)
        try:
            outputs = self.f.send(inputs)
        except StopIteration:
            self.terminate()
        """
        if self.first_time:
            self.f = self.user_function(**inputs)
            outputs = self.f.next()
            self.first_time = False
        else:
            try:
                outputs = self.f.send(inputs)
            except StopIteration:
                self.terminate()

        if outputs:
            for key in outputs.keys():
                self.set_output_data(key, outputs[key])
        if 'previous_outputs' in self.output_channels.keys():
            self.output_channels['previous_outputs'].set_value(Data(self.time, copy.deepcopy(outputs)))

class Filter(Block):
    """
    User function should return a Boolean.
    If True, all input args will be passed unmodified as output args.
    If False, all output args will have a value of None.
    """
    def block_code(self):
        inputs = self._get_all_input_values()
        filter_result = self.user_function(**inputs)
        if filter_result:
            self.pass_data_through(inputs)
        else:
            self.clear_outputs()

class PassThrough(Block):
    """
    User function need not return anything. We just pass all inputs to outputs.
    """
    def block_code(self):
        inputs = self._get_all_input_values()
        filter_result = self.user_function(**inputs)
        self.pass_data_through(inputs)


class Terminator(Block):
    """
    Terminator is a convenient mechanism for setting global termination conditions.
    Just provide a predicate that returns a boolean.
    """

    def block_code(self):
        inputs = self._get_all_input_values()
        filter_result = self.user_function(**inputs)
        if filter_result:
            self.pass_data_through(inputs)
        else:
            self.clear_outputs()


class MasterControlBlock(DataBlock):
    """
    The MasterControlBlock provides a way to kill the simulation.
    Simply set a non-True value on its 'continue' input channel.
    The block is somewhat unique in that it acts immediately when an input value is set. This guarantees that it
    executes before other blocks that are peers in the graph (the usual approach is that all inputs for all peers
    get set in arbitrary order and then -- again in arbitrary order -- the step() method is called for all peers).
    """
    def block_initialization(self):
        self.priority = 0  # Lower numbers have higher priority

    def block_code(self):
        if not self.read_input_data('continue'):
            graph = self.read_input_data('graph')
            graph.terminate()
        return


class Graph():
    def __init__(self, head=None):
        self.heads = []
        if head:
            self.add_head(head)
        self.master_control_block = MasterControlBlock()
        self.master_control_block.set_input_data('graph', self)

    def add_head(self, block):
        self.heads.append(block)

    def set_termination_condition(self, source_block, source_channel):
        connect(source_block, source_channel, self.master_control_block, 'continue')

    def run(self, start=None, end=None):
        # TODO: It would be a good optimization to make sure that is X is upstream of Y, X has the opportunity to
        # run first in any given iteration. This is an optimization, not a requirement (but it turns out to be really
        # easy to create bugs in the logic in the block step() method that are resolved if this is a requirement).
        run_set = set()
        for head in self.heads:
            run_set.add(head)
            head.set_start(start)
            head.set_end(end)
        while run_set:  # Run through multiple iterations of entire graph
            while run_set:  # Run through a single iteration of entire graph
                block = run_set.pop()
                for downstream_block in block.step():
                    run_set.add(downstream_block)
            for head in self.heads:
                if not head.terminated():
                    head.increment_time()
                    run_set.add(head)


def connect(source_block, source_channel_name, destination_block, destination_channel_name):
    channel = source_block.get_output_channel(source_channel_name)
    destination_block.set_input_channel(destination_channel_name, channel)
