import sys

import time

from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import FLOAT, WriteMode

class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))


if __name__ == "__main__":
    env = get_environment()


    input_file = 'file:///home/souza/Downloads/ERAD_alunos/hillary_data-tiny.txt'
    output_file = 'file:///home/souza/Downloads/ERAD_alunos/out.txt'

    data = env.read_text(input_file)
    # we first map each word into a (1, word) tuple, then flat map across that, and group by the key, and sum
    # aggregate on it to get (count, word) tuples
    data \
		.flat_map(lambda x, c: [(1, word) for word in x.lower().split()]) \
		.group_by(1) \
		.reduce_group(Adder(), combinable=True) \
		.map(lambda y: 'Count: %s Word: %s' % (y[0], y[1])) \
		.write_text(output_file, write_mode=WriteMode.OVERWRITE)


    # execute the plan locally.
    env.execute(local=True)

    