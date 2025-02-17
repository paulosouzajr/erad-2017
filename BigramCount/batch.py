import sys
import os
import time

from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import FLOAT, WriteMode
from pymongo import MongoClient

class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

if __name__ == "__main__": 
    #not using dynamic path. Some part of how pyflink is executing the python code moves it, so the abspath term evaluates to some temp directory.
    #base_path = sys.argv[0]
 
    input_file = 'file:///home/souza/Downloads/ERAD_alunos/hillary_data-tiny.txt'
    output_file = 'file:///home/souza/Downloads/ERAD_alunos/out.txt'

    env = get_environment()
    data = env.read_text(input_file)

    #Flat_map produz as possiveis combinações de bigramas, iterando entre os espaços e combinando palavras.
    #Agrupa-se pelo indice 1 da tupla (count, bigram) para preparar o dado para o reducegroup (que realiza o reduce dos grupos)
    #Por motivo de deficiencia da API em Python do Flink é necessário outra função de map para inserir os dados no banco de dados (como datasink)
    data \
		.flat_map(lambda x, c: [(1, bigram) for bigram in [x.lower().split(' ')[i] + ' ' + x.split(' ')[1+i] for i in range(0, len(x.split(' '))-1)]]) \
        .group_by(1) \
        .reduce_group(Adder(), combinable=True) \
        .map(lambda y: cl.insert({"_id":y[1], "value":y[0]}) if cl.find({"_id":y[1]}).count() == 0 else cl.update({"_id":y[1]}, {"value": cl.find_one({"_id":y[1]})['value'] + 
            y[0]})) \
        .output()
        
    # execute the plan locally.
    env.execute(local=True)
