import sys
import os
import time

from flink.plan.Environment import get_environment
from flink.functions.GroupReduceFunction import GroupReduceFunction
from pymongo import MongoClient

if __name__ == "__main__":
    #Connect to mongo
    #client = MongoClient('localhost', 27017)
    #Get database
    #db = client['DB']
    #Get collection
    #cl = db['collection_name']
    
    #base_path = sys.argv[0]

    input_file = 'file:///home/souza/Downloads/ERAD2017/hillary_data-tiny.txt'
    output_file = 'file:///home/souza/Downloads/ERAD2017/out.txt'

    print(input_file)

    env = get_environment()
    data = env.read_text(input_file)
    
    #Implementar a função de flat_map para extrair os possiveis bigramas da string
    #Agrupar os dados pelo bigrama (count, bigram) utilizando group_by
    #Realizar a soma desses bigramas com o Reduce Group (Mesmo processo do word count)
    #Implementar a função de maps para inserir os dados ao BD
    data \
		.flat_map() \
        .group_by() \
        .reduce_group() \
        .map() \
        .output()
        
    # execute the plan locally.
    env.execute(local=True)
