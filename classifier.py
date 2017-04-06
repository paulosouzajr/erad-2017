import sys

import time

from numpy import multiply, product
from flink.plan.Environment import get_environment
from flink.plan.Constants import FLOAT, WriteMode
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.ReduceFunction import ReduceFunction

from pymongo import MongoClient

output_file = 'file:///home/souza/Downloads/ERAD2017/out.txt'

def classify(c):
    cl1_size = cl1.find_one({"_id":c[1]}); #Quantidade de vezes que o bigrama se repetiu na sua respectiva classe
    cl2_size = cl2.find_one({"_id":c[1]}); #Quantidade de vezes que o bigrama se repetiu na sua respectiva classe
    cl1_amount = cl1.find().count() #O total de bigramas em sua respectiva classe
    cl2_amount = cl2.find().count() #O total de bigramas em sua respectiva classe
    if(cl1_size != None): #Caso nenhum, é definido 0
        cl1_size = cl1_size['value']
    else:
        cl1_size = 0
    if(cl2_size != None):
        cl2_size = cl2_size['value']
    else:
        cl2_size = 0

    return ((cl1_size/cl1_amount), (cl2_size/cl2_amount)) #São retornadas as probabilidades de frequencia desse bigrama

class Product(ReduceFunction): #Produtorio da tupla, sendo x o iterador e y o collector
    def reduce(self, x, y):
        return(x[0] * y[0], x[1] * y[1]) 
        

def classifier():
    env = get_environment()

    data =  env.from_elements("Hillary is going to be a better president than Trump #usElection")

    #Flat_map extrai os bigramas da string (tweet)
    #Em seguida uma função de map adiciona as informações relativas a probabilidade dos bigramas acontecerem em cada classe (retirados do banco de dados)
    #É necessário filtras os dados obtidos pela segunda função de map, no caso são retirados bigramas com probabilidade 0
    #É aplicada uma função de reduce que faz o produtorio das probabilidades
    #Em seguida esses dados são normalizados e adaptados para serem escritos no arquivo de saída
    data \
        .flat_map(lambda tweet, c: [(1, word) for word in [tweet.split(' ')[i] + ' ' + tweet.split(' ')[1+i] for i in range(0, len(tweet.split(' '))-1)]]) \
        .map(lambda x: classify(x)) \
        .filter(lambda x: x[0] != 0 and x[1] != 0) \
        .reduce(Product()) \
        .map(lambda y: 'Class 1 probability: %s Class 2 probability: %s' % (y[0] * (cl1.find().count() / (cl1.find().count() + cl2.find().count())), 
            y[1] * (cl2.find().count() / (cl1.find().count() + cl2.find().count())))) \
        .write_text(output_file)

    # execute the plan locally.
    env.execute(local=True)


if __name__ == "__main__":
    #Connect to mongo
    client = MongoClient('localhost', 27017)
    #Get database
    db = client['DB']
    #Get collections
    cl1 = db['trump']
    cl2 = db['hillary']

    classifier()