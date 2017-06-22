Python Flink™ ERAD Naive Bayes
==============================

Pacotes e procedimentos.
========================

ssh (ssh,openssh-server, openssh-client)

apt-get install -y ssh openssh-server openssh-client

Java (default-jre; openjdk-8- jdk), setar o JAVA_HOME

apt-get install -y default-jre openjdk-8- jdk

Como root, execute o comando abaixo

echo JAVA_HOME=/usr/bin/java &gt;&gt; /etc/environment

MongoDB (mongodb-org)

sudo rm /etc/apt/sources.list.d/mongodb*.listc

sudo apt-key adv -- keyserver hkp://keyserver.ubuntu.com:80 -- recv EA312927

echo &quot;deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse&quot; | sudo tee /etc/apt/sources.list.d/mongodb-org-

3.2.list

sudo apt-get update

sudo apt install mongodb-org

Iniciar o MongoDB, vai exigir senha do usuário

sudo service mongod start

Python2 e Python3 (python-dev, python3-dev, python-pip)

apt-get install -y python-dev python3-dev

pip e pip3 install (pymongo tweepy)

pip install pymongo tweepy

pip3 install pymongo, tweepy

OBS. Os procedimentos abaixo não devem ser executados como root

Flink 1.1.3 (wget http://archive.apache.org/dist/flink/flink-1.1.3/flink- 1.1.3-bin- hadoop1-

scala_2.10.tgz)

tar -xzf flink-1.1.3- bin-hadoop1- scala_2.10.tgz

Execução
========

Iniciar o Flink (Job Manager e o Task Manager), vai exigir senha do usuário

./flink-1.1.3/bin/start- cluster.sh

Verificar se os processos do flink estão em execução.

jps

A saída do comando deve ser algo semelhante a isso.

28871 TaskManager

28921 Jps

28555 JobManager

./flink-1.1.3/bin/pyflink3.sh example.py

Para executar os exemplos é necessário definir o path absoluto dos arquivos de input e output.

Para utilizar o banco de dados, deve-se executar:

mongo

dentro do banco de dados, selecione o banco com o comando.

use DB_POD

Para mostra alguns tweets - usar &quot;it&quot; para mostrar mais resultados.

db.tweets.find()

db.tweets.count()

Maiores referências em relação ao mongodb: 
https://docs.mongodb.com/manual/

Exemplos
========

As aplicações estão separadas por pasta. 
Bigram count -> realiza a contagem por bigramas e armazena no Mongodb
Word count -> Contagem de palavras, escreve em um arquivo de output

Datasets
========

Estão disponiveis alguns datasets, separados por quebra de linha, dos tweets realizados na epoca da eleição.

A coleta em tempo real de tweets pode ser feita utilizando a biblioteca Tweepy. A mesma irá utilizar a API do Twitter, por isso é necessário criar uma aplicação   
em https://apps.twitter.com/. Após criar a aplicação na API é necessário informar ao Tweepy os seguintes campos disponibilizados pela API:

consumer_key = '' 
consumer_secret = '' 
access_token = ''
access_secret = ''

Contato
=======

kassianoj@gmail.com

paulosouzjunior@gmail.com
