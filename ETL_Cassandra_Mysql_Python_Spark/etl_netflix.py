
import datetime
import csv
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from cassandra.cluster import Cluster


# CRIANDO CONEXÃO COM BANCO MYSQL

def executar(query): #INSERT. UPDATE. DELETE
   try:
      con = mysql.connector.connect(user='root', password='123', host='localhost', database='filmes')
      cursor = con.cursor()
      cursor.execute(query)
      cursor.close()
      con.commit()
      con.close()
   except Exception as e:
    print(e)

def buscar(query): #SELECT
    try:
      con = mysql.connector.connect(user='root', password='123', host='127.0.0.1', database='filmes')
      cursor = con.cursor()
      cursor.execute(query)
      return cursor.fetchall()
    except Exception as e:
      print(e)
    finally:
        cursor.close()
        con.close()


# CRIANDO CONEXÃO COM O BANCO CASSANDRA

cluster = Cluster()
session = cluster.connect("filmes") #--> keyspace


# IMPORTANDO DATASET COM PYTHON

lista = []
lista2 = []

with open('/home/rowedder/netflix.csv', 'r') as ficheiro:
    reader = csv.reader(ficheiro)
    next(reader)

# NORMALIZANDO O FORMATO DA DATA.
    
    for i in reader:
    
       i[0] = i[0].replace("'", " ").replace('"', '')
       i[1] = i[1].replace("'", " ").replace('"', '')
       i[2] = i[2].replace("'", " ").replace('"', '')    
       i[3] = i[3].replace("'", " ").replace('"', '')
       i[4] = i[4].replace("'", " ").replace('"', '')
       i[5] = i[5].replace("'", " ").replace('"', '')
       i[6] = i[6].replace("'", " ").replace('"', '')
       i[7] = i[7].replace("'", " ").replace('"', '')
       i[8] = i[8].replace("'", " ").replace('"', '')
       i[9] = i[9].replace("'", " ").replace('"', '')
       i[10] = i[10].replace("'", " ").replace('"', '')
       i[11] = i[11].replace("'", " ").replace('"', '')

       lista2.append(i)

       data = i[6].replace('January', '01').replace('February', '02').replace('March', '03')\
       .replace('April', '04').replace('May', '05').replace('June', '06').replace('July', '07')\
       .replace('August', '08').replace('September', '09').replace('October', '10')\
       .replace('November', '11').replace('December', '12').replace(',', '').replace(' ', '')

       if len(data) == 8:
           for j in data: 

               mes = data[0:2] + '-'
               dia = data[2:4] 
               ano = data[4:8] + '-'

               if mes == '04-' and dia == '31': # Normalizando datas que não existem 04/31 por 04/30
                   dia = '30'
               else:
                   f = 0

           a = ano + mes + dia
           lista.append(a)

       elif len(data) < 8:
           for j in data: 
               mes = data[0:2] + '-0'
               dia = data[2] 
               ano = data[3:7] + '-'

               if mes == '04-' and dia == '31': # Normalizando datas que não existem 04/31 por 04/30
                    dia = '30'
               else:
                    f = 0
    
           b = ano + mes + dia
           lista.append(b)

a1 = []
a2 = []
a3 = []
a4 = []
a5 = []
a6 = []
a7 = []
a8 = []
a9 = []
a10 = []
a11 = []

#id = list(range(7787))
for h in lista:
    col_data = h
    a6.append(col_data)

for m in lista2:
 
    col1 =  m[1]
    col2 =  m[2]
    col3 =  m[3]
    col4 =  m[4]
    col5 =  m[5]
    col6 =  m[6]  # Data (ignorar essa coluna, desformatada)
    col7 =  m[7]
    col8 =  m[8]
    col9 =  m[9]
    col10 = m[10]
    col11 = m[11]
    a1.append(col1)
    a2.append(col2)
    a3.append(col3)
    a4.append(col4)
    a5.append(col5)
    a7.append(col7)
    a8.append(col8)
    a9.append(col9)
    a10.append(col10)
    a11.append(col11)

# CRIANDO UM OBJETO SPARK

spark = SparkSession.builder.master("local").enableHiveSupport()\
    .appName("Python Spark SQL")\
    .getOrCreate()

# CRIANDO UM DATAFRAME Á PARTIR DAS COLUNAS NORMALIZADAS (date_added == lista)

colunas = ['tipo', 'title', 'director', 'caste', 'country', 'date_added', 
'release_year', 'rating', 'duration', 'listed_in','descricao']

                                                                                       
dataframe_netflix = spark.createDataFrame(zip(a1, a2, a3, a4, a5, a6, a7,\
     a8, a9, a10, a11), colunas)

#inicio = datetime.datetime.now()

# ITERANDO SOBRE O DATAFRAME SPARK

for i in dataframe_netflix.collect():

    a = i[0]
    b = i[1]
    c = i[2]
    d = i[3]
    e = i[4]
    f = i[5]
    g = i[6]
    h = i[7]
    j = i[8]
    l = i[9]
    m = i[10]

# EXPORTANDO DADOS PARA O MYSQL
   
    query = f"INSERT INTO netflix (tipo, title, director, caste, country, date_added,\
    release_year, rating, duration,listed_in, descricao) VALUES('{a}', '{b}', '{c}', '{d}', '{e}'\
    ,'{f}', '{g}', '{h}', '{j}', '{l}', '{m}')"
    print(query) #Apenas para confirmar os valores enviados ao banco
    executar(query)
#fim = datetime.datetime.now()
#print('Iní:', inicio)
#print('Fim:', fim)

#============================================================================================#

# IMPORTANTO DADOS DO BANCO MYSQL

query = "SELECT * FROM netflix;"
dados_mysql = buscar(query)

inicio = datetime.datetime.now()

for i in dados_mysql:

    a = i[0]
    b = i[1]
    c = i[2]
    d = i[3]
    e = i[4]
    f = i[5]
    g = str(i[6])
    h = i[7]
    j = i[8]
    l = i[9]
    m = i[10]
    n = i[11]


# INSERIR DADOS NO CASSANDRA (TODOS DE UMA VEZ)

    query = f" BEGIN BATCH INSERT INTO netflix(id, tipo, title, director, caste, country, date_added, release_year, rating, duration, listed_in, descricao) VALUES(uuid(), '{b}', '{c}', '{d}', '{e}', '{f}','{g}', '{h}', '{j}', '{l}', '{m}', '{n}') APPLY BATCH;"
    session.execute(query)
    print(query)
fim = datetime.datetime.now()
print('Iní:', inicio)
print('Fim:', fim)

"""
# CONSULTANDO OS DADOS

linhas = session.execute('SELECT date_added FROM "filmes"."netflix";')

for i in linhas:
    print(linhas)
    
"""
