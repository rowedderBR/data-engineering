#### INTRODUÇÃO
###
Esse projeto tem o propósito de aprimorar os meus conhecimentos na área de Engenharia de Dados. Para alcançar tal objetivo utilizei as modernas ferramentas de processamento de dados ETL e Spark.
###
#### Base de dados
###
Para realizar a análise utilizei uma base de dados pública da comunidade Kaggle. O arquivo  que está no formato CSV e o contéudo é um catálogo de filmes da empresa Netflix. A base de dados pode ser encontrado através do link:
###
<https://www.kaggle.com/code/lucasnatali/netflix-movies-and-tv-shows>
###
#### Normalizando os dados
###
O dataset veio com vários problemas:

* Na coluna date_added o mês veio escrito por extenso ao invés de numérico. Exemplo: December 23, 2016. A solução foi utilizar o replace para substituir o mês por um valor númerico.
###
* Alguns meses vieram com dias que não existem. Exemplo: o mês de Abril tinha 31 dias. A solução foi utilizar o replace para substituir o dia do mês inexistente por um existente.
###
* A data também foi organizada e normalizada para o formato date yyyy/mm/dd.
###
* Alguns dados vieram com aspas simples e duplas no meio do texto o que não permitia popular o Mysql com os todos os dados. Na hora da inserção o Mysql entendia que havia colunas a mais e isso gerava erro. A solução foi remover algumas aspas simples e duplas utilizando o replace.
###
**Exportando dados para o banco relacional Mysql**
###
Após a correção e normalização, os dados foram transformados em um dataframe spark e enviados para um banco de dados Mysql através do conector mysql-connector-python. 
###
**Importando os dados do Mysql e exportando para o banco não relacional Cassandra**
###
Os dados foram importados do banco de dados Mysql utilizando o mysql-connector-python e enviados para o banco não relacional Cassandra através do conector cassandra-driver.
