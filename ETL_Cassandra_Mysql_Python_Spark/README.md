### INTRODUÇÃO

Esse projeto tem o propósito de aprimorar os meus conhecimentos na área de engenharia de dados. Para alcançar tal objetivo utilizei as modernas ferramentas de processamento de dados ETL e Spark.

#### Base de dados

Para realizar a análise utilizei uma base de dados pública da comunidade Kaggle, que está em formato CSV. O contéudo é um catálogo de filmes da empresa Netflix, que pode ser encontrado nesse linK:

<https://www.kaggle.com/code/lucasnatali/netflix-movies-and-tv-shows>

#### Normalizando os dados

O dataset veio com vários problemas:

* Na coluna date_added o mês veio escrito por extenso ao invés de numérico. Exemplo: December 23, 2016. A solução foi utilizar o replace para substituir o mês por um valor númerico.

* Alguns meses vieram com dias que não existem. Exemplo: o mês de Abril tinha 31 dias. A solução foi utilizar o replace para substituir o dia do mês inexistente por um existente.

* Alguns dados vieram com aspas simples e duplas no meio do texto o que não permitia popular o Mysql com os todos os dados. Na hora da inserção o Mysql entendia que havia colunas a mais e isso gerava erro. A solução foi remover algumas aspas simples e duplas utilizando o replace.
