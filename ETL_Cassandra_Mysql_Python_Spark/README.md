### INTRODUÇÃO

Nesse projeto foi aplicado o processo conhecido em Engenharia de dados como ETL: Extract, transform e load.

#### Base de dados

A base de dados utilizada foi um arquivo CSV. Trata-se de um catálogo de filmes da Netflix. Pode ser encontrada no Kaggle nesse endereço:

<https://www.kaggle.com/code/lucasnatali/netflix-movies-and-tv-shows>

#### Normalizando os dados

O dataset veio com vários problemas:

* Na coluna date_added o mês veio escrito por extenso ao invés de numérico. Exemplo: December 23, 2016. A solução foi utilizar o replace para substituir o mês por um valor númerico.

* Alguns meses vieram com dias que não existem. Exemplo: o mês de Abril tinha 31 dias. A solução foi utilizar o replace para substituir o dia do mês inexistente por um existente.

* Alguns dados vieram com aspas simples e duplas no meio do texto o que não permitia popular o Mysql com os todos os dados. Na hora da inserção o Mysql entendia que havia colunas a mais e isso gerava erro. A solução foi remover algumas aspas simples e duplas utilizando o replace.
