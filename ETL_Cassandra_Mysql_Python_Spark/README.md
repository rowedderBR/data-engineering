### INTRODUÇÃO

Nesse projeto foi aplicado o processo conhecido em Engenharia de dados como ETL: Extract, transform e load.

#### Base de dados

A base de dados utilizada foi um arquivo CSV. Trata-se de um catálogo de filmes da Netflix. Pode ser encontrada no Kaggle nesse endereço:

<https://www.kaggle.com/code/lucasnatali/netflix-movies-and-tv-shows>

#### Normalizando os dados

O dataset veio com vários problemas:

* Na coluna date_added o mês veio escrito por extenso ao invés de numérico. Exemplo: December 23, 2016.

* Alguns meses vieram com dias que não existem. Exemplo: O mês de Abril tinha 31 dias.

