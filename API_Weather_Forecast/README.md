**INTRODUÇÃO**


O código foi escrito em resposta ao desafio proposto pela empresa Dataside.


**DESAFIO: CONSUMO DE DADOS PARA PREVISÃO DO TEMPO DAS CIDADES DO VALE DO PARAÍBA**


**Objetivo**

Avaliar conhecimentos nas linguagens Python e SQL e na engine de processamento Apache Spark.

**Descrição**

Neste desafio, você desenvolverá um notebook que será responsável por extrair dados de previsão do tempo das cidades do Vale do Paraíba, região onde se localiza a Dataside. Para consultar todas as cidades dessa região, utilizaremos a API do IBGE. No caso, basta realizar uma requisição HTTP com o método GET, utilizando a URL abaixo:

<https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes/3513/municipios>

Com esses dados, gerar um data frame e a partir dele uma temp view. Ex: "cities"

Utilizando os nomes das cidades, deverão ser consultados os dados de previsão de tempo para cada cidade. Para realizar essa consulta, poderá ser utilizada qualquer uma das APIs informadas no link abaixo.

Public APIs - Wather

Obs.: Para algumas, pode ser necessário cadastrar-se para acessar sua API Key. Mas nenhuma delas deve precisar cadastrar cartão de crédito ou adicionar qualquer valor monetário para utilizar. Caso alguma solicite, basta optar por outra.

Com os dados consultados, gerar um data frame e partir dele outra temp view. Ex: "forecasts"

Com as temp views geradas, utilizar Spark SQL para criar queries e gerar data frames das seguintes tabelas:

**Tabela 1:** dados de previsão do tempo para os próximos cinco dias, para cada data e cidade consultadas. As colunas dessa tabela serão:

* Cidade

* CodigoDaCidade

* Data

* Regiao

* Pais

* Latitude

* Longigute

* TemperaturaMaxima

* TemperaturaMinima

* TemperaturaMedia

* VaiChover

* ChanceDeChuva

* CondicaoDoTempo

* NascerDoSol

* PorDoSol

* VelocidadeMaximaDoVento

Obs.: Os valores da coluna "VaiChover" deverá ser "Sim" ou "Não". E a coluna "CodigoDaCidade" é o ID retornado junto com os nomes da cidades na API do IBGE. Obs.: Dependendo da API utilizada, algumas colunas podem não existir e ficarão em branco. Você deve optar por uma API que traga o maior número de informações possível.

**Tabela 2**: quantidade de dias com chuva e sem chuva para os dias consultados, para cada data consultada. Colunas:

Cidade
QtdDiasVaiChover
QtdDiasNaoVaiChover
TotalDiasMapeados
Essas tabelas deverão ser exportadas em formado CSV e entregue no final do desafio.

**To Do**

[ ] - Consultar municípios do Vale do Paraíba, gerar um data frame e criar uma temp view com esses dados. [ ] - Consultar dados do tempo para cada município, gerar um data frame e criar uma outra temp view. [ ] - Utilizar Spark SQL para gerar os data frames das Tabelas 1 e 2. [ ] - Exportar os data frames para CSV.

**Atenção**

Existe um limite de requisições de 10000 requests por conta cadastrada na m3o.
Essa API pode retornar cidades de outras regiões que possuem nome semelhante a alguma cidade do Vale do Paraiba. Pode mantê-las ou filtrar para gerar as tabelas apenas com dados de Regiao = Sao Paulo. Fica a seu critério.

**Entregando o desafio**

Concluindo todos os passos informados em To Do, basta salvar o arquivo .ipynb do notebook e enviar para a Dataside juntamente com os CSVs das duas tabelas.





