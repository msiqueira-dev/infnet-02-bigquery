**Infraestrutura Kafka [24E1_2]**

Marcus Vinicius Barreto Siqueira

1. Escolha 5 conceitos fundamentais sobre o Google BigQuery e os descreva.
    1. O BigQuery é um Data Lake da Google com toda uma infraestrutura própria e integrada com os serviços do Google.
    2. O BigQuery é um Data Lake analítico, ou seja, é um banco de dados focado em processamento de dados e é um banco não relacional. É um banco não recomendado para transações de plataformas como por exemplo APIS que fazem operações CRUD com constância.
    3. O BigQuery tem um custo muito baixo para armazenamento de dados e o modelo financeiro do mesmo cobra por consultas não cacheadas no banco de dados, ou seja, é cobrado a conta que realiza a consulta no BigQuery.
    4. É uma plataforma de armazenamento e processamento de de dados em BigData, onde a estrutura do BigQuery é inteira por conta do Google, que segue uma arquitetura própria construida pela Google, utilizando multiplos nós para processamento, uma parte isolada para storage, e uma rede interna de comunicação entre os componentes.
    5. Por ser tratar de um banco não relacional, o BigQuery aceita dados duplicados, e metodologias devem ser implantadas para tratamento dos dados.
2. Descreva como é a arquitetura do Google BigQuery.
    1. A interface do BigQuery se comunica com os nós através da tecnologia Dremel, onde toda comunicação interna entre chamadas e respostas são feitas através da rede Jupiter. O armazenamento dos dados fica por conta de uma tecnologia de Storage chamada Colossus. O Borg é outro tecnologia privada que atua como um gerenciador de todo o sistema de execução de queries, de armazenamento e controle dos nós. Não tão antigo o google implementou a tecnologia de Streaming Buffer, onde dados inseridos via Streaming passam um tempo em um Buffer de Streaming, enquanto neste estado, estes dados não pode ser apagados ou atualizados, sendo necessário aguardar 90 minutos, para que esses dados sejam movidos para o Storage Colossus, e aí sim, podem ser atualizados ou excluidos, enquanto apenas no buffer os dados ficam prontos para consumo de maneira imediata.
3. Apresente exemplos de utilização do Google BigQuery em bases NoSQL e SQL.
    1. NoSQL são bases em formato JSON/XML etc, onde a inserção segue o padrão JSON, por exemplo, baseado em tags. Exemplos NoSQL anexados no projeto nomeados de example_no_sql_create.json e example_no_sql_insert.json.
    2. SQL significa Structured Query Language, é uma linguagem aceita por praticamente todos os banco de dados, e os comandos básicos seguem os mesmos. Sendo assim, o BigQuery aceita criações de tabelas no formato SQL, como por exemplo Create Table....E aceita realização de queries para manipulação dos dados, como Insert para inserção, Delete para deleção, Update para atualização e Select para obtenção. Para o esquema usado anteriormente, segue exemplo de manipulação dos dados através de linguagem SQL:
    select * from `raw_inimigo_do_google.product`;
4. Descreva os principais benefícios em utilizar o Google BigQuery
    1. O BigQuery é um banco de dados que oferece alta performance para processamento de dados e consegue trabalhar com Big Data, grande volumes de dados, além de ter uma estrutura escalável, gerenciada pelo Google. Ao contrário de instancia de bancos de dados relacionais, onde a estrutura, é gerenciado por um time de infraestrutura, no BigQuer a mesma é totalmente escalável e gerenciado pelo google. Além disso, não é necessário ampliar a estrutura em casos de necessidade de mais uso do BigQuery, a estrutura do Google está prepara para execuções diversas a todo momento. Também diferente de outros Data Warehouses, como os concorrentes diretos, Red Shift (Amazon), o BigQuery tem um custo escalável com base em boa construção, e consumo planejado dos dados, enquanto que o Red Shift, requer a necessidade de aumentar a instancia, o que escala o custo mesmo que o uso não seja tão efecaz (100% do recurso).
5. O que é um pipeline de dados
    1. Pipeline de dados é um processo onde é definido / desenhado todo o trajeto do dado, desde sua obtenção até o seu consumo. Pipeline de dados podem ser construídos através de multiplas ferramentas e/ou linguagens de programação. Em um pipeline de dados existe uma fonte de dados, um ou mais etapas de processamento de dados e por fim o destino.
6. Dê 2 (dois) exemplos de aplicações onde os pipelines de dados são utilizados em seu dia-a-dia.
    1. Obtenção de dados do Varejo através de coleta de dados em um Banco de Dados relacional, processamento destes dados e armazenamento de dados em um banco relacional transacional, se caracterizando integração de dados. O dado é replicado através de um serviço que gera arquivos .parquet (alta compressão) e obtido pelo Google Data Transfer, armazenado no Google Cloud Storage, depois processado pelo Airflow e armazenado no BigQuery.
    2. Obtenção de dados via API Rest em integração de dados, que valida, processa e armazena em uma instancia de banco relacional, mas também, como etapa seguinte do sistema em Python, insere em massa via Inserção Streaming no BigQuery utilizando a biblioteca google.cloud.bigquery
7. Selecione uma base de dados pública brasileira para utilizar neste exercício. Você pode baixá-la em algum formato que desejar (ex.: formato .csv). Informe onde e como você conseguiu os seus dados. Explique se são estruturados ou não estruturados. Cada linha/registro em seu banco de dados corresponde a quais informações? Cada registro possui quantas colunas associadas e quais atributos elas representam? Qual o tamanho do banco de dados escolhido?
    1. A base de dados publica escolhida foi da Escola Virtual, que contempla dados de matricula de alunos na escola virtual, acessível através do link: https://emnumeros.escolavirtual.gov.br/dados-abertos/
    2. Os dados são estruturados, em formato CSV (necessário extrair os arquivos compactados).
    3. Cada linha contempla uma matricula de um aluno, como nome e outros dados pessoais (CPF e dado que identificam uma pessoa foram mascarados, pela própria escola virtual).
    4. São um total de 29 colunas, cujo o separador é o caractere '|'.
    5. Utilizei dois arquivos, base de dados de 2018 e de 2022, a base de 2018 possui 174MB e um total de 502311 dados, a base de 2022, possui 795MB e um total de 2266168 dados
8. Formule pelo menos 2 perguntas sobre sua base de dados. O que você quer saber sobre os dados que escolheu?
    1. Diante das duas bases de dados, que contenplam tempos diferentes, qual deve possuir mais alunos?
        1. SELECT count(1) FROM `woven-honor-413920.escola_virtual.matricula_2018`
            -- TOTAL MATRICUAL DE 2018: 502311
        2. SELECT count(1) FROM `woven-honor-413920.escola_virtual.matricula_2022`
            -- TOTAL MATRICUAL DE 2018: 2266168
    2. Quantos cursos de dados haviam em 2018 e em 2022?
        1. SELECT distinct(nome_curso) FROM `woven-honor-413920.escola_virtual.matricula_2018`where nome_curso like '%dado%'
            -- 0 cursos de dados
        2. SELECT distinct(nome_curso) FROM `woven-honor-413920.escola_virtual.matricula_2022`where nome_curso like '%dado%'
            -- Total de 4 Cursos. 
                Análise de dados como suporte à tomada de decisão
                Análise de dados: uma leitura crítica das informações
                Monitoramento da biodiversidade: gestão, análise e síntese dos dados
                LGPD: Como coordenar a atuação do município para a governança de dados aplicada
9. Formule uma hipótese sobre o que você acha que vai encontrar quando filtrar e analisar seus dados.
    1. Uma hipótese já respondida nas perguntas acima é que, há um aumento de alunos e também de cursos de dados quando se analisa o ano de 2018 e ano 2022, isso decorrente de que a pandemia forçou que muitos cursos passassem a ser online, e que muitas pessaos se adequassem ao novo modelo de ensino, por tanto, cursos online ganharem diversos novos alunos. A propria infnet tem cursos hoje em modalidade online que talvez em 2010 (por exemplo), não fosse o caso. Referente a cursos de dados, acredito que por ser um tema mais moderno, a procura pelo mesmo aumentou, e um dos resultados a cima, curso especifico de LGPD, este só pode ser possível um periodo após a lei em questão ter entrado em vigor, que foi meados de 2018.
10. Crie uma nova variável a partir de outras variáveis da base de dados que te auxilie na avaliação de sua   hipótese.
    1. select count(1) FROM `woven-honor-413920.escola_virtual.matricula_2018` where uf_servidor = 'RJ' or uf_servidor = 'SP' 
    -- Total de alunos no estado do RJ e de SP 57098
    2. select count(1) FROM `woven-honor-413920.escola_virtual.matricula_2022` where uf_servidor = 'RJ' or uf_servidor = 'SP' 
    -- Total de alunos no estado do RJ e de SP 161985
    Como já esperado, o número total de alunos nas grandes metrópoles teve um grande aumento, pois foram cidades de grande impacto pelo Lockdown durante a pandemia, e ao mesmo tempo são cidades cujo o acesso a internet e difusão da informação é grande, sendo assim é mais propenço que a tecnologia tenha mais impacto em grandes centros urbanos e estados desenvolvidos.
11. Importe a sua base de dados na infraestrutura Google BigQuery. Inclua em seu relatório a forma que você realizou a importação.
    1. Como ambos os arquivos são maiores do que o permitido no upload direto no BigQuery com arquivos locais (limite de 100MB), foi necessário enviar os dois arquivos para o Google Cloud Storage (imagem anexo: google_cloud_storage.jpg), após isso, é necessário importar direto pro BigQuery utilizando a função Google Cloud Storage e marcando a opção de delimitador ser "pipe" (imagem anexo: google_big_query_import_1 google_big_query_import_2 google_big_query_import_3 google_big_query_import_4). A criação da tabela é feita, e os dados importados, onde é necessário que a tabela não exista. O separador ser o pipe, deve-se pois a base de dados veio com o separador pipe em sua origem.
12. Realize pré-processamento dos dados importados. Inclua eu seu relatório os códigos utilizados para o pré-processamento e criação de novas variáveis.
    1. Processamento realizado pelo arquivo pipeline.py que utiliza o connector_bigquery.py para operações diretas no BigQuery