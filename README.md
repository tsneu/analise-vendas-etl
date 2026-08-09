#  Demo de ETL na AWS com  Glue e PySpark - análise de vendas

Job de ETL para demonstrar tratamentos de dados nas camadas Bronze, Silver e Gold, e  uso de Apache Iceberg contemplando o gerenciamento otimizado para grandes volumes de dados.

Esta demo foi construída com o auxílio da Gemini IA.


## Principais recursos

* Terraform - para gerenciar a infraestrutura como código (IaC)
* AWS Glue - para executar o job com PySpark e catalogar as tabelas criadas
* AWS S3 - para repositório de dados
* Poetry - gerenciador de pacotes


## infra

Utilizado terraform para criar e gerenciar a infraestrutura na nuvem da AWS.

Gestão de Custos: A configuração utiliza `worker_type = "G.1X"` e `number_of_workers = 2`, que é o número mínimo exigido pela AWS para criar um cluster Spark no Glue 4.0, garantindo o menor custo possível na validação. Esta configuração pode ser alterada no arquivo `prod.tvars` para os valores apropriados para um ambiente de produção.

Suporte a Iceberg: O argumento `--datalake-formats = "iceberg"` habilita as bibliotecas do Apache Iceberg nativamente na versão 4.0 do AWS Glue.

Injeção de Argumentos Dinâmicos: Os caminhos dos buckets são passados dinamicamente para o script de Glue através de argumentos (--datalake-bronze, etc.), garantindo desacoplamento entre infraestrutura e código de aplicação.

IU Spark: o argumento `--enable-spark-ui` ativa o recurso de monitoramento do Spark que permite acompanhar a execução do job no ambiente da AWS, e avaliar o funcionamento dos works, tempo de execução, falhas e paralelismo, entre outros.

Logs para observabilidade: O argumento `--enable-continuous-cloudwatch-log` ativa a geração de logs da plataforma AWS, que podem ser capturados por uma ferramenta de observabilidade, como o DataDog, permitindo a criação de paineis de acompanhamento dos jobs em ambientes de produção.


## Limitações

Para o uso do AWS Glue 4.0, as versões do Python e do (Py)Spark ficam limitadas às versões 3.10.x e 3.3.x respectivamente. Versões mais recentes ocasionarão incompatibilidade com a plataforma.

