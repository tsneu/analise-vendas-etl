import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


def init_spark_session(path_silver) -> SparkSession:
    """Inicializa a sessão Spark com configurações nativas para Apache Iceberg no AWS Glue."""
    return (
        SparkSession.builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            "spark.sql.catalog.glue_catalog.warehouse",
            f"{path_silver}",
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .getOrCreate()
    )


# ------------------------------------------------------------------------------
# 1. Leitura da Camada BRONZE (Raw JSON / CSV)
# ------------------------------------------------------------------------------
def read_bronze_data(spark: SparkSession, path_bronze: str):
    """Lê os dados brutos de vendas em formato JSON armazenados no S3."""
    print(f"{path_bronze}vendas/")
    return spark.read.option("multiline", "true").json(f"{path_bronze}vendas/")


# ------------------------------------------------------------------------------
# 2. Transformação e Carga na Camada PRATA (Clean & Enriched - Apache Iceberg)
# ------------------------------------------------------------------------------
def transform_to_silver(df_bronze):
    """
    Aplica regras de limpeza e padronização:
    - Remoção de duplicatas
    - Tratamento de nulos
    - Casting de tipos de dados
    - Criação de colunas particionadoras (ano, mês)
    """
    df_silver = (
        df_bronze.dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("order_id", F.col("order_id").cast(IntegerType()))
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("order_timestamp", F.col("order_timestamp").cast(TimestampType()))
        .withColumn("year", F.year(F.col("order_timestamp")))
        .withColumn("month", F.month(F.col("order_timestamp")))
    )
    return df_silver


def write_silver_iceberg(df_silver, db_name: str, table_name: str = "stg_vendas"):
    """Escreve a camada Prata em tabela Apache Iceberg registrada no Glue Catalog."""
    full_table_name = f"glue_catalog.{db_name}.{table_name}"

    df_silver.writeTo(full_table_name).tableProperty(
        "format-version", "2"
    ).partitionedBy(F.col("year"), F.col("month")).createOrReplace()


# ------------------------------------------------------------------------------
# 3. Agregação e Carga na Camada OURO (Business Curated - Apache Iceberg)
# ------------------------------------------------------------------------------
def transform_to_gold_kpi_vendas_diarias(df_silver):
    """Calcula métricas agregadas de vendas por dia para relatórios de BI/Analytics."""
    return (
        df_silver.groupBy(F.to_date("order_timestamp").alias("data_venda"))
        .agg(
            F.count("order_id").alias("total_pedidos"),
            F.sum("total_amount").alias("faturamento_total"),
            F.avg("total_amount").alias("ticket_medio"),
        )
        .withColumn("faturamento_total", F.round("faturamento_total", 2))
        .withColumn("ticket_medio", F.round("ticket_medio", 2))
    )


def write_gold_iceberg(df_gold, db_name: str, table_name: str = "fct_vendas_diarias"):
    """Escreve a camada Ouro pronta para consultas de negócios no Athena."""
    full_table_name = f"glue_catalog.{db_name}.{table_name}"

    df_gold.writeTo(full_table_name).tableProperty(
        "format-version", "2"
    ).createOrReplace()


# ------------------------------------------------------------------------------
# Execução Principal (Pipeline Flow)
# ------------------------------------------------------------------------------
def main():
    # Obtém argumentos passados pelo Glue/Terraform
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "datalake_bronze",
            "datalake_silver",
            "datalake_gold",
            "database_name",
        ],
    )

    path_bronze = args["datalake_bronze"]
    path_silver = args["datalake_silver"]
    db_name = args["database_name"]

    spark = init_spark_session(path_silver)

    # 1. Processa Bronze -> Silver
    df_bronze = read_bronze_data(spark, path_bronze)
    df_silver = transform_to_silver(df_bronze)
    write_silver_iceberg(df_silver, db_name=db_name)

    # 2. Processa Silver -> Gold
    df_gold = transform_to_gold_kpi_vendas_diarias(df_silver)
    write_gold_iceberg(df_gold, db_name=db_name)


if __name__ == "__main__":
    main()