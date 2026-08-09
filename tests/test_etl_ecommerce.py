import sys
from chispa import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

# Importa as funções de transformação do seu job
from src.jobs.etl_ecommerce import (
    transform_to_gold_kpi_vendas_diarias,
    transform_to_silver,
    main
)

def test_main_com_sucesso(spark_session, monkeypatch, s3_bucket):
    '''Testa a função main usando o fixture de argumentos.'''

    # Se quiser forçar valores específicos para este teste, mude o sys.argv
    monkeypatch.setattr(
        sys, 
        'argv', 
        [
            'script.py', 
            '--JOB_NAME', 'meu-job', 
            '--datalake_bronze', f's3a://{s3_bucket}/bronze/',
            '--datalake_silver', f's3a://{s3_bucket}/silver/',
            '--datalake_gold', f's3a://{s3_bucket}/gold/',
            '--database_name', 'dbtest'
        ],
    )

    resultado = main()
    
    assert resultado['JOB_NAME'] == 'meu-job'
    assert resultado['datalake_gold'] == f's3a://{s3_bucket}/gold/'


def test_transform_to_silver_deduplication_and_casting(spark_session):
    """Garante que registros duplicados por 'order_id' sejam removidos

    e que a tipagem dos dados seja ajustada corretamente.
    """
    # 1. Esquema e dados de entrada (Simulando Bronze Raw)
    schema_bronze = StructType()
    schema_bronze.add("order_id", StringType())
    schema_bronze.add("customer_id", StringType())
    schema_bronze.add("total_amount", StringType())
    schema_bronze.add("order_timestamp", StringType())

    data_bronze = [
        # Registro Válido 1
        ("1001", "501", "150.50", "2026-08-01 10:15:00"),
        # Duplicata exata do Registro 1
        ("1001", "501", "150.50", "2026-08-01 10:15:00"),
        # Registro com order_id NULO (Deve ser filtrado)
        (None, "502", "89.90", "2026-08-01 11:20:00"),
        # Registro Válido 2
        ("1002", "503", "310.00", "2026-08-02 14:05:00"),
    ]

    df_bronze_mock = spark_session.createDataFrame(data_bronze, schema=schema_bronze)

    # 2. Executa a transformação Prata
    df_silver_actual = transform_to_silver(df_bronze_mock)

    # 3. Define o resultado esperado
    data_expected = [
        (1001, 501, 150.50, 2026, 8,),
        (1002, 503, 310.00, 2026, 8,),
    ]
    schema_bronze_expected = StructType()
    schema_bronze_expected.add("order_id", IntegerType())
    schema_bronze_expected.add("customer_id", IntegerType())
    schema_bronze_expected.add("total_amount", DoubleType())
    schema_bronze_expected.add("year", IntegerType())
    schema_bronze_expected.add("month", IntegerType())
    
    df_silver_expected = spark_session.createDataFrame(
        data=data_expected,
        schema=schema_bronze_expected,
    )

    # 4. Asserção com Chispa (Valida schema e conteúdo ignorando order_timestamp por simplicidade)
    columns_to_compare = [
        "order_id",
        "customer_id",
        "total_amount",
        "year",
        "month",
    ]
    assert_df_equality(
        df_silver_actual.select(columns_to_compare),
        df_silver_expected.select(columns_to_compare),
        ignore_nullable=True,
    )


def test_transform_to_gold_kpi_vendas_diarias(spark_session):
    """Garante que a agregação por data calcule corretamente
    total de pedidos, faturamento e ticket médio.
    """
    # 1. Dados mockados da camada Prata
    data_silver = [
        ("1001", "501", 100.00, "2026-08-01 10:00:00"),
        ("1002", "502", 200.00, "2026-08-01 15:00:00"),
        ("1003", "503", 500.00, "2026-08-02 11:00:00"),
    ]
    schema_silver = StructType()
    schema_silver.add("order_id", StringType())
    schema_silver.add("customer_id", StringType())
    schema_silver.add("total_amount", DoubleType())
    schema_silver.add("order_timestamp", StringType())

    df_silver_mock = spark_session.createDataFrame(data=data_silver, schema=schema_silver)

    # 2. Executa a transformação Ouro
    df_gold_actual = transform_to_gold_kpi_vendas_diarias(
        df_silver_mock
    ).orderBy("data_venda")

    # 3. Resultado Esperado para a Ouro
    data_expected = [
        ("2026-08-01", 2, 300.00, 150.00,),  # 2 pedidos, 300 total, 150 ticket
        ("2026-08-02", 1, 500.00, 500.00),  # 1 pedido, 500 total, 500 ticket
    ]

    df_gold_expected = (
        spark_session.createDataFrame(
            data_expected,
            ["data_venda", "total_pedidos", "faturamento_total", "ticket_medio"],
        )
        .withColumn(
            "data_venda", 
            F.col("data_venda").cast(df_gold_actual.schema["data_venda"].dataType)
        )
        .orderBy("data_venda")
    )

    # 4. Asserção
    assert_df_equality(df_gold_actual, df_gold_expected, ignore_nullable=True)