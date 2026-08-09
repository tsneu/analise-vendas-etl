import os
import sys
import pytest
import boto3
import json
from moto import mock_aws
from types import ModuleType
from unittest.mock import MagicMock

# 1. Força o uso do Java 11 instalado pelo apt no Ubuntu
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# 2. Como estamos rodando localmente sem o ambiente da AWS,
# precisamos mockar APENAS o AWS Glue para que ele não quebre os imports.
awsglue_mock = ModuleType("awsglue")
awsglue_utils_mock = ModuleType("awsglue.utils")
awsglue_mock.utils = awsglue_utils_mock

# Define o mock para o getResolvedOptions ler os argumentos do pytest
def _get_resolved_options(args, options_list):
    resultado = {}
    for opt in options_list:
        flag = f"--{opt}"
        if flag in args:
            idx = args.index(flag)
            resultado[opt] = args[idx + 1] if idx + 1 < len(args) else ""
        else:
            resultado[opt] = f"mock-{opt.lower()}"
    return resultado

awsglue_utils_mock.getResolvedOptions = _get_resolved_options

sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.utils"] = awsglue_utils_mock
sys.modules["awsglue.context"] = MagicMock()
sys.modules["awsglue.job"] = MagicMock()


@pytest.fixture(scope="session")
def aws_credentials():
    """Configura credenciais fictícias para evitar acessar a AWS real."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# Cria um Fixture do PySpark real para ser usado nos seus testes de DataFrame
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session(aws_credentials):
    """Cria uma sessão local do Spark isolada para os testes unitários."""

    endpoint_url = "http://127.0.0.1:5000"

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("testes-glue-local") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "testing") \
        .config("spark.hadoop.fs.s3a.secret.key", "testing") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("pyspark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def s3_bucket(aws_credentials):
    """Cria e gerencia o ciclo de vida de um bucket S3 mockado."""
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        bucket_name = "datalake-teste"
        s3.create_bucket(Bucket=bucket_name)

        dados_bronze = [
            {"order_id": "1001", "customer_id": "10", "total_amount": "59.80", "order_timestamp": "2026-08-01 10:15:00"},
            {"order_id": "1002", "customer_id": "12", "total_amount": "91.46", "order_timestamp": "2026-08-01 10:45:00"},
            {"order_id": "1003", "customer_id": "34", "total_amount": "159.20", "order_timestamp": "2026-08-01 12:04:00"},
        ]
        conteudo_json = json.dumps(dados_bronze).encode('utf-8')
        s3.Bucket(bucket_name).put_object(Key="bronze/vendas/dados.json", Body=conteudo_json)

        yield bucket_name
