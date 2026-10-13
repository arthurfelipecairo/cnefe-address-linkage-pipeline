"""Leitura das bases e criacao da sessao Spark."""

import csv

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.normalizacao import normalize_search_dataframe
from src.settings import Settings


SEARCH_COLUMNS = (
    "consulta_logradouro",
    "consulta_numero",
    "consulta_bairro",
    "consulta_cep",
    "consulta_complemento",
    "consulta_municipio",
)


def create_spark_session(
    app_name: str,
    settings: Settings | None = None,
) -> SparkSession:
    settings = settings or Settings.from_env()

    return (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config(
            "spark.sql.shuffle.partitions",
            str(settings.spark_stream_partitions),
        )
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Le um CSV com ponto e virgula mantendo todas as colunas como texto."""
    return (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", False)
        .csv(path)
    )


def read_search_records(path: str) -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, delimiter=";")
        missing = set(SEARCH_COLUMNS) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(
                "Colunas ausentes na base de busca: "
                + ", ".join(sorted(missing))
            )
        return [dict(row) for row in reader]


def create_normalized_search_dataframe(
    spark: SparkSession,
    path: str,
) -> tuple[DataFrame, list[dict[str, str]]]:
    """Cria um DataFrame normalizado preservando a ordem do arquivo original."""
    records = read_search_records(path)
    schema = StructType(
        [StructField("consulta_id", StringType(), False)]
        + [StructField(column, StringType(), True) for column in SEARCH_COLUMNS]
    )
    values = [
        (str(position), *(record.get(column, "") for column in SEARCH_COLUMNS))
        for position, record in enumerate(records, start=1)
    ]
    dataframe = spark.createDataFrame(values, schema=schema)

    return normalize_search_dataframe(dataframe), records
