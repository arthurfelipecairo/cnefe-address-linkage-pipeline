import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    """Cria a sessão local do Spark."""
    return (
        SparkSession.builder
        .appName("cnefe-data-profile")
        .master("local[*]")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str):
    """Lê inicialmente todas as colunas como texto."""
    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", False)
        .csv(path)
    )


def show_basic_profile(df, dataset_name: str) -> None:
    """Mostra informações gerais sobre um DataFrame."""
    print(f"\n===== {dataset_name} =====")

    print(f"Quantidade de linhas: {df.count()}")
    print(f"Quantidade de colunas: {len(df.columns)}")
    print(f"Colunas: {df.columns}")

    print("\nEsquema:")
    df.printSchema()

    print("\nAmostra:")
    df.show(5, truncate=False)


def show_null_counts(df) -> None:
    """Conta valores nulos e strings vazias em cada coluna."""
    expressions = [
        F.sum(
            F.when(
                F.col(column).isNull()
                | (F.trim(F.col(column)) == ""),
                1,
            ).otherwise(0)
        ).alias(column)
        for column in df.columns
    ]

    print("\nValores ausentes por coluna:")
    df.select(expressions).show(truncate=False, vertical=True)


def show_distinct_counts(df) -> None:
    """Estima a quantidade de valores diferentes por coluna."""
    expressions = [
        F.approx_count_distinct(F.col(column)).alias(column)
        for column in df.columns
    ]

    print("\nValores distintos aproximados:")
    df.select(expressions).show(truncate=False, vertical=True)


def profile_dataset(df, dataset_name: str) -> None:
    show_basic_profile(df, dataset_name)
    show_null_counts(df)
    show_distinct_counts(df)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Gera o perfil das bases do projeto."
    )

    parser.add_argument(
        "--dataset",
        choices=["cnefe", "search"],
        default="cnefe",
        help="Base que será analisada.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    cnefe_path = os.getenv(
        "CNEFE_PATH",
        "/app/raw/29_BA.csv",
    )

    search_path = os.getenv(
        "SEARCH_PATH",
        "/app/raw/2026_teste_tecnico_relinke_cidacs(in).csv",
    )

    datasets = {
        "cnefe": {
            "path": cnefe_path,
            "name": "CNEFE Bahia",
        },
        "search": {
            "path": search_path,
            "name": "Base de busca",
        },
    }

    selected_dataset = datasets[args.dataset]

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Lendo arquivo: {selected_dataset['path']}")

        dataframe = read_csv(
            spark,
            selected_dataset["path"],
        )

        profile_dataset(
            dataframe,
            selected_dataset["name"],
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()