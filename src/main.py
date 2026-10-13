"""Interface de linha de comando da pipeline de vinculacao de enderecos."""

import argparse
import logging

from src.elasticsearch_service import (
    create_elasticsearch_client,
    wait_for_elasticsearch,
)
from src.index_cnefe import index_cnefe
from src.logging_utils import configure_logging
from src.match_addresses import match_addresses
from src.settings import Settings
from src.spark_utils import create_spark_session
from src.validate_output import validate_output, write_validation


LOGGER = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Indexa o CNEFE e vincula os enderecos da base de busca."
    )
    parser.add_argument(
        "command",
        choices=("index", "match", "all", "validate"),
        nargs="?",
        default="all",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Indexa apenas uma amostra; destinado a testes de infraestrutura.",
    )
    parser.add_argument(
        "--keep-index",
        action="store_true",
        help="Nao recria um indice Elasticsearch que ja existe.",
    )
    parser.add_argument(
        "--all-municipalities",
        action="store_true",
        help="Indexa toda a Bahia em vez dos municipios consultados.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    settings = Settings.from_env()
    overrides = dict(settings.__dict__)
    if args.keep_index:
        overrides["recreate_index"] = False
    if args.all_municipalities:
        overrides["relevant_municipalities_only"] = False
    settings = Settings(**overrides)

    settings.output_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(settings.output_dir)

    if args.command == "validate":
        report = validate_output(settings.search_path, settings.result_path)
        write_validation(report, settings.validation_path)
        LOGGER.info("Resultado valido: %s", settings.result_path)
        return

    client = create_elasticsearch_client(settings)
    wait_for_elasticsearch(client, settings.elasticsearch_request_timeout)
    spark = create_spark_session("cnefe-address-linkage", settings)
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.command in {"index", "all"}:
            index_cnefe(spark, client, settings, limit=args.limit)
        if args.command in {"match", "all"}:
            match_addresses(spark, client, settings)
    finally:
        spark.stop()
        client.close()


if __name__ == "__main__":
    main()
