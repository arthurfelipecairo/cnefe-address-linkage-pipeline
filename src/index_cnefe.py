"""Preparacao e indexacao do CNEFE no Elasticsearch."""

from datetime import datetime, timezone
import json
import logging
from typing import Any, Iterator

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.elasticsearch_service import ensure_index, set_refresh_interval
from src.normalizacao import normalize_cnefe_dataframe
from src.settings import Settings
from src.spark_utils import create_normalized_search_dataframe, read_csv


LOGGER = logging.getLogger(__name__)


def prepare_cnefe_for_index(dataframe: DataFrame) -> DataFrame:
    """Seleciona somente os campos necessarios para recuperar enderecos."""
    normalized = normalize_cnefe_dataframe(dataframe)
    street_original = F.concat_ws(
        " ",
        F.col("NOM_TIPO_SEGLOGR"),
        F.col("NOM_TITULO_SEGLOGR"),
        F.col("NOM_SEGLOGR"),
    )
    complement_original = F.concat_ws(
        " ",
        F.col("NOM_COMP_ELEM1"),
        F.col("VAL_COMP_ELEM1"),
        F.col("NOM_COMP_ELEM2"),
        F.col("VAL_COMP_ELEM2"),
        F.col("NOM_COMP_ELEM3"),
        F.col("VAL_COMP_ELEM3"),
        F.col("NOM_COMP_ELEM4"),
        F.col("VAL_COMP_ELEM4"),
        F.col("NOM_COMP_ELEM5"),
        F.col("VAL_COMP_ELEM5"),
    )

    return (
        normalized.select(
            F.col("COD_UNICO_ENDERECO").alias("id_endereco"),
            street_original.alias("logradouro_original"),
            "logradouro_normalizado",
            F.col("NUM_ENDERECO").alias("numero_original"),
            "numero_normalizado",
            F.col("DSC_LOCALIDADE").alias("localidade_original"),
            "localidade_normalizada",
            F.col("CEP").alias("cep_original"),
            "cep_normalizado",
            F.col("COD_MUNICIPIO").alias("municipio_original"),
            "municipio_normalizado",
            complement_original.alias("complemento_original"),
            "complemento_normalizado",
            F.col("COD_SETOR").alias("setor_censitario"),
            "cep_valido",
            "municipio_valido",
        )
        .filter(F.col("id_endereco").isNotNull())
        .filter(F.col("setor_censitario").isNotNull())
        .filter(F.col("municipio_normalizado").isNotNull())
        .filter(F.col("logradouro_normalizado").isNotNull())
    )


def _document_actions(
    dataframe: DataFrame,
    index_name: str,
) -> Iterator[dict[str, Any]]:
    """Converte particoes Spark em um fluxo, sem coletar o CNEFE na memoria."""
    for row in dataframe.toLocalIterator():
        document = {
            key: value
            for key, value in row.asDict(recursive=True).items()
            if value is not None
        }
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_id": str(document["id_endereco"]),
            "_source": document,
        }


def index_cnefe(
    spark: SparkSession,
    client: Elasticsearch,
    settings: Settings,
    limit: int | None = None,
) -> dict[str, Any]:
    """Normaliza e indexa o CNEFE; retorna metadados da execucao."""
    LOGGER.info("Lendo CNEFE em %s", settings.cnefe_path)
    dataframe = prepare_cnefe_for_index(read_csv(spark, settings.cnefe_path))

    municipalities: list[str] = []
    if settings.relevant_municipalities_only:
        search_dataframe, _ = create_normalized_search_dataframe(
            spark,
            settings.search_path,
        )
        municipalities = [
            row.municipio_normalizado
            for row in search_dataframe.select("municipio_normalizado")
            .where(F.col("municipio_normalizado").isNotNull())
            .distinct()
            .collect()
        ]
        dataframe = dataframe.filter(
            F.col("municipio_normalizado").isin(municipalities)
        )
        LOGGER.info(
            "Indexacao limitada aos %d municipios presentes na busca.",
            len(municipalities),
        )

    if limit is not None:
        if limit <= 0:
            raise ValueError("--limit precisa ser maior que zero.")
        dataframe = dataframe.limit(limit)
        LOGGER.warning("Modo de amostra ativo: no maximo %d documentos.", limit)

    # A deduplicacao ocorre depois do filtro municipal para reduzir o shuffle.
    # O repartition explicito impede que toLocalIterator tente transportar uma
    # particao de varios gigabytes de uma so vez para o processo Python.
    stream_partitions = (
        min(settings.spark_stream_partitions, 8)
        if limit is not None
        else settings.spark_stream_partitions
    )
    dataframe = dataframe.dropDuplicates(["id_endereco"]).repartition(
        stream_partitions
    )

    ensure_index(
        client,
        settings.elasticsearch_index,
        settings.elasticsearch_mapping_path,
        settings.recreate_index,
    )
    set_refresh_interval(client, settings.elasticsearch_index, "-1")

    indexed = 0
    failures: list[dict[str, Any]] = []
    try:
        for success, item in helpers.streaming_bulk(
            client,
            _document_actions(dataframe, settings.elasticsearch_index),
            chunk_size=settings.index_batch_size,
            max_chunk_bytes=50 * 1024 * 1024,
            max_retries=3,
            initial_backoff=2,
            request_timeout=settings.elasticsearch_request_timeout,
            raise_on_error=False,
            raise_on_exception=True,
        ):
            if success:
                indexed += 1
                if indexed % 100_000 == 0:
                    LOGGER.info("%d documentos enviados ao Elasticsearch.", indexed)
            elif len(failures) < 20:
                failures.append(item)
    finally:
        set_refresh_interval(client, settings.elasticsearch_index, "1s")
        client.indices.refresh(index=settings.elasticsearch_index)

    if failures:
        raise RuntimeError(
            "Falhas durante a indexacao. Primeiros exemplos: "
            + json.dumps(failures, ensure_ascii=False)[:4000]
        )

    index_count = int(client.count(index=settings.elasticsearch_index)["count"])
    metadata = {
        "executado_em_utc": datetime.now(timezone.utc).isoformat(),
        "indice": settings.elasticsearch_index,
        "documentos_enviados": indexed,
        "documentos_no_indice": index_count,
        "somente_municipios_da_busca": settings.relevant_municipalities_only,
        "quantidade_municipios": len(municipalities),
        "limite_amostra": limit,
    }
    settings.indexing_metadata_path.parent.mkdir(parents=True, exist_ok=True)
    settings.indexing_metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LOGGER.info("Indexacao concluida: %d documentos no indice.", index_count)
    return metadata
