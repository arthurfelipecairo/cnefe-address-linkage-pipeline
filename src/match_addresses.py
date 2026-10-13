"""Recuperacao de candidatos, vinculacao e gravacao das entregas."""

import csv
import json
import logging
from pathlib import Path
from typing import Any, Iterable

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.linkage import (
    STATUS_NOT_FOUND,
    build_search_strategies,
    decide_match,
    rank_candidates,
)
from src.metrics import calculate_linkage_metrics, write_metrics
from src.settings import Settings
from src.spark_utils import SEARCH_COLUMNS, create_normalized_search_dataframe
from src.validate_output import validate_output, write_validation


LOGGER = logging.getLogger(__name__)

RESULT_EXTRA_COLUMNS = (
    "setor_censitario_encontrado",
    "status_vinculacao",
    "metodo_vinculacao",
    "score_vinculacao",
    "margem_score",
    "quantidade_candidatos",
    "id_endereco_candidato",
)

AUDIT_COLUMNS = (
    "consulta_id",
    "status_vinculacao",
    "metodo_vinculacao",
    "rank_candidato",
    "id_endereco_candidato",
    "setor_censitario_candidato",
    "logradouro_candidato",
    "numero_candidato",
    "localidade_candidato",
    "cep_candidato",
    "score_vinculacao",
    "elasticsearch_score",
    "evidencias",
)


def _search_candidates(
    client: Elasticsearch,
    settings: Settings,
    query: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    candidates_by_id: dict[str, dict[str, Any]] = {}
    for method, elasticsearch_query in build_search_strategies(query):
        response = client.search(
            index=settings.elasticsearch_index,
            query=elasticsearch_query,
            size=settings.match_candidate_size,
            collapse={"field": "setor_censitario"},
            track_total_hits=True,
        )
        hits = list(response["hits"]["hits"])
        for hit in hits:
            candidate_id = str(hit.get("_id"))
            if candidate_id not in candidates_by_id:
                candidate = dict(hit)
                candidate["_retrieval_method"] = method
                candidates_by_id[candidate_id] = candidate
    return (
        "MULTIESTRATEGIA" if candidates_by_id else "SEM_CANDIDATO",
        list(candidates_by_id.values()),
    )


def _write_csv(
    rows: list[dict[str, Any]],
    path: Path,
    columns: Iterable[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=columns,
            delimiter=";",
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_audit_parquet(
    spark: SparkSession,
    rows: list[dict[str, Any]],
    path: Path,
) -> None:
    schema = StructType(
        [
            StructField("consulta_id", StringType(), False),
            StructField("status_vinculacao", StringType(), False),
            StructField("metodo_vinculacao", StringType(), False),
            StructField("rank_candidato", IntegerType(), False),
            StructField("id_endereco_candidato", StringType(), True),
            StructField("setor_censitario_candidato", StringType(), True),
            StructField("logradouro_candidato", StringType(), True),
            StructField("numero_candidato", StringType(), True),
            StructField("localidade_candidato", StringType(), True),
            StructField("cep_candidato", StringType(), True),
            StructField("score_vinculacao", DoubleType(), False),
            StructField("elasticsearch_score", DoubleType(), False),
            StructField("evidencias", StringType(), False),
        ]
    )
    values = [tuple(row.get(column) for column in AUDIT_COLUMNS) for row in rows]
    spark.createDataFrame(values, schema=schema).write.mode("overwrite").parquet(
        str(path)
    )


def match_addresses(
    spark: SparkSession,
    client: Elasticsearch,
    settings: Settings,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Vincula todas as consultas e grava resultado, auditoria e metricas."""
    normalized, originals = create_normalized_search_dataframe(
        spark,
        settings.search_path,
    )
    normalized_by_id = {
        row["consulta_id"]: row.asDict(recursive=True)
        for row in normalized.collect()
    }

    results: list[dict[str, Any]] = []
    audit: list[dict[str, Any]] = []

    for position, original in enumerate(originals, start=1):
        query_id = str(position)
        query = normalized_by_id[query_id]
        if build_search_strategies(query):
            method, hits = _search_candidates(client, settings, query)
        else:
            method, hits = "DADOS_INSUFICIENTES", []

        ranked = rank_candidates(query, hits)
        decision = decide_match(
            ranked,
            min_score=settings.match_min_score,
            min_margin=settings.match_min_margin,
        )
        best = decision["best_candidate"]
        if best is not None:
            method = best["metodo_recuperacao"]
        result = dict(original)
        result.update(
            {
                "setor_censitario_encontrado": decision["setor_censitario"] or "",
                "status_vinculacao": decision["status"],
                "metodo_vinculacao": method,
                "score_vinculacao": decision["best_score"],
                "margem_score": decision["margin"],
                "quantidade_candidatos": len(ranked),
                "id_endereco_candidato": (
                    best["id_endereco"] if best is not None else ""
                ),
            }
        )
        results.append(result)

        if ranked:
            for rank, candidate in enumerate(ranked, start=1):
                audit.append(
                    {
                        "consulta_id": query_id,
                        "status_vinculacao": decision["status"],
                        "metodo_vinculacao": candidate["metodo_recuperacao"],
                        "rank_candidato": rank,
                        "id_endereco_candidato": str(candidate["id_endereco"]),
                        "setor_censitario_candidato": candidate["source"].get(
                            "setor_censitario"
                        ),
                        "logradouro_candidato": candidate["source"].get(
                            "logradouro_original"
                        ),
                        "numero_candidato": candidate["source"].get(
                            "numero_original"
                        ),
                        "localidade_candidato": candidate["source"].get(
                            "localidade_original"
                        ),
                        "cep_candidato": candidate["source"].get(
                            "cep_original"
                        ),
                        "score_vinculacao": float(candidate["score_vinculacao"]),
                        "elasticsearch_score": float(
                            candidate["elasticsearch_score"]
                        ),
                        "evidencias": json.dumps(
                            candidate["evidencias"],
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    }
                )
        else:
            audit.append(
                {
                    "consulta_id": query_id,
                    "status_vinculacao": STATUS_NOT_FOUND,
                    "metodo_vinculacao": method,
                    "rank_candidato": 0,
                    "id_endereco_candidato": None,
                    "setor_censitario_candidato": None,
                    "logradouro_candidato": None,
                    "numero_candidato": None,
                    "localidade_candidato": None,
                    "cep_candidato": None,
                    "score_vinculacao": 0.0,
                    "elasticsearch_score": 0.0,
                    "evidencias": "{}",
                }
            )

        LOGGER.info(
            "Consulta %d/%d: %s (%s, score %.2f).",
            position,
            len(originals),
            decision["status"],
            method,
            decision["best_score"],
        )

    _write_csv(
        results,
        settings.result_path,
        (*SEARCH_COLUMNS, *RESULT_EXTRA_COLUMNS),
    )
    _write_csv(audit, settings.audit_csv_path, AUDIT_COLUMNS)
    _write_audit_parquet(spark, audit, settings.audit_parquet_path)

    metrics = calculate_linkage_metrics(results)
    write_metrics(metrics, settings.metrics_path)
    validation = validate_output(settings.search_path, settings.result_path)
    write_validation(validation, settings.validation_path)
    LOGGER.info("Resultado validado e salvo em %s", settings.result_path)
    return results, metrics
