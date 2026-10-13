"""Metricas descritivas da vinculacao sem assumir um padrao-ouro."""

from collections import Counter
import json
from pathlib import Path
from statistics import mean, median
from typing import Any

from src.linkage import STATUS_AMBIGUOUS, STATUS_FOUND, STATUS_NOT_FOUND


def calculate_linkage_metrics(results: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(results)
    status_counts = Counter(result["status_vinculacao"] for result in results)
    method_counts = Counter(result["metodo_vinculacao"] for result in results)
    scores = [float(result["score_vinculacao"]) for result in results]
    margins = [float(result["margem_score"]) for result in results]

    def rate(value: int) -> float:
        return round(value / total, 4) if total else 0.0

    return {
        "observacao": (
            "Metricas descritivas sem padrao-ouro; nao representam precisao "
            "ou recall."
        ),
        "total_consultas": total,
        "status": {
            STATUS_FOUND: status_counts[STATUS_FOUND],
            STATUS_AMBIGUOUS: status_counts[STATUS_AMBIGUOUS],
            STATUS_NOT_FOUND: status_counts[STATUS_NOT_FOUND],
        },
        "taxa_vinculacao": rate(status_counts[STATUS_FOUND]),
        "taxa_ambiguidade": rate(status_counts[STATUS_AMBIGUOUS]),
        "taxa_nao_encontrado": rate(status_counts[STATUS_NOT_FOUND]),
        "metodos": dict(sorted(method_counts.items())),
        "scores": {
            "minimo": round(min(scores), 4) if scores else 0.0,
            "maximo": round(max(scores), 4) if scores else 0.0,
            "media": round(mean(scores), 4) if scores else 0.0,
            "mediana": round(median(scores), 4) if scores else 0.0,
        },
        "margens": {
            "minima": round(min(margins), 4) if margins else 0.0,
            "maxima": round(max(margins), 4) if margins else 0.0,
            "media": round(mean(margins), 4) if margins else 0.0,
            "mediana": round(median(margins), 4) if margins else 0.0,
        },
        "completude_entrada": {
            "com_numero": sum(bool(row.get("consulta_numero")) for row in results),
            "com_bairro": sum(bool(row.get("consulta_bairro")) for row in results),
            "com_cep": sum(bool(row.get("consulta_cep")) for row in results),
            "com_complemento": sum(
                bool(row.get("consulta_complemento")) for row in results
            ),
        },
    }


def write_metrics(metrics: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(metrics, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
