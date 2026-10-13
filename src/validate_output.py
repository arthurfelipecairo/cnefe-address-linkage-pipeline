"""Validacao estrutural do resultado exigido pelo enunciado."""

from collections import Counter
import csv
import json
from pathlib import Path
from typing import Any

from src.linkage import STATUS_AMBIGUOUS, STATUS_FOUND, STATUS_NOT_FOUND
from src.spark_utils import SEARCH_COLUMNS, read_search_records


VALID_STATUSES = {STATUS_FOUND, STATUS_AMBIGUOUS, STATUS_NOT_FOUND}


def read_result(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream, delimiter=";"))


def validate_output(
    search_path: str,
    result_path: Path,
) -> dict[str, Any]:
    originals = read_search_records(search_path)
    results = read_result(result_path)
    errors: list[str] = []

    if len(results) != len(originals):
        errors.append(
            f"Quantidade de linhas diferente: entrada={len(originals)}, "
            f"saida={len(results)}."
        )

    for position, (original, result) in enumerate(
        zip(originals, results),
        start=1,
    ):
        for column in SEARCH_COLUMNS:
            if (original.get(column) or "") != (result.get(column) or ""):
                errors.append(
                    f"Linha {position}: coluna original alterada ({column})."
                )

        status = result.get("status_vinculacao")
        sector = result.get("setor_censitario_encontrado") or ""
        if status not in VALID_STATUSES:
            errors.append(f"Linha {position}: status invalido ({status}).")
        if status == STATUS_FOUND and not sector:
            errors.append(
                f"Linha {position}: ENCONTRADO sem setor censitario."
            )
        if status in {STATUS_AMBIGUOUS, STATUS_NOT_FOUND} and sector:
            errors.append(
                f"Linha {position}: setor preenchido para status {status}."
            )

    report = {
        "valido": not errors,
        "linhas_entrada": len(originals),
        "linhas_saida": len(results),
        "status": dict(Counter(row.get("status_vinculacao") for row in results)),
        "erros": errors,
    }

    if errors:
        raise ValueError("Falha na validacao:\n- " + "\n- ".join(errors[:20]))
    return report


def write_validation(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
