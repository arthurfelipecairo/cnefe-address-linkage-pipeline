"""Regras puras para recuperar, pontuar e classificar candidatos."""

from difflib import SequenceMatcher
from typing import Any, Mapping


STATUS_FOUND = "ENCONTRADO"
STATUS_AMBIGUOUS = "AMBIGUO"
STATUS_NOT_FOUND = "NAO_ENCONTRADO"


def text_similarity(left: str | None, right: str | None) -> float | None:
    if not left or not right:
        return None
    if left == right:
        return 1.0
    return SequenceMatcher(None, left, right).ratio()


def build_search_strategies(
    query: Mapping[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    municipality = query.get("municipio_normalizado")
    street = query.get("logradouro_normalizado")
    number = query.get("numero_normalizado")
    postal_code = query.get("cep_normalizado")
    neighborhood = query.get("bairro_normalizado")
    complement = query.get("complemento_normalizado")

    if not municipality or not street:
        return []

    strategies: list[tuple[str, dict[str, Any]]] = []

    if postal_code:
        filters: list[dict[str, Any]] = [
            {"term": {"municipio_normalizado": municipality}},
            {"term": {"cep_normalizado": postal_code}},
            {"term": {"logradouro_normalizado.exact": street}},
        ]
        if number:
            filters.append({"term": {"numero_normalizado": number}})
        strategies.append(("EXATO_CEP", {"bool": {"filter": filters}}))

        if number:
            strategies.append(
                (
                    "CEP_NUMERO",
                    {
                        "bool": {
                            "filter": [
                                {
                                    "term": {
                                        "municipio_normalizado": municipality
                                    }
                                },
                                {"term": {"cep_normalizado": postal_code}},
                                {"term": {"numero_normalizado": number}},
                            ],
                            "should": [
                                {
                                    "match": {
                                        "logradouro_normalizado": {
                                            "query": street,
                                            "fuzziness": "AUTO",
                                            "boost": 4,
                                        }
                                    }
                                }
                            ],
                        }
                    },
                )
            )

    if number:
        strategies.append(
            (
                "EXATO_LOGRADOURO_NUMERO",
                {
                    "bool": {
                        "filter": [
                            {"term": {"municipio_normalizado": municipality}},
                            {"term": {"logradouro_normalizado.exact": street}},
                            {"term": {"numero_normalizado": number}},
                        ]
                    }
                },
            )
        )

    exact_filters: list[dict[str, Any]] = [
        {"term": {"municipio_normalizado": municipality}},
        {"term": {"logradouro_normalizado.exact": street}},
    ]
    if neighborhood:
        exact_filters.append(
            {"term": {"localidade_normalizada.exact": neighborhood}}
        )
    strategies.append(
        ("EXATO_LOGRADOURO", {"bool": {"filter": exact_filters}})
    )

    if postal_code:
        strategies.append(
            (
                "CEP_LOGRADOURO_APROXIMADO",
                {
                    "bool": {
                        "filter": [
                            {"term": {"municipio_normalizado": municipality}},
                            {"term": {"cep_normalizado": postal_code}},
                        ],
                        "must": [
                            {
                                "match": {
                                    "logradouro_normalizado": {
                                        "query": street,
                                        "fuzziness": "AUTO",
                                        "prefix_length": 1,
                                        "minimum_should_match": "50%",
                                    }
                                }
                            }
                        ],
                    }
                },
            )
        )

    should: list[dict[str, Any]] = []
    if postal_code:
        should.append(
            {"term": {"cep_normalizado": {"value": postal_code, "boost": 8}}}
        )
    if number:
        should.append(
            {"term": {"numero_normalizado": {"value": number, "boost": 6}}}
        )
    if neighborhood:
        should.append(
            {
                "match": {
                    "localidade_normalizada": {
                        "query": neighborhood,
                        "fuzziness": "AUTO",
                        "boost": 2,
                    }
                }
            }
        )
    if complement:
        should.append(
            {
                "match": {
                    "complemento_normalizado": {
                        "query": complement,
                        "boost": 1,
                    }
                }
            }
        )

    strategies.append(
        (
            "APROXIMADO",
            {
                "bool": {
                    "filter": [
                        {"term": {"municipio_normalizado": municipality}}
                    ],
                    "must": [
                        {
                            "match": {
                                "logradouro_normalizado": {
                                    "query": street,
                                    "fuzziness": "AUTO",
                                    "prefix_length": 1,
                                    "minimum_should_match": "50%",
                                }
                            }
                        }
                    ],
                    "should": should,
                }
            },
        )
    )

    return strategies


def score_candidate(
    query: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> tuple[float, dict[str, Any]]:
    if query.get("municipio_normalizado") != candidate.get(
        "municipio_normalizado"
    ):
        return 0.0, {"municipio_igual": False}

    components: list[tuple[str, float, float | None]] = [
        (
            "logradouro_similaridade",
            45.0,
            text_similarity(
                query.get("logradouro_normalizado"),
                candidate.get("logradouro_normalizado"),
            ),
        ),
        (
            "numero_igual",
            25.0,
            (
                1.0
                if query.get("numero_normalizado")
                == candidate.get("numero_normalizado")
                else 0.0
            )
            if query.get("numero_normalizado")
            else None,
        ),
        (
            "cep_igual",
            20.0,
            (
                1.0
                if query.get("cep_normalizado")
                == candidate.get("cep_normalizado")
                else 0.0
            )
            if query.get("cep_normalizado")
            else None,
        ),
        (
            "bairro_similaridade",
            7.0,
            text_similarity(
                query.get("bairro_normalizado"),
                candidate.get("localidade_normalizada"),
            ),
        ),
        (
            "complemento_similaridade",
            3.0,
            text_similarity(
                query.get("complemento_normalizado"),
                candidate.get("complemento_normalizado"),
            ),
        ),
    ]

    available_weight = sum(weight for _, weight, value in components if value is not None)
    weighted_score = sum(
        weight * value
        for _, weight, value in components
        if value is not None
    )
    score = 100.0 * weighted_score / available_weight if available_weight else 0.0

    evidence: dict[str, Any] = {"municipio_igual": True}
    evidence.update(
        {
            name: round(value, 4) if value is not None else None
            for name, _, value in components
        }
    )
    return round(score, 4), evidence


def rank_candidates(
    query: Mapping[str, Any],
    hits: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []

    for hit in hits:
        source = dict(hit.get("_source", {}))
        score, evidence = score_candidate(query, source)
        ranked.append(
            {
                "id_endereco": hit.get("_id") or source.get("id_endereco"),
                "elasticsearch_score": float(hit.get("_score") or 0.0),
                "score_vinculacao": score,
                "evidencias": evidence,
                "metodo_recuperacao": hit.get(
                    "_retrieval_method",
                    "NAO_INFORMADO",
                ),
                "source": source,
            }
        )

    ranked.sort(
        key=lambda item: (
            item["score_vinculacao"],
            item["elasticsearch_score"],
        ),
        reverse=True,
    )
    return ranked


def decide_match(
    ranked_candidates: list[dict[str, Any]],
    min_score: float,
    min_margin: float,
) -> dict[str, Any]:
    candidates_with_sector = [
        candidate
        for candidate in ranked_candidates
        if candidate["source"].get("setor_censitario")
    ]

    if not candidates_with_sector:
        return {
            "status": STATUS_NOT_FOUND,
            "setor_censitario": None,
            "best_candidate": None,
            "best_score": 0.0,
            "second_score": 0.0,
            "margin": 0.0,
        }

    best_by_sector: dict[str, dict[str, Any]] = {}
    for candidate in candidates_with_sector:
        sector = candidate["source"]["setor_censitario"]
        current = best_by_sector.get(sector)
        if current is None or candidate["score_vinculacao"] > current[
            "score_vinculacao"
        ]:
            best_by_sector[sector] = candidate

    sector_candidates = sorted(
        best_by_sector.values(),
        key=lambda item: item["score_vinculacao"],
        reverse=True,
    )
    best = sector_candidates[0]
    best_score = float(best["score_vinculacao"])
    second_score = (
        float(sector_candidates[1]["score_vinculacao"])
        if len(sector_candidates) > 1
        else 0.0
    )
    margin = best_score - second_score

    if best_score < min_score:
        status = STATUS_NOT_FOUND
    elif len(sector_candidates) > 1 and margin < min_margin:
        status = STATUS_AMBIGUOUS
    else:
        status = STATUS_FOUND

    return {
        "status": status,
        "setor_censitario": (
            best["source"]["setor_censitario"]
            if status == STATUS_FOUND
            else None
        ),
        "best_candidate": best,
        "best_score": round(best_score, 4),
        "second_score": round(second_score, 4),
        "margin": round(margin, 4),
    }
