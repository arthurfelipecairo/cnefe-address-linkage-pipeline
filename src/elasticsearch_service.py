"""Criacao do cliente e gerenciamento do indice Elasticsearch."""

import json
import logging
import time
from pathlib import Path

from elasticsearch import Elasticsearch

from src.settings import Settings


LOGGER = logging.getLogger(__name__)


def _read_password(settings: Settings) -> str | None:
    if settings.elasticsearch_password_file:
        return settings.elasticsearch_password_file.read_text(
            encoding="utf-8"
        ).strip()
    return settings.elasticsearch_password


def create_elasticsearch_client(settings: Settings) -> Elasticsearch:
    options: dict[str, object] = {
        "request_timeout": settings.elasticsearch_request_timeout,
        "retry_on_timeout": True,
        "max_retries": 3,
    }

    if settings.elasticsearch_api_key:
        options["api_key"] = settings.elasticsearch_api_key
    elif settings.elasticsearch_username:
        password = _read_password(settings)
        if not password:
            raise ValueError(
                "ELASTICSEARCH_USERNAME foi informado sem uma senha."
            )
        options["basic_auth"] = (
            settings.elasticsearch_username,
            password,
        )

    if settings.elasticsearch_ca_certs:
        options["ca_certs"] = settings.elasticsearch_ca_certs
    options["verify_certs"] = settings.elasticsearch_verify_certs

    return Elasticsearch(settings.elasticsearch_url, **options)


def wait_for_elasticsearch(
    client: Elasticsearch,
    timeout_seconds: int = 120,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            if client.ping():
                client.cluster.health(wait_for_status="yellow", timeout="5s")
                return
        except Exception as error:  # pragma: no cover - depende da rede
            last_error = error
        time.sleep(2)

    message = "Elasticsearch nao ficou disponivel dentro do tempo limite."
    if last_error:
        message += f" Ultimo erro: {last_error}"
    raise RuntimeError(message)


def load_mapping(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        mapping = json.load(stream)

    if "settings" not in mapping or "mappings" not in mapping:
        raise ValueError(
            "O mapeamento precisa conter 'settings' e 'mappings'."
        )
    return mapping


def ensure_index(
    client: Elasticsearch,
    index_name: str,
    mapping_path: Path,
    recreate: bool,
) -> None:
    exists = bool(client.indices.exists(index=index_name))

    if exists and recreate:
        LOGGER.info("Removendo indice anterior: %s", index_name)
        client.indices.delete(index=index_name)
        exists = False

    if not exists:
        mapping = load_mapping(mapping_path)
        LOGGER.info("Criando indice: %s", index_name)
        client.indices.create(
            index=index_name,
            settings=mapping["settings"],
            mappings=mapping["mappings"],
        )


def set_refresh_interval(
    client: Elasticsearch,
    index_name: str,
    interval: str,
) -> None:
    client.indices.put_settings(
        index=index_name,
        settings={"index": {"refresh_interval": interval}},
    )
