"""Configuracao central da pipeline por variaveis de ambiente."""

from dataclasses import dataclass
import os
from pathlib import Path


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "sim", "on"}


def _as_int(value: str | None, default: int) -> int:
    return int(value) if value not in (None, "") else default


def _as_float(value: str | None, default: float) -> float:
    return float(value) if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    cnefe_path: str
    search_path: str
    output_dir: Path
    elasticsearch_url: str
    elasticsearch_index: str
    elasticsearch_mapping_path: Path
    elasticsearch_username: str | None
    elasticsearch_password: str | None
    elasticsearch_password_file: Path | None
    elasticsearch_api_key: str | None
    elasticsearch_ca_certs: str | None
    elasticsearch_verify_certs: bool
    elasticsearch_request_timeout: int
    recreate_index: bool
    relevant_municipalities_only: bool
    index_batch_size: int
    match_candidate_size: int
    match_min_score: float
    match_min_margin: float
    spark_master: str
    spark_driver_memory: str
    spark_stream_partitions: int

    @classmethod
    def from_env(cls) -> "Settings":
        password_file_value = os.getenv("ELASTICSEARCH_PASSWORD_FILE")

        return cls(
            cnefe_path=os.getenv("CNEFE_PATH", "/app/raw/29_BA.csv"),
            search_path=os.getenv(
                "SEARCH_PATH",
                "/app/raw/2026_teste_tecnico_relinke_cidacs(in).csv",
            ),
            output_dir=Path(os.getenv("OUTPUT_DIR", "/app/output")),
            elasticsearch_url=os.getenv(
                "ELASTICSEARCH_URL",
                "http://elasticsearch:9200",
            ),
            elasticsearch_index=os.getenv(
                "ELASTICSEARCH_INDEX",
                "cnefe-enderecos-v1",
            ),
            elasticsearch_mapping_path=Path(
                os.getenv(
                    "ELASTICSEARCH_MAPPING_PATH",
                    "/app/elasticsearch/index_cnefe.json",
                )
            ),
            elasticsearch_username=os.getenv("ELASTICSEARCH_USERNAME"),
            elasticsearch_password=os.getenv("ELASTICSEARCH_PASSWORD"),
            elasticsearch_password_file=(
                Path(password_file_value) if password_file_value else None
            ),
            elasticsearch_api_key=os.getenv("ELASTICSEARCH_API_KEY"),
            elasticsearch_ca_certs=os.getenv("ELASTICSEARCH_CA_CERTS"),
            elasticsearch_verify_certs=_as_bool(
                os.getenv("ELASTICSEARCH_VERIFY_CERTS"),
                True,
            ),
            elasticsearch_request_timeout=_as_int(
                os.getenv("ELASTICSEARCH_REQUEST_TIMEOUT"),
                120,
            ),
            recreate_index=_as_bool(os.getenv("RECREATE_INDEX"), True),
            relevant_municipalities_only=_as_bool(
                os.getenv("INDEX_RELEVANT_MUNICIPALITIES_ONLY"),
                True,
            ),
            index_batch_size=_as_int(os.getenv("INDEX_BATCH_SIZE"), 1000),
            match_candidate_size=_as_int(
                os.getenv("MATCH_CANDIDATE_SIZE"),
                20,
            ),
            match_min_score=_as_float(os.getenv("MATCH_MIN_SCORE"), 70.0),
            match_min_margin=_as_float(os.getenv("MATCH_MIN_MARGIN"), 2.0),
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            spark_driver_memory=os.getenv("SPARK_DRIVER_MEMORY", "2g"),
            spark_stream_partitions=_as_int(
                os.getenv("SPARK_STREAM_PARTITIONS"),
                128,
            ),
        )

    @property
    def result_path(self) -> Path:
        return self.output_dir / "resultado.csv"

    @property
    def audit_csv_path(self) -> Path:
        return self.output_dir / "auditoria.csv"

    @property
    def audit_parquet_path(self) -> Path:
        return self.output_dir / "auditoria.parquet"

    @property
    def metrics_path(self) -> Path:
        return self.output_dir / "metricas.json"

    @property
    def validation_path(self) -> Path:
        return self.output_dir / "validacao.json"

    @property
    def indexing_metadata_path(self) -> Path:
        return self.output_dir / "indexacao.json"
