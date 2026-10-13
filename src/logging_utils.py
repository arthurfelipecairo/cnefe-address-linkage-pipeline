"""Configuracao de logs para console e arquivo."""

import logging
from pathlib import Path


def configure_logging(output_dir: Path) -> None:
    log_dir = output_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                log_dir / "pipeline.log",
                encoding="utf-8",
            ),
        ],
        force=True,
    )
    # Evita uma linha por requisicao HTTP durante centenas de consultas. As
    # decisoes da pipeline continuam registradas no log de aplicacao.
    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)
