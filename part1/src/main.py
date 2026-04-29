import logging
from pathlib import Path

from src.config import PROJECT_ROOT, load_settings
from src.extract import extract_records, read_json
from src.load import write_jsonl, load_to_bigquery_stub
from src.logging_config import setup_logging
from src.transform import normalize_records


logger = logging.getLogger(__name__)


def resolve_path(path_from_config: str) -> Path:
    """
    Resolve path relative to part1 project root.
    
    Example:
        ./input/file.json
    """
    return (PROJECT_ROOT / path_from_config).resolve()


def main() -> None:
    setup_logging()

    settings = load_settings()

    input_path = resolve_path(settings["input"]["fb_ads_path"])
    output_path = resolve_path(settings["output"]["normalized_jsonl_path"])

    logger.info(f"Reading data from {input_path}")

    payload = read_json(input_path)
    records = extract_records(payload)

    logger.info(f"Extracted {len(records)} records")

    normalized_rows = normalize_records(records, settings)

    logger.info(f"Normalized {len(normalized_rows)} records")

    write_jsonl(normalized_rows, output_path)

    #load_to_bigquery_stub(normalized_rows, settings) Example of how loading function could be called.

    if normalized_rows:
        logger.info(f"Sample normalized record: {normalized_rows[0]}")


if __name__ == "__main__":
    main()
