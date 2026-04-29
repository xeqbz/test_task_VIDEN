from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    """
    Load pipeline settings from YAML config.

    Args:
        config_path: Optional path to the YAML config file.
            If omitted, config/settings.yaml will be used by default.

    Returns:
        Parsed settings as a dictionary.
    """
    path = (
        Path(config_path) if config_path else PROJECT_ROOT / "config" / "settings.yaml"
    )

    if not path.exists():
        raise FileNotFoundError(f"Config file not found at {path}")

    with path.open("r", encoding="utf-8") as file:
        settings = yaml.safe_load(file)

    if not isinstance(settings, dict):
        raise ValueError("Config file must contain a YAML object")

    return settings
