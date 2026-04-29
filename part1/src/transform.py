from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any


logger = logging.getLogger(__name__)


def to_int(value: Any, default: int = 0) -> int:
    if value is None or value == "":
        return default

    try:
        return int(float(value))
    except (ValueError, TypeError):
        logger.warning("Failed to cast value to int: %r", value)
        return default


def to_decimal(value: Any, default: Decimal | None = Decimal("0")) -> Decimal | None:
    if value is None or value == "":
        return default

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        logger.warning("Failed to cast value to Decimal: %r", value)
        return default


def decimal_to_output(value: Decimal | None) -> float | None:
    if value is None:
        return None

    return float(value)


def array_to_metric_map(
    items: list[dict[str, Any]] | None,
    value_type: str,
) -> dict[str, int | Decimal | None]:
    """
    Convert arrays into a dictionary.

    Example:
        [
            {"action_type": "purchase", "value": "2"},
            {"action_type": "add_to_cart", "value": "8"}
        ]

    Result:
        {
            "purchase": 2,
            "add_to_cart": 8
        }
    """
    if not items:
        return {}

    result: dict[str, int | Decimal | None] = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        action_type = item.get("action_type")
        raw_value = item.get("value")

        if not action_type:
            continue

        if value_type == "int":
            value = to_int(raw_value)
        elif value_type == "decimal":
            value = to_decimal(raw_value)
        else:
            raise ValueError(f"Unsupported value_type: {value_type}")

        result[action_type] = value

    return result


def get_default_value(default_from_config: Any, value_type: str) -> int | Decimal | None:
    if default_from_config is None:
        return None

    if value_type == "int":
        return to_int(default_from_config)

    if value_type == "decimal":
        return to_decimal(default_from_config)

    raise ValueError(f"Unsupported value_type: {value_type}")


def normalize_nested_metric_group(
    record: dict[str, Any],
    array_name: str,
    metric_config: dict[str, Any],
) -> dict[str, Any]:
    """
    Normalize one nested metric array into flat columns.

    Example:
        array_name = "cost_per_action_type"

        source:
            [{"action_type": "purchase", "value": "36.07"}]

        output:
            {"cost_per_purchase": 36.07}
    """
    value_type = metric_config["value_type"]
    mappings = metric_config.get("mappings", {})
    default_value = get_default_value(
        metric_config.get("default"),
        value_type,
    )

    source_map = array_to_metric_map(
        record.get(array_name),
        value_type=value_type,
    )

    result: dict[str, Any] = {}

    for source_action_type, output_column in mappings.items():
        value = source_map.get(source_action_type, default_value)

        if value_type == "decimal":
            result[output_column] = decimal_to_output(value)
        else:
            result[output_column] = value

    return result


def calculate_roas(purchase_revenue: Decimal | None, spend: Decimal | None) -> float | None:
    if purchase_revenue is None or spend is None or spend <= 0:
        return None

    return float(purchase_revenue / spend)


def normalize_record(record: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize one record into flat reporting row.

    Output grain:
        one row per (ad_id, date_start, age, gender)
    """
    fb_settings = settings["facebook_ads"]
    flat_fields = fb_settings["flat_fields"]
    nested_metrics = fb_settings["nested_metrics"]

    row: dict[str, Any] = {}

    for field in flat_fields.get("string_fields", []):
        row[field] = record.get(field)

    for field in flat_fields.get("int_fields", []):
        row[field] = to_int(record.get(field))

    for field in flat_fields.get("decimal_fields", []):
        row[field] = decimal_to_output(to_decimal(record.get(field)))

    for array_name, metric_config in nested_metrics.items():
        row.update(
            normalize_nested_metric_group(
                record=record,
                array_name=array_name,
                metric_config=metric_config,
            )
        )

    purchase_action = fb_settings.get("purchase_action", "purchase")

    action_values_map = array_to_metric_map(
        record.get("action_values"),
        value_type="decimal",
    )

    purchase_revenue = action_values_map.get(purchase_action, Decimal("0"))
    spend = to_decimal(record.get("spend"))

    if not isinstance(purchase_revenue, Decimal):
        purchase_revenue = to_decimal(purchase_revenue)

    row["purchase_revenue"] = decimal_to_output(purchase_revenue)
    row["roas"] = calculate_roas(
        purchase_revenue=purchase_revenue,
        spend=spend,
    )

    return row


def normalize_records(
    records: list[dict[str, Any]],
    settings: dict[str, Any],
) -> list[dict[str, Any]]:
    return [
        normalize_record(record, settings)
        for record in records
    ]