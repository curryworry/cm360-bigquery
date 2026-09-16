from src.dynamic_ingestion import _select_key_columns


def test_cm360_value_variables_are_key_dimensions() -> None:
    key_columns, strategy = _select_key_columns(
        [
            "date",
            "activity",
            "activity_id",
            "ord_value",
            "total_revenue",
            "total_conversions",
            "transaction_count",
        ]
    )

    assert strategy == "dimension_columns"
    assert "ord_value" in key_columns
    assert "total_revenue" not in key_columns
    assert "total_conversions" not in key_columns
    assert "transaction_count" not in key_columns
