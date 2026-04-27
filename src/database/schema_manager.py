"""Quản lý schema PostgreSQL — migration, introspection, seed."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from src.database.address_cleaner import (
    district_alias_names,
    ward_alias_names,
)
from src.database.postgres_connect import PostgreSQLConnect


PROJECT_ROOT = Path(__file__).resolve().parents[2]




def _table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
    return cursor.fetchone()[0] is not None


def _column_exists(cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    return cursor.fetchone() is not None


def _constraint_exists(cursor, table_name: str, constraint_name: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = 'public'
            AND table_name = %s
            AND constraint_name = %s
        """,
        (table_name, constraint_name),
    )
    return cursor.fetchone() is not None


def _run_sql_file(cursor, file_path: Path) -> None:
    sql_text = file_path.read_text(encoding="utf-8")
    cursor.execute(sql_text)


def _drop_incompatible_table(cursor, table_name: str, required_columns: set[str]) -> None:
    if not _table_exists(cursor, table_name):
        return
    missing_cols = [col for col in required_columns if not _column_exists(cursor, table_name, col)]
    if missing_cols:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")




def _ensure_existing_schema_compatibility(pg: PostgreSQLConnect) -> None:
    cursor = pg.cursor
    _drop_incompatible_table(
        cursor, "dim_ward",
        required_columns={"ward_id", "district_id", "ward_name", "city_name", "created_at"},
    )
    _drop_incompatible_table(
        cursor, "fact_property_listing",
        required_columns={"source_id", "district_id", "type_id", "date_key", "listing_url"},
    )


def _ensure_alias_schema(pg: PostgreSQLConnect) -> None:
    cursor = pg.cursor

    if _table_exists(cursor, "dim_district"):
        if not _column_exists(cursor, "dim_district", "alias_names"):
            cursor.execute("ALTER TABLE dim_district ADD COLUMN alias_names TEXT[] NOT NULL DEFAULT '{}'::text[]")
        # TODO: batch — dùng execute_values thay vì N câu UPDATE riêng lẻ khi dataset lớn
        cursor.execute("SELECT district_id, district_name, district_type FROM dim_district")
        for district_id, district_name, district_type in cursor.fetchall():
            aliases = district_alias_names(district_name, district_type)
            cursor.execute(
                "UPDATE dim_district SET alias_names = %s WHERE district_id = %s",
                (aliases, district_id),
            )

    if _table_exists(cursor, "dim_ward"):
        if not _column_exists(cursor, "dim_ward", "canonical_name"):
            cursor.execute("ALTER TABLE dim_ward ADD COLUMN canonical_name VARCHAR(120)")
        if not _column_exists(cursor, "dim_ward", "alias_names"):
            cursor.execute("ALTER TABLE dim_ward ADD COLUMN alias_names TEXT[] NOT NULL DEFAULT '{}'::text[]")
        cursor.execute(
            """
            UPDATE dim_ward
            SET canonical_name = COALESCE(NULLIF(canonical_name, ''), ward_name),
                alias_names = COALESCE(alias_names, '{}'::text[])
            """
        )
        cursor.execute("SELECT ward_id, ward_name, canonical_name FROM dim_ward")
        for ward_id, ward_name, canonical_name in cursor.fetchall():
            aliases = ward_alias_names(ward_name, canonical_name)
            cursor.execute(
                "UPDATE dim_ward SET alias_names = %s WHERE ward_id = %s",
                (aliases, ward_id),
            )
        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN canonical_name SET NOT NULL")
        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN alias_names SET DEFAULT '{}'::text[]")
        cursor.execute("ALTER TABLE dim_ward ALTER COLUMN alias_names SET NOT NULL")
        if _constraint_exists(cursor, "dim_ward", "uq_dim_ward"):
            cursor.execute("ALTER TABLE dim_ward DROP CONSTRAINT uq_dim_ward")
        if not _constraint_exists(cursor, "dim_ward", "uq_dim_ward_canonical"):
            cursor.execute(
                "ALTER TABLE dim_ward ADD CONSTRAINT uq_dim_ward_canonical UNIQUE (district_id, canonical_name)"
            )




def ensure_postgres_schema(pg: PostgreSQLConnect) -> None:
    """Khởi tạo / migrate schema PostgreSQL."""
    cursor = pg.cursor
    _ensure_existing_schema_compatibility(pg)

    sql_dir = PROJECT_ROOT / "sql"
    for sql_file in ["schema.sql", "seed.sql", "trigger.sql"]:
        file_path = sql_dir / sql_file
        if not file_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file SQL: {file_path}")
        if sql_file == "seed.sql":
            _ensure_alias_schema(pg)
        _run_sql_file(cursor, file_path)

    _ensure_alias_schema(pg)

    # ward_id không còn dùng trong fact table vì chất lượng trích xuất ward không ổn định.
    if _table_exists(cursor, "fact_property_listing") and _column_exists(cursor, "fact_property_listing", "ward_id"):
        cursor.execute("DROP INDEX IF EXISTS idx_fact_district_ward")
        cursor.execute("ALTER TABLE fact_property_listing DROP COLUMN ward_id")

    # Đảm bảo tất cả property types đều tồn tại (bao gồm cả types mới thêm)
    for type_name in ('khac', 'phong_tro', 'mat_bang', 'kho_xuong'):
        cursor.execute(
            "INSERT INTO dim_property_type (type_name) VALUES (%s) ON CONFLICT (type_name) DO NOTHING",
            (type_name,),
        )
