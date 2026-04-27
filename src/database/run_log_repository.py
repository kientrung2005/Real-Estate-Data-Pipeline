"""ETL run log tracking cho PostgreSQL."""

from __future__ import annotations

from typing import Optional

from src.database.postgres_connect import PostgreSQLConnect


def ensure_run(pg: PostgreSQLConnect, dag_id: str, run_type: str) -> str:
    """Tạo ETL run mới, trả về run_id."""
    pg.cursor.execute(
        """
        INSERT INTO etl_run_log (dag_id, run_type, status, started_at)
        VALUES (%s, %s, 'running', CURRENT_TIMESTAMP)
        RETURNING run_id
        """,
        (dag_id, run_type),
    )
    run_id = pg.cursor.fetchone()[0]
    return str(run_id)


def finish_run(
    pg: PostgreSQLConnect,
    run_id: str,
    status: str,
    records_read: int,
    records_loaded: int,
    records_quarantined: int,
    error_message: Optional[str] = None,
) -> None:
    """Cập nhật trạng thái ETL run."""
    pg.cursor.execute(
        """
        UPDATE etl_run_log
        SET status = %s,
            ended_at = CURRENT_TIMESTAMP,
            records_read = %s,
            records_loaded = %s,
            records_quarantined = %s,
            error_message = %s
        WHERE run_id = %s
        """,
        (status, records_read, records_loaded, records_quarantined, error_message, run_id),
    )
