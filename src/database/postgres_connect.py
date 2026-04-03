"""Kết nối PostgreSQL theo kiểu class/context-manager."""

import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection

load_dotenv()


class PostgreSQLConnect:
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.config = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        self.connection: connection | None = None
        self.cursor = None

    @classmethod
    def from_env(cls) -> "PostgreSQLConnect":
        return cls(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "real_estate_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "admin123"),
        )

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("------Connected successfully to PostgreSQL-------")
            return self.connection, self.cursor
        except Exception as exc:
            raise Exception(f"------Failed to connect PostgreSQL: {exc}------") from exc

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("------POSTGRESQL: Close Connection-------")

    def __enter__(self) -> "PostgreSQLConnect":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.connection:
            if exc_type is None:
                self.connection.commit()
            else:
                self.connection.rollback()
        self.close()


def get_postgres_connection():
    connector = PostgreSQLConnect.from_env()
    return connector.connect()


def test_postgres_connection() -> bool:
    with PostgreSQLConnect.from_env() as connector:
        connector.cursor.execute("SELECT 1")
        _ = connector.cursor.fetchone()
    return True
