"""Kết nối MongoDB theo kiểu class/context-manager."""

import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import ConnectionFailure

load_dotenv()


class MongoDBConnect:
    def __init__(self, mongo_uri: str, database: str):
        self.mongo_uri = mongo_uri
        self.database = database
        self.client: MongoClient | None = None
        self.db: Database | None = None

    @classmethod
    def from_env(cls) -> "MongoDBConnect":
        host = os.getenv("MONGO_HOST", "localhost")
        port = os.getenv("MONGO_PORT", "27017")
        user = os.getenv("MONGO_USER", "admin")
        password = os.getenv("MONGO_PASS", "password123")
        database = os.getenv("MONGO_DB_NAME", "real_estate_raw")
        auth_source = os.getenv("MONGO_AUTH_SOURCE", "admin")
        mongo_uri = f"mongodb://{user}:{password}@{host}:{port}/?authSource={auth_source}"
        return cls(mongo_uri=mongo_uri, database=database)

    def connect(self) -> Database:
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db = self.client[self.database]
            print(f"------Connected successfully to MongoDB: {self.database}-------")
            return self.db
        except ConnectionFailure as exc:
            raise Exception(f"------Failed to connect MongoDB: {self.database}------") from exc

    def close(self) -> None:
        if self.client:
            self.client.close()
            print("------MONGODB: Close Connection-------")

    def __enter__(self) -> "MongoDBConnect":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def test_mongo_connection() -> bool:
    connector = MongoDBConnect.from_env()
    try:
        connector.connect()
        return True
    finally:
        connector.close()
