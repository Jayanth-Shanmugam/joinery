import json

from sqlalchemy import create_engine


class Database:
    def __init__(
        self,
        name: str | None,
        backend: str,
        conn_str: str | None,
        dialect: str | None,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
    ):
        self.name = name
        self.backend = backend
        self.dialect = dialect
        self.host = host
        self.port = port

    def load_credentials(self):
        pass
