import os
from dataclasses import dataclass


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    mssql_host: str
    mssql_port: int
    mssql_database: str
    mssql_user: str
    mssql_password: str

    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            mssql_host=os.getenv("MSSQL_HOST", "mssql"),
            mssql_port=int(os.getenv("MSSQL_PORT", "1433")),
            mssql_database=os.getenv(
                "MSSQL_DATABASE",
                "Northwind_OLTP",
            ),
            mssql_user=os.getenv("MSSQL_USER", "sa"),
            mssql_password=required_env("MSSQL_SA_PASSWORD"),
            postgres_host=os.getenv(
                "POSTGRES_HOST",
                "postgres-staging",
            ),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_database=required_env("POSTGRES_DB"),
            postgres_user=required_env("POSTGRES_USER"),
            postgres_password=required_env("POSTGRES_PASSWORD"),
        )
