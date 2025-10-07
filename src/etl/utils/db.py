
from __future__ import annotations
import os
import oracledb
import pyodbc

def oracle_conn():
    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASS"),
        dsn=os.getenv("ORACLE_DSN"),
    )

def mssql_conn():
    cn = pyodbc.connect(
        (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={os.getenv('MSSQL_SERVER')};"
            f"DATABASE={os.getenv('MSSQL_DB')};"
            f"UID={os.getenv('MSSQL_USER')};PWD={os.getenv('MSSQL_PASS')};"
            "TrustServerCertificate=Yes"
        )
    )
    cn.autocommit = False
    return cn
