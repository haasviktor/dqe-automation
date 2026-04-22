import psycopg2
import pandas as pd


class PostgresConnectorContextManager:
    """
    A context manager for managing PostgreSQL database connections.

    Provides a safe mechanism for opening and closing database connections,
    and exposes a method for executing SQL queries and returning results
    as a pandas DataFrame.

    Usage:
        with PostgresConnectorContextManager(
            db_user="user",
            db_password="password",
            db_host="localhost",
            db_name="mydb",
            db_port="5432"
        ) as connector:
            df = connector.get_data_sql("SELECT * FROM my_table")
    """

    def __init__(
        self,
        db_user: str,
        db_password: str,
        db_host: str,
        db_name: str,
        db_port: str,
    ) -> None:
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name
        self.db_port = db_port
        self.connection = None

    def __enter__(self) -> "PostgresConnectorContextManager":
        try:
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
            print(
                f"[PostgresConnector] Connected to '{self.db_name}' "
                f"at {self.db_host}:{self.db_port}"
            )
            return self
        except psycopg2.OperationalError as e:
            raise ConnectionError(
                f"[PostgresConnector] Failed to connect to PostgreSQL database "
                f"'{self.db_name}' at {self.db_host}:{self.db_port}.\n"
                f"Error: {e}"
            )

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self.connection:
            self.connection.close()
            print("[PostgresConnector] Connection closed.")
        # Returning False re-raises any exception that occurred in the with block
        return False

    def get_data_sql(self, query: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a pandas DataFrame.

        Args:
            query (str): The SQL query string to execute.

        Returns:
            pd.DataFrame: Query results as a DataFrame.

        Raises:
            RuntimeError: If the query execution fails.
        """
        if not self.connection:
            raise ConnectionError(
                "[PostgresConnector] No active database connection."
            )
        try:
            df = pd.read_sql_query(query, self.connection)
            print(
                f"[PostgresConnector] Query executed successfully. "
                f"Rows returned: {len(df)}"
            )
            return df
        except Exception as e:
            raise RuntimeError(
                f"[PostgresConnector] Failed to execute query.\n"
                f"Query: {query}\n"
                f"Error: {e}"
            )
