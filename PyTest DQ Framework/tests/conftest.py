import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary


# ---------------------------------------------------------------------------
# CLI Options Registration
# ---------------------------------------------------------------------------

def pytest_addoption(parser):
    """Register custom command-line options for the test suite."""
    parser.addoption(
        "--db_host",
        action="store",
        default="localhost",
        help="Hostname or IP address of the PostgreSQL database server. "
             "Default: localhost",
    )
    parser.addoption(
        "--db_name",
        action="store",
        default="mydatabase",
        help="Name of the PostgreSQL database to connect to. "
             "Default: mydatabase",
    )
    parser.addoption(
        "--db_port",
        action="store",
        default="5434",
        help="Port number for the PostgreSQL database connection. "
             "Default: 5434",
    )
    parser.addoption(
        "--db_user",
        action="store",
        default=None,
        help="Username for PostgreSQL authentication. Required.",
    )
    parser.addoption(
        "--db_password",
        action="store",
        default=None,
        help="Password for PostgreSQL authentication. Required.",
    )


# ---------------------------------------------------------------------------
# Pre-flight Validation
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """
    Validate that all required CLI options are provided before test execution.

    Halts pytest immediately with a descriptive error message if any
    required option is missing.
    """
    required_options = ["--db_user", "--db_password"]
    missing = []

    for option in required_options:
        try:
            value = config.getoption(option)
            if value is None:
                missing.append(option)
        except ValueError:
            missing.append(option)

    if missing:
        pytest.exit(
            f"\n[conftest] Required command-line options are missing: "
            f"{', '.join(missing)}\n"
            f"Please re-run pytest with the missing options, e.g.:\n"
            f"  pytest tests -m 'parquet_data' "
            f"--db_user=<user> --db_password=<password>\n",
            returncode=1,
        )


# ---------------------------------------------------------------------------
# Session-Scoped Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def db_connection(request):
    """
    Establish and yield a PostgreSQL database connection for the test session.

    The connection is opened once at the start of the session and closed
    automatically when the session ends.

    Yields:
        PostgresConnectorContextManager: An active database connector instance.
    """
    db_host = request.config.getoption("--db_host")
    db_name = request.config.getoption("--db_name")
    db_port = request.config.getoption("--db_port")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")

    try:
        with PostgresConnectorContextManager(
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_name=db_name,
            db_port=db_port,
        ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(
            f"[conftest] Failed to initialize PostgresConnectorContextManager.\n"
            f"Error: {e}"
        )


@pytest.fixture(scope="session")
def parquet_reader():
    """
    Provide a ParquetReader instance for the test session.

    Yields:
        ParquetReader: An instance ready to read Parquet files.
    """
    reader = None
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(
            f"[conftest] Failed to initialize ParquetReader.\n"
            f"Error: {e}"
        )
    finally:
        if reader is not None:
            del reader


@pytest.fixture(scope="session")
def data_quality_library():
    """
    Provide a DataQualityLibrary instance for the test session.

    Yields:
        DataQualityLibrary: An instance with all data quality check methods.
    """
    dq_library = None
    try:
        dq_library = DataQualityLibrary()
        yield dq_library
    except Exception as e:
        pytest.fail(
            f"[conftest] Failed to initialize DataQualityLibrary.\n"
            f"Error: {e}"
        )
    finally:
        if dq_library is not None:
            del dq_library
