import os
import certifi
from dotenv import load_dotenv
from clickhouse_connect import get_client

# Load .env so environment variables are available
load_dotenv()

def ch_client():
    """
    Create and return a ClickHouse client using environment variables.
    Secure=True → forces TLS (HTTPS).
    verify=True + certifi → ensures SSL certificates are valid.
    """
    return get_client(
        host=os.getenv("CH_HOST"),
        port=int(os.getenv("CH_PORT", "8443")),
        username=os.getenv("CH_USER"),
        password=os.getenv("CH_PASSWORD"),
        database=os.getenv("CH_DATABASE", "crypto"),
        secure=True,
        verify=True,
        ca_cert=certifi.where(),
    )