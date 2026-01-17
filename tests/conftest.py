import os
import uuid
import pytest
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

@pytest.fixture(scope="session")
def db_url():
    url = os.getenv("DATABASE_URL")
    assert url, "DATABASE_URL missing"
    return url

@pytest.fixture()
def test_symbol():
    return "ZZTEST_" + uuid.uuid4().hex[:8]
