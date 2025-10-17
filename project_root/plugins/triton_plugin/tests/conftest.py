import os
import pytest
import httpx

@pytest.fixture(scope="session")
def triton_client():
    with httpx.Client(base_url=f"http://triton_plugin:{os.environ['TRITON_HTTP_PORT']}") as client:
        yield client