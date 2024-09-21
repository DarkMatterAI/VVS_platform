
def test_server_ping(test_api_client):
    response = test_api_client.get('/')
    assert response.status_code == 200 