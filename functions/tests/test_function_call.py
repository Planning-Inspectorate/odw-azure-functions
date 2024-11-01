import requests

def check_api_status(url):
    response = requests.get(url)
    return response.status_code

def test_api_response_serviceuser():
    url = "http://localhost:7071/api/serviceuser"
    assert check_api_status(url) == 200, f"API call to {url} did not return 200"

def test_api_response_nsipdocument():
    url = "http://localhost:7071/api/nsipdocument"
    assert check_api_status(url) == 200, f"API call to {url} did not return 200"

def test_api_response_nsipsubscription():
    url = "http://localhost:7071/api/nsipsubscription"
    assert check_api_status(url) == 200, f"API call to {url} did not return 200"