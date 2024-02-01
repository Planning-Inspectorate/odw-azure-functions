import pytest
import aiohttp

async def make_async_http_call(url):
    timeout = aiohttp.ClientTimeout(total = 60)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout = timeout) as response:
            return response

@pytest.mark.asyncio
@pytest.mark.parametrize("url", [
        "http://localhost:7071/api/caseschedule",
        "http://localhost:7071/api/employee",
        "http://localhost:7071/api/nsipdocument",
        "http://localhost:7071/api/nsipexamtimetable",
        "http://localhost:7071/api/nsipproject",
        "http://localhost:7071/api/nsipprojectupdate",
        "http://localhost:7071/api/nsiprepresentation",
        "http://localhost:7071/api/nsips51advice",
        "http://localhost:7071/api/nsipsubscription",
        "http://localhost:7071/api/serviceuser"
    ]
)

async def test_async_http_call(url):

    try:
        response = await make_async_http_call(url)

        if response.status == 200:
            print(f"Success - {url} returned status code {response.status}")
            # Successful response with status code 200
            assert response.status == 200, f"Failed for URL: {url}"
        elif response.status == 500:
            # Response with status code 500, check for "Failed validation"
            text = await response.text()
            print(f"Failed - {url} returned status code {response.status} - {response.text()}")
            assert "Failed validation" in text, f"Failed validation not found in response for URL: {url}"
        else:
            # Unexpected status code
            print(f"Failed - {url} returned status code {response.status} - {response.text}")
            assert False, f"Unexpected status code {response.status} for URL: {url}"

    except aiohttp.ClientConnectionError as e:
        raise aiohttp.ClientConnectionError(f"Servicebus topic or subscription does not exist for {url}: {e}")

if __name__ == "__main__":
    pytest.main(["-v", "test_http_calls.py", "-n", "auto"])