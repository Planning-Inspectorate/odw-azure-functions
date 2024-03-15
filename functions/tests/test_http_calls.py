import pytest
import aiohttp


async def make_async_http_call(url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=timeout) as response:
            content = await response.text()
            return response, content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        # "http://localhost:7071/api/caseschedule",
        # "http://localhost:7071/api/employee",
        "http://localhost:7071/api/nsipdocument",
        "http://localhost:7071/api/nsipexamtimetable",
        "http://localhost:7071/api/nsipproject",
        # "http://localhost:7071/api/nsipprojectupdate", no data
        # "http://localhost:7071/api/nsiprepresentation", no data
        "http://localhost:7071/api/nsips51advice",
        # "http://localhost:7071/api/nsipsubscription", no data
        "http://localhost:7071/api/serviceuser",
    ],
)
async def test_async_http_call(url):
    try:
        response, content = await make_async_http_call(url)

        if response.status == 200:
            print(f"Success - {url} returned status code {response.status}")
            # Successful response with status code 200
            assert response.status == 200, f"Failed for URL: {url}"
        elif response.status == 500:
            # Response with status code 500, most likely a validation error
            print(f"Failed - {url} returned status code {response.status} - {content}")
            assert response.status == 200
        else:
            # Unexpected status code
            print(
                f"Failed - {url} returned status code {response.status} - {response.content}"
            )
            assert False, f"Unexpected status code {response.status} for URL: {url}"

    except aiohttp.ClientConnectionError as e:
        raise aiohttp.ClientConnectionError(
            f"Servicebus topic or subscription does not exist for {url}: {e}"
        )


if __name__ == "__main__":
    pytest.main(["-v", "test_http_calls.py", "-n", "auto"])
