import pytest
import httpx
import asyncio

urls_to_test = [
    "http://localhost:7071/api/serviceuser",
    "http://localhost:7071/api/nsipdocument",
    "http://localhost:7071/api/nsipsubscription"
]

async def check_api_status(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.status_code

@pytest.mark.asyncio
async def test_api_responses():
    tasks = [check_api_status(url) for url in urls_to_test]
    results = await asyncio.gather(*tasks)

    for i, status_code in enumerate(results):
        assert status_code == 200, f"API call to {urls_to_test[i]} did not return 200"