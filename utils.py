from typing import Callable
from time import sleep
import httpx
from sqlalchemy.exc import DBAPIError

class BadRequestResponseError(Exception):
    pass

def repeated_query_attempts(number_of_attempts = 10, wait_time_seconds=10) -> Callable:
    def decorator(func: Callable) -> Callable:
        def repeated_query_function(*args, **kwargs):
            count = 0
            while count < number_of_attempts:
                try:
                    return func(*args, **kwargs)
                except DBAPIError as err:
                    # only handle this particular flavor of error
                    if not "sqlitecloud.exceptions.SQLiteCloudException" in str(err):
                        raise err
                    else:
                        print(f'Attempt {count+1}/{number_of_attempts}, waiting {wait_time_seconds} seconds...')
                        sleep(wait_time_seconds)
                        count += 1
            return func(*args, **kwargs)
        return repeated_query_function
    return decorator
                    

async def fetch(url: str, client: None| httpx.AsyncClient = None, **kwargs) -> dict|list[dict]:
    """From here: https://stackoverflow.com/questions/22190403/how-could-i-use-requests-in-asyncio/50312981#50312981"""
    if client:
        response = await client.get(url, **kwargs)
    else:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, **kwargs)
    return response.json()
    
