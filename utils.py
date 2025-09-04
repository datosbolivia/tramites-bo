"""
Utilidades adicionales para obtención de trámites.
"""

import asyncio
import functools
import httpx


TRANSIENT = {408, 425, 429, 500, 502, 503, 504}

def async_httpx_retry(max_retries: int = 3, base_delay: float = 0.5):
    """
    Decorator for async HTTP requests with retry functionality.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds (will exponentially increase)
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except httpx.InvalidURL:
                    raise
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in TRANSIENT and attempt < max_retries:
                        await asyncio.sleep(base_delay * (2 ** attempt))
                        continue
                    raise
                except (httpx.RequestError, asyncio.TimeoutError):
                    if attempt < max_retries:
                        await asyncio.sleep(base_delay * (2 ** attempt))
                        continue
                    raise
        return wrapper
    return decorator
