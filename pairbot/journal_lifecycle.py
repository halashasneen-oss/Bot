"""Stop every journal producer before writing the terminal complete record."""
import asyncio


async def stop_producers(tasks):
    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            raise RuntimeError('Journal producer failed during shutdown') from result
