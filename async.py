import asyncio
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer
from time import sleep


n_client_worker = 10


async def get_data_asynchronous(fn):
    print("{0:<30} {1:>20}".format("Start-time", "Elapsed"))
    with ThreadPoolExecutor(max_workers=n_client_worker) as executor:
        with requests.Session() as session:
            # Set any session parameters here before calling `fetch`
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor,
                    fn,
                    *(session, i, vals[i]) # Allows passing multiple arguments
                )
                for i in range(len(vals))
            ]
            return await asyncio.gather(*tasks)


def map_async(fn, vals):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_data_asynchronous(fn, vals))
    return loop.run_until_complete(future)
