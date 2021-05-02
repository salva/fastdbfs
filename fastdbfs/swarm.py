
import asyncio
import aiohttp
import logging

seq = 0

class Swarm:
    def __init__(self, max_workers, queue_max_size, use_priority_queue=False, name=None):
        global seq
        seq += 1
        self.name = name or f"swarm{seq}"
        queue_class = asyncio.PriorityQueue if use_priority_queue else asyncio.Queue
        self.queue = queue_class(maxsize = queue_max_size)
        self.max_workers = max_workers

    async def run(self):
        logging.debug(f"Swarm {self.name} running")
        workers = [self._worker(ix) for ix in range(self.max_workers)]
        await asyncio.gather(*workers)

    async def run_while(self, task):
        logging.debug("Swarm {self.name} running")

        r = None
        ex = None

        async def wrapper():
            nonlocal r, ex
            try:
                r = await task
                #r = await asyncio.gather(task)[0]
            except Exception as ex1:
                logging.debug(f"Swarm {self.name} concurrent task failed, exception: {ex1}")
                ex = ex1
            await self.terminate()

        workers = [self._worker(ix) for ix in range(self.max_workers)]
        await asyncio.gather(wrapper(), *workers)

        if ex:
            logging.debug(f"Swarm {self.name} rethrowing exception {ex}")
            raise ex
        logging.debug(f"Swarm {self.name} run_while done")
        return r

    def loop_until_complete(self, task):
        loop = asyncio.get_event_loop()
        r = loop.run_until_complete(self.run_while(task))
        logging.debug(f"Swarm {self.name} loop_until_complete done")
        return r

    async def terminate(self):
        logging.debug(f"Swarm {self.name} terminating")
        for ix in range(self.max_workers):
            await self.put(None, task_key=ix)

    async def _worker(self, ix):
        logging.debug(f"Worker {ix} started")

        async with aiohttp.ClientSession() as session:
            while True:
                logging.debug(f"Swarm {self.name}/{ix} waiting for tasks")
                (task_key, cb, response_queue, kwargs) = await self.queue.get()
                if cb is None:
                    logging.debug(f"Swarm {self.name}/{ix} terminating")
                    return
                try:
                    logging.debug(f"Swarm {self.name}/{ix} running task {task_key}")
                    value = await cb(session, **kwargs)
                    res = (task_key, value, None)
                except Exception as ex:
                    logging.debug(f"Swarm {self.name}/{ix} running task {task_key} failed with exception {ex}")
                    res = (task_key, None, ex)

                if response_queue:
                    logging.debug(f"Swarm {self.name}/{ix} running task {task_key} queues response")
                    await response_queue.put(res)

    async def put(self, task, task_key=None, response_queue=None, **kwargs):
        wrapper = (task_key, task, response_queue, kwargs)
        logging.debug(f"Swarm {self.name} queueing wrapper {wrapper}")
        return await self.queue.put(wrapper)

    @staticmethod
    def unwrap_response(res):
        (_, value, ex) = res
        if ex is not None:
            logging.debug(f"Rethrowing exception received from Swarm: {ex}")
            raise ex
        return value
