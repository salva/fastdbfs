
import asyncio
import aiohttp
import logging

seq = 0

class AlwaysComparable():
    def __init__(self, v):
        self.v = v

    def __lt__(a, b):
        av = a.v
        bv = b.v
        if bv is None: return True
        if av is None: return False
        if type(av) == type(bv): return av < bv
        if isinstance(av, int): return True
        if isinstance(bv, int): return False
        raise Exception("Internal error: don't know how to compare {type(av).__name__} with {type(bv).__name__}")

class Swarm:
    def __init__(self, max_workers, queue_max_size, use_priority_queue=False, name=None):
        global seq
        seq += 1
        self.name = name or f"swarm{seq}"
        queue_class = asyncio.PriorityQueue if use_priority_queue else asyncio.Queue
        self.queue = queue_class(maxsize = queue_max_size)
        self.max_workers = max_workers
        self.task_seq = 0


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
                task_key = task_key.v
                if cb is None:
                    logging.debug(f"Swarm {self.name}/{ix} terminating")
                    return
                try:
                    logging.debug(f"Swarm {self.name}/{ix} starting task {task_key}")
                    value = await cb(session, **kwargs)
                    res = (task_key, value, None)
                except Exception as ex:
                    logging.debug(f"Swarm {self.name}/{ix} running task {task_key} failed with exception {ex}",
                                  exc_info=True)
                    res = (task_key, None, ex)

                logging.debug(f"Swarm {self.name}/{ix} ended task {task_key}")
                if response_queue:
                    await response_queue.put(res)
                    logging.debug(f"Swarm {self.name}/{ix} queued response for task {task_key}")

    async def put(self, task, task_key=None, response_queue=None, **kwargs):
        if task_key is None:
            task_key = self.task_seq
            self.task_seq += 1
        wrapper = (AlwaysComparable(task_key), task, response_queue, kwargs)
        logging.debug(f"Swarm {self.name} queueing task {task and task.__name__} with key {task_key}")
        return await self.queue.put(wrapper)

    @staticmethod
    def unwrap_response(res):
        (_, value, ex) = res
        if ex is not None:
            logging.debug(f"Rethrowing exception received from Swarm: {ex}")
            raise ex
        return value
