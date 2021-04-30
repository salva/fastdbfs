import sys
import asyncio
import aiohttp
import os
import os.path
import urllib.parse
import time
import base64
import traceback
import base64
import json
import progressbar

from fastdbfs.fileinfo import FileInfo

class Disconnected():
    def __init__(self):
        pass

    def __getattr__(self, method):
        raise Exception("open must be called first!")

    def prompt(self):
        return "*disconnected* "

class DBFS():

    def __init__(self, id, host, cluster_id, token,
                 workers=8, chunk_size=1048576, max_retries=10,
                 error_delay=10, error_delay_increment=10):
        self.id = id
        self.host = host
        self.cluster_id = cluster_id
        self.token = token
        self.path = "/"
        self.workers = workers
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.error_delay = error_delay
        self.error_delay_increment = error_delay_increment
        self.wait_until = 0

        self.semaphore = asyncio.Semaphore(workers)

        # print(f"host: {self.host}, cluster: {self.cluster_id}, token: {self.token}")

    def check(self):
        self._assert_dir(".")

    def _resolve(self, path, *more):
        return os.path.normpath(os.path.join(self.path, path, *more))

    def prompt(self):
        if self.id == "DEFAULT":
            return f"{self.path}$ "
        else:
            return f"{self.id}:{self.path}$ "

    def cd(self, path):
        path = self._resolve(path)
        self._assert_dir(path)
        self.path = path

    async def _unpack_response(self, response):
        status = response.status

        if response.headers.get("x-envoy-ratelimited", None) == "true":
            # print(f"Too many requests!")
            raise RateError("Too many requests")

        if "content-type" not in response.headers:
            print(f"Response headers: {response.headers}")

        ct = response.headers['content-type']
        if ct != "application/json":
            raise Exception(f"Unexpected response received from API, Content-type: {ct}, HTTP status: {status}")

        try:
            data = await response.json()
        except OSError as ex:
            # connection errors and alike should be retried
            raise ex
        except Exception as ex:
            raise Exception("Unable to retrieve API response") from ex

        if status == 200:
            return data

        try:
            error_code = data["error_code"]
            message = data["message"]
        except:
            raise Exception("Unexpected response received from API, HTTP status {status}")

        raise APIError(error_code, message)

    async def _async_control(self, cb):
        retries = 0
        while True:
            async with self.semaphore:
                try:
                    delta = self.wait_until - time.time()
                    if delta > 0:
                        # print(f"Delaying for {delta} seconds")
                        await asyncio.sleep(delta)
                    return await cb()
                except RateError as ex:
                    self.wait_until = time.time() + 1
                    # we do not increase the retries count when a
                    # RateError pops up!
                except OSError as ex:
                    if retries > self.max_retries:
                        raise ex
                    # We keep the semaphore while we sleep in order to
                    # reduce the pressure on the remote side.
                    await asyncio.sleep(self.error_delay + self.error_delay_increment * retries)
                    retries += 1

                except:
                    _, ex, trace = sys.exc_info()
                    print("Stack trace:")
                    traceback.print_tb(trace)
                    raise ex


    async def _async_http_get(self, session, end_point, **params):
        async def cb():
            url = urllib.parse.urljoin(self.host, end_point)
            headers = { "Authorization": "Bearer " + self.token }
            #print(f"session: {session}\nurl: {url}\nparams: {params}\nheaders: {headers}")
            async with session.get(url,
                                   params=params,
                                   headers=headers) as response:
                return await self._unpack_response(response)
        return await self._async_control(cb)


    async def _async_http_post(self, session, end_point, **data):
        async def cb():
            url = urllib.parse.urljoin(self.host, end_point)
            headers = { "Authorization": "Bearer " + self.token,
                        "Content-Type": "application/json" }
            load = json.dumps(data)
            async with session.post(url,
                                    headers=headers,
                                    data=load) as response:
                return await self._unpack_response(response)

        return await self._async_control(cb)

    async def _async_call_with_session(self, cb, *params, **kwparams):
        async with aiohttp.ClientSession() as session:
            return await cb(session, *params, **kwparams)

    async def _async_get_status(self, session, path):
        r = await self._async_http_get(session, "api/2.0/dbfs/get-status",
                                       path=self._resolve(path))
        return FileInfo.from_json(r)

    def _run_with_session(self, task, *args, **kwargs):
        task_with_session = self._async_call_with_session(task, *args, **kwargs)
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(task_with_session)

    def _simple_get(self, end_point, **params):
        return self._run_with_session(self._async_http_get, end_point, **params)

    def _simple_post(self, end_point, **params):
        return self._run_with_session(self._async_http_post, end_point, **params)

    def get_status(self, path):
        return self._run_with_session(self._async_get_status, path)

    async def _async_mkdir(self, session, path):
        path = self._resolve(path)
        await self._async_http_post(session, "api/2.0/dbfs/mkdirs", path=path)

    def mkdir(self, path):
        return self._run_with_session(self._async_mkdir, path=path)

    async def _async_list(self, session, path):
        path = self._resolve(path)
        #print(f"> {path}")
        r = await self._async_http_get(session, "api/2.0/dbfs/list", path=path)
        #print(f"listing {path} done")
        return [FileInfo.from_json(e) for e in r.get("files", [])]

    def ls(self, path):
        path = self._resolve(path)
        fi = self.get_status(path)
        if fi.is_dir():
            return self._run_with_session(self._async_list, path=path)
        else:
            return [fi]

    async def _async_rm(self, session, path, recursive=False):
        path = self._resolve(path)
        await self._async_http_post(session, "api/2.0/dbfs/delete",
                                    path=path, recursive=recursive)

    def rm(self, path, recursive=False):
        path = self._resolve(path)
        self._run_with_session(self._async_rm,
                               path=path, recursive=recursive)

        # Have we removed the CWD?
        if self.path != "/":
            if (self.path == path or
                (self.path.startswith(path) and self.path[len(path)] == "/")):
                # Yes, fix it!
                self.path = os.path.split(path)[0]

    def _assert_dir(self, path):
        if not self.get_status(path).is_dir():
            raise Exception("Not a directory")

    async def _async_create(self, session, path, overwrite=False):
        path = self._resolve(path)
        out = await self._async_http_post(session,
                                     "api/2.0/dbfs/create",
                                     path=path, overwrite=overwrite)
        return int(out["handle"])

    async def _async_add_block(self, session, handle, block):
        return await self._async_http_post(session,
                                           "api/2.0/dbfs/add-block",
                                           data=base64.standard_b64encode(block).decode('ascii'),
                                           handle=handle)

    async def _async_close(self, session, handle):
        return await self._async_http_post(session,
                                           "api/2.0/dbfs/close",
                                           handle=handle)

    async def _async_put_from_file(self, session, infile, target, size=None, overwrite=False, update_cb=None):
        target = self._resolve(target)

        handle = await self._async_create(session, target, overwrite)
        try:
            bytes_copied = 0
            while True:
                chunk = infile.read(self.chunk_size)
                chunk_len = len(chunk)
                if chunk_len == 0:
                    break

                await self._async_add_block(session, handle, chunk)

                bytes_copied += chunk_len
                if update_cb:
                    await update_cb(size, bytes_copied)

            await self._async_close(session, handle)

            fi = await self._async_get_status(session, target)
            if fi.size() != bytes_copied:
                raise Exception(f"corrupted copy detected, copied: {bytes_copied}, remote: {fi.size()}, expected: {size}")
            return fi
        except Exception as ex:
            try: await self._async_rm(target)
            except: pass
            raise ex

    def put_from_file(self, infile, target, size=None, overwrite=False):

        with progressbar.DataTransferBar() as bar:
            if size is not None:
                bar.max_value = size

            async def update_cb(size, bytes_read):
                bar.update(bytes_read)

            return self._run_with_session(self._async_put_from_file,
                                          infile, target, size, overwrite,
                                          update_cb)

    async def _async_put(self, session, src, target, **kwargs):
        size = os.stat(src).st_size
        with open(src, "rb") as infile:
            return await self._async_put_from_file(session, infile, target, size, **kwargs)

    def _new_swarm(self, workers, queue_max_size, use_priority_queue=False):

        queue_class = asyncio.PriorityQueue if use_priority_queue else asyncio.Queue
        # print(f"allocating queue of class {queue_class}")
        queue = queue_class(maxsize = queue_max_size)

        async def worker(ix):
            # print(f"worker {ix} started")

            async with aiohttp.ClientSession() as session:
                while True:
                    # print(f"worker {ix}  waiting for tasks")
                    (task_key, cb, response_queue, kwargs) = await queue.get()
                    if cb is None:
                        return
                    try:
                        # print(f"cb kwargs: {kwargs}")
                        value = await cb(session, **kwargs)
                        res = (task_key, value, None)
                    except Exception as ex:
                        #_, ex, trace = sys.exc_info()
                        #print("Stack trace:")
                        #traceback.print_tb(trace)

                        res = (task_key, None, ex)
                    #print(f"Queueing response for {task_key}")
                    if response_queue:
                        await response_queue.put(res)

        async def end():
            for ix in range(workers):
                await self._queue_task(queue, None, task_key=ix)

        swarm = asyncio.gather(*[worker(ix) for ix in range(workers)])
        return (swarm, queue, end)

    async def _queue_task(self, _queue, _task,
                          task_key=None, response_queue=None,
                          **kwargs):
        return await _queue.put((task_key, _task, response_queue, kwargs))

    def _unwrap_response(self, res):
        (_, value, ex) = res
        if ex is not None:
            raise ex
        return value

    async def _async_get_chunk(self, session, path, offset, length, out):
        # print(f"processing chunk at {offset}")
        remaining = length
        while remaining > 0:
            r = await self._async_http_get(session,
                                           "api/2.0/dbfs/read",
                                           path=path, offset=offset, length=length)

            bytes_read = r["bytes_read"]
            if bytes_read == 0 or bytes_read > remaining:
                raise Exception("Invalid data response")

            chunk = base64.standard_b64decode(r["data"])
            if len(chunk) != bytes_read:
                raise Exception("Invalid data response, size is not as promised")

            out.seek(offset)
            out.write(chunk)
            remaining -= bytes_read
            offset += bytes_read
        # print(f"chunk of {length} bytes copied at offset {offset - length}")
        return length

    async def _async_get(self, session, low_queue, src, target,
                         overwrite=False, update_cb=None):
        # FIXME: a race condition exists here!
        if not overwrite and os.path.exists(target):
            raise Exception("File already exists")
        with open(target, "wb") as out:
            await self._async_get_to_file(session, low_queue, src, out, update_cb)

    async def _async_get_to_file(self, session, low_queue, src, out, update_cb=None):
        src = self._resolve(src)
        fi = await self._async_get_status(session, src)
        size = fi.size()

        response_queue = asyncio.Queue()
        active_tasks = 0 # we count the request we have queued
        bytes_copied = 0
        offset = 0

        try:
            while True:
                while True:
                    try:
                        res = response_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    active_tasks -= 1
                    bytes_copied += self._unwrap_response(res)
                    if update_cb:
                        await update_cb(size, bytes_copied)

                next_offset = min(offset + self.chunk_size, size)
                length = next_offset - offset
                if length <= 0:
                    break

                await self._queue_task(low_queue,
                                       self._async_get_chunk,
                                       response_queue=response_queue,
                                       path=src, offset=offset, length=length,
                                       out=out)
                active_tasks += 1
                offset = next_offset

            while active_tasks > 0:
                res = await response_queue.get()
                active_tasks -= 1
                bytes_copied += self._unwrap_response(res)
                if update_cb:
                    await update_cb(size, bytes_copied)

        finally:
            for _ in range(active_tasks):
                try: await response_queue.get()
                except: pass

        return fi


    def get_to_file(self, src, out):

        (swarm, queue, end) = self._new_swarm(self.workers, self.workers * 2)

        with progressbar.DataTransferBar() as bar:
            async def update_cb(size, read):
                bar.max_value = size
                bar.update(read)


            result = None
            async def control():
                nonlocal result
                try:
                    result = await self._async_call_with_session(self._async_get_to_file,
                                                                 queue, src, out, update_cb)
                finally:
                    await end()

            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.gather(control(), swarm))
            return result

    async def _async_find(self, session, path, update_cb):
        path = self._resolve(path)
        root_fi = await self._async_get_status(session, path)

        if not root_fi.is_dir():
            await update_cb(root_fi, True, None)
            return

        response_queue = asyncio.Queue()
        (swarm, queue, end) = self._new_swarm(workers = self.workers,
                                              queue_max_size = self.workers,
                                              use_priority_queue=True)

        async def control():
            await self._queue_task(queue,
                                   self._async_list,
                                   task_key = path,
                                   response_queue = response_queue,
                                   path = path)

            pending = [[path, root_fi, None, None]] # path, fileinfo, ok, exception

            while pending:
                (key, fis, ex) = await response_queue.get()
                # update the entry
                for e in pending:
                    if e[0] == key:
                        if ex:
                            e[2] = False
                            e[3] = ex
                        else:
                            e[2] = True
                        break
                else:
                    raise Exception("Internal error, key not found in pending")

                if fis:
                    # add the new entries
                    pending += [[fi.abspath(), fi,
                                 None if fi.is_dir() else True,
                                 None]
                                for fi in fis]
                    pending.sort()

                    # request their listings
                    for fi in fis:
                        if fi.is_dir():
                            #print(f"< {fi.abspath()}")
                            await self._queue_task(queue,
                                                   self._async_list,
                                                   task_key = fi.abspath(),
                                                   response_queue = response_queue,
                                                   path = fi.abspath())

                # finally report entries ready from the top
                while pending and pending[0][2] is not None:
                    head = pending.pop(0)
                    await update_cb(head[1], head[2], head[3])
            await end()
        await asyncio.gather(control(), swarm)

    def find(self, path, update_cb):
        path = self._resolve(path)

        async def _async_update_cb(fi, ok, ex):
            update_cb(fi, ok, ex)

        self._run_with_session(self._async_find,
                               path,
                               _async_update_cb)

    async def _async_rget(self, session, src, target, update_cb=None):
        src = self._resolve(src)

        # We use two worker queues here, low is for the low level
        # requests. We pass it to the _self_get method without ever
        # using it directly. high_queue is for parallelizing the
        # _self_get calls.

        (high_swarm, high_queue, high_end) = self._new_swarm(self.workers, self.workers * 2)
        (low_swarm, low_queue, low_end) = self._new_swarm(self.workers, self.workers * 2)
        response_queue = asyncio.Queue()
        active_gets = 0

        async def empty_response_queue():
            nonlocal active_gets
            while True:
                try:
                    (relpath, res, ex) = response_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                active_gets -= 1
                ok = ex is None
                await update_cb(relpath, ok, ex)

        async def find_cb(fi, ok, ex):
            nonlocal active_gets

            # We empty the response_queue everytime this function is called.
            await empty_response_queue()

            # print(f"entry found: {fi.abspath()}, ok: {ok}, ex: {ex}")

            remote_path = fi.abspath()
            relpath = fi.relpath(src)
            local_path = os.path.abspath(os.path.join(target, relpath))

            if fi.is_dir():
                try:
                    # print(f"creating local path {local_path}")
                    os.mkdir(local_path)
                except Exception as ex:
                    if not os.path.isdir(local_path):
                        if update_cb:
                            await update_cb(relpath, False, ex)
            else:
                await self._queue_task(high_queue,
                                       self._async_get,
                                       task_key=relpath,
                                       response_queue=response_queue,
                                       low_queue=low_queue, src=remote_path, target=local_path)
                active_gets += 1

        # So, here we go!
        # First we run the find task...
        await self._async_find(session, src, find_cb)

        # Then we finish the high level queue...
        await high_end()

        # Remove any pending notification from response_queue...
        while active_gets > 0:
            (relpath, res, ex) = await response_queue.get()
            active_gets -= 1
            ok = ex is None
            await update_cb(relpath, ok, ex)

        # And finally terminate the low queue...
        await low_end()


    def rget(self, src, target, update_cb=None):
        async def _async_update_cb(*args):
            update_cb(*args)
        self._run_with_session(self._async_rget,
                               src, target,
                               _async_update_cb if update_cb else None)

    async def _async_rput(self, session, src, target, update_cb=None):
        target = self._resolve(target)
        local_root_fi = FileInfo.from_local(src)

        (swarm, queue, end) = self._new_swarm(self.workers, self.workers * 2)
        response_queue = asyncio.Queue()

        async def control():
            active_puts = 0
            for (dir, _, filenames) in os.walk(src):
                local_fi = FileInfo.from_local(dir)
                relpath = local_fi.relpath(local_root_fi.abspath())
                remote_path = self._resolve(target, relpath)
                local_path = os.path.join(src, relpath)

                status = None
                try:
                    await self._async_mkdir(session, path=remote_path)
                    if update_cb:
                        await update_cb(local_path, True, None)

                except Exception as ex:
                    if update_cb:
                        await update_cb(local_path, False, ex)
                        for fn in filenames:
                            await update_cb(os.path.join(local_path, fn), False, None)

                else:
                    for fn in filenames:
                        local_child_path = os.path.join(local_path, fn)
                        remote_child_path = self._resolve(remote_path, fn)
                        #print(f"remote_child_path: {remote_child_path}")
                        await self._queue_task(queue,
                                               self._async_put,
                                               task_key=local_child_path,
                                               src=local_child_path,
                                               target=remote_child_path,
                                               response_queue=response_queue)
                        active_puts += 1


                while True:
                    try:
                        (local_child_path, res, ex) = response_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    active_puts -= 1
                    if update_cb:
                        ok = ex is None
                        await update_cb(local_child_path, ok, ex)

            while active_puts > 0:
                (local_child_path, res, ex) = await response_queue.get()
                active_puts -= 1
                if update_cb:
                    ok = ex is None
                    await update_cb(local_child_path, ok, ex)

            await end()
        await asyncio.gather(control(), swarm)

    def rput(self, src, target, update_cb=None):
        async def _async_update_cb(fi, ok, ex):
            update_cb(fi, ok, ex)
        self._run_with_session(self._async_rput, src, target,
                               _async_update_cb if update_cb else None)

