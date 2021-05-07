import sys
import asyncio
import aiohttp
import os
import os.path
import posixpath
import urllib.parse
import time
import base64
import traceback
import base64
import json
import logging
import inspect
import tempfile

from fastdbfs.fileinfo import FileInfo
from fastdbfs.swarm import Swarm
from fastdbfs.exceptions import RateError, APIError
import fastdbfs.util

class FindEntry():
    def __init__(self, fi, good=True, ex=None, done=False):
        self.fi = fi
        self.good = good
        self.ex = ex
        self.done = done

    def __str__(self):
        return f"FindEntry(path={self.fi.abspath()}, done={self.done}, ex={self.ex})"

    def __repr__(self):
        return self.__str__()

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
        self.cwd = "/"
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
        return posixpath.normpath(posixpath.join(self.cwd, path, *more))

    def prompt(self):
        if self.id == "DEFAULT":
            return f"{self.cwd}$ "
        else:
            return f"{self.id}:{self.cwd}$ "

    def cd(self, path):
        path = self._resolve(path)
        self._assert_dir(path)
        self.cwd = path

    def _asynchronize(self, cb):
        if cb is None:
            return None
        async def async_cb(*args, **kwargs):
            return cb(*args, **kwargs)
        return async_cb

    def _make_swarm(self, name=None, queue_max_size=None, **kwargs):
        if name is None:
            name = inspect.stack()[1].function
        if queue_max_size is None:
            queue_max_size=self.workers
        return Swarm(self.workers, queue_max_size=queue_max_size, name=name, **kwargs)

    async def _unpack_http_response(self, response):
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
            logging.debug("Unable to retrieve API response {ex}", exc_info=True)
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

                except Exception as ex:
                    logging.debug("Stack trace:", exc_info=True)
                    raise ex

    def _async_http_get(self, session, end_point, **params):
        async def cb():
            url = urllib.parse.urljoin(self.host, end_point)
            headers = { "Authorization": "Bearer " + self.token }
            #print(f"session: {session}\nurl: {url}\nparams: {params}\nheaders: {headers}")
            async with session.get(url,
                                   params=params,
                                   headers=headers) as response:
                return await self._unpack_http_response(response)
        return self._async_control(cb)

    def _async_http_post(self, session, end_point, **data):
        async def cb():
            url = urllib.parse.urljoin(self.host, end_point)
            headers = { "Authorization": "Bearer " + self.token,
                        "Content-Type": "application/json" }
            load = json.dumps(data)
            async with session.post(url,
                                    headers=headers,
                                    data=load) as response:
                return await self._unpack_http_response(response)

        return self._async_control(cb)

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

    async def _async_filetest(self, session, path, fi_check_cb):
        try:
            fi = await self._async_get_status(session, path)
            return fi_check_cb(fi)
        except APIError as ex:
            if ex.error_code == "RESOURCE_DOES_NOT_EXIST":
                return False
            raise ex

    def filetest_e(self, path):
        return self._filetest(path, lambda _: True)

    def filetest_d(self, path):
        self._filetest(path, lambda fi: fi.is_dir())

    def filetest_f(self, path):
        return self._filetest(path, lambda fi: not fi.is_dir())

    def _async_mkdir(self, session, path):
        path = self._resolve(path)
        return self._async_http_post(session, "api/2.0/dbfs/mkdirs", path=path)

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

    def _async_rm(self, session, path, recursive=False):
        path = self._resolve(path)
        return self._async_http_post(session, "api/2.0/dbfs/delete",
                                     path=path, recursive=recursive)

    def rm(self, path, recursive=False):
        path = self._resolve(path)
        self._run_with_session(self._async_rm,
                               path=path, recursive=recursive)

        # Have we removed the CWD?
        if self.cwd != "/":
            if (self.cwd == path or
                (self.cwd.startswith(path) and self.cwd[len(path)] == "/")):
                # Yes, fix it!
                self.cwd = posixpath.split(path)[0]

    def _async_simple_mv(self, session, src, target):
        return self._async_http_post(session, "api/2.0/dbfs/move",
                                     source_path=self._resolve(src),
                                     destination_path=self._resolve(target))

    async def _async_mv(self, session, src, target, overwrite=False):
        src = self._resolve(src)
        target = self._resolve(target)
        try:
            return await self._async_simple_mv(session, src, target)
        except APIError as ex:
            if (ex.error_code == "RESOURCE_ALREADY_EXISTS" and
                await self._async_filetest(session, target,
                                           lambda fi: not fi.is_dir())):
                await self._async_rm(session, target)
                return await self._async_simple_mv(session, src, target)
            raise ex

    def mv(self, src, target, overwrite=False):
        return self._run_with_session(self._async_mv,
                                      src, target,
                                      overwrite=overwrite)

    def _assert_dir(self, path):
        if not self.get_status(path).is_dir():
            raise Exception("Not a directory")

    async def _async_put(self, session, src, target, overwrite=False, update_cb=None):
        target = self._resolve(target)
        st = os.stat(src)
        size = st.st_size
        with open(src, "rb") as infile:
            if update_cb:
                await update_cb(size=size, bytes_copied=0)

            if size > self.chunk_size:
                return await _async_stream_put_from_file(session,
                                                         infile, target,
                                                         size=st.st_size,
                                                         overwrite=overwrite,
                                                         update_cb=update_cb)
            else:
                block = infile.read(size)
                while len(block) < size:
                    more = infile.read(size - len(block))
                    if len(more) == 0:
                        raise Exception("Unable to read data from local file")
                    block += more
                await self._async_http_post(session,
                                            "api/2.0/dbfs/put",
                                            path=self._resolve(target),
                                            contents=base64.standard_b64encode(block).decode('ascii'),
                                            overwrite=overwrite)
                if update_cb:
                    await update_cb(size=size, bytes_copied=size)

    async def _async_create(self, session, path, overwrite=False):
        path = self._resolve(path)
        out = await self._async_http_post(session,
                                     "api/2.0/dbfs/create",
                                     path=path, overwrite=overwrite)
        return int(out["handle"])

    def _async_add_block(self, session, handle, block):
        return self._async_http_post(session,
                                     "api/2.0/dbfs/add-block",
                                     data=base64.standard_b64encode(block).decode('ascii'),
                                     handle=handle)

    def _async_close(self, session, handle):
        return self._async_http_post(session,
                                     "api/2.0/dbfs/close",
                                     handle=handle)

    async def _async_stream_put_from_file(self, session, infile, target,
                                          size=None, overwrite=False,
                                          update_cb=None):
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
                    await update_cb(size=size, bytes_copied=bytes_copied)

            await self._async_close(session, handle)

            fi = await self._async_get_status(session, target)
            if fi.size() != bytes_copied:
                raise Exception(f"corrupted copy detected, copied: {bytes_copied}, remote: {fi.size()}, expected: {size}")
            return fi
        except Exception as ex:
            try: await self._async_rm(target)
            except: pass
            raise ex

    def put(self, src, target, overwrite, update_cb):
        return self._run_with_session(self._async_put,
                                      src, target,
                                      overwrite=overwrite,
                                      update_cb=self._asynchronize(update_cb))

    async def _async_get_chunk(self, session, path, offset, length, out):
        logging.debug(f"processing chunk at {offset}, length {length}")
        remaining = length
        while remaining > 0:
            r = await self._async_http_get(session,
                                           "api/2.0/dbfs/read",
                                           path=path, offset=offset, length=length)

            bytes_read = r["bytes_read"]
            logging.debug(f"received {bytes_read} bytes, offset {offset}")
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

    async def _async_get(self, session, low_swarm, src, target,
                         overwrite=False, mkdirs=False, update_cb=None):
        parentdir = os.path.dirname(target)
        if mkdirs:
            fastdbfs.util.mkdirs(parentdir)

        # FIXME: a race condition exists here!
        if not overwrite and os.path.exists(target):
            raise Exception("File already exists")
        tmp_fn = await self._async_get_to_temp(session, low_swarm, src,
                                               dir=parentdir, update_cb=update_cb)
        os.rename(tmp_fn, target)

    async def _async_get_to_temp(self, session, low_swarm, src, update_cb=None,
                                 prefix=".fastdbfs-transfer-", **mkstemp_args):
        try:
            (f, target) = tempfile.mkstemp(prefix=prefix, **mkstemp_args)
            with os.fdopen(f, "wb") as out:
                await self._async_get_to_file(session, low_swarm, src, out, update_cb=update_cb)
                return target
        except Exception as ex:
            try: os.remove(target)
            except: pass
            raise ex

    async def _async_get_to_file(self, session, low_swarm, src, out, update_cb=None):
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
                    bytes_copied += Swarm.unwrap_response(res)
                    if update_cb:
                        await update_cb(size, bytes_copied)

                next_offset = min(offset + self.chunk_size, size)
                length = next_offset - offset
                if length <= 0:
                    break

                await low_swarm.put(self._async_get_chunk,
                                    response_queue=response_queue,
                                    path=src, offset=offset, length=length,
                                    out=out)
                active_tasks += 1
                offset = next_offset

            while active_tasks > 0:
                res = await response_queue.get()
                active_tasks -= 1
                bytes_copied += Swarm.unwrap_response(res)
                if update_cb:
                    await update_cb(size, bytes_copied)

        finally:
            for _ in range(active_tasks):
                try: await response_queue.get()
                except: pass

        return fi

    def get_to_temp(self, src, update_cb=None, **mkstemp_args):
        swarm = self._make_swarm()
        task = self._async_call_with_session(self._async_get_to_temp,
                                             swarm, src,
                                             update_cb=self._asynchronize(update_cb),
                                             **mkstemp_args)
        return swarm.loop_until_complete(task)

    def get(self, src, target,
            overwrite=False, mkdirs=True, update_cb=None):
        swarm = self._make_swarm()
        task = self._async_call_with_session(self._async_get,
                                             swarm, src, target,
                                             overwrite=overwrite, mkdirs=mkdirs,
                                             update_cb=self._asynchronize(update_cb))
        return swarm.loop_until_complete(task)

    def get_to_file(self, src, out, update_cb=None):
        swarm = self._make_swarm()
        task =  self._async_call_with_session(self._async_get_to_file,
                                              swarm, src, out,
                                              self._asynchronize(update_cb))
        return swarm.loop_until_complete(task)

    async def _async_find(self, session, path, update_cb, filter_cb=None, predicates={}):
        path = self._resolve(path)

        async def filter(entries):
            entries_by_relpath = { e.fi.relpath(path): e for e in entries }
            for relpath, e in entries_by_relpath.items():
                if not e.fi.check_predicates(relpath=relpath, **predicates):
                    e.good = False
            if filter_cb:
                input = { k: v.fi for k, v in entries_by_relpath.items() }
                selected_paths = await filter_cb(input)
                for p in selected_paths:
                    try:
                        del entries_by_relpath[p]
                    except KeyError:
                        logging.warning(f"Ignoring unexpected path {p} returned by filter")
                for e in entries_by_relpath.values():
                    e.good = False

        root_fi = await self._async_get_status(session, path)
        root_entry = FindEntry(root_fi)
        await filter([root_entry])

        if not root_fi.is_dir():
            await update_cb(entry=root_entry, entries_found=1, done=1)
            return

        swarm = self._make_swarm(use_priority_queue=True, queue_max_size=0)
        response_queue = asyncio.PriorityQueue()

        async def control():
            await swarm.put(self._async_list,
                            task_key = path,
                            response_queue = response_queue,
                            path = path)
            pending = [root_entry]
            entries_found = 1
            while pending:
                logging.debug(f"_async_find control() loop, first pending entry is {pending[0].fi.abspath()} of a total of {len(pending)}")
                (key, fis, ex) = await response_queue.get()
                logging.debug(f"_async_find control() loop, got response for {key}");
                for e in pending:
                    if e.fi.abspath() == key:
                        e.ex = ex
                        e.done = True
                        break
                else:
                    raise Exception("Internal error, key not found in pending")

                if fis:
                    entries_found += len(fis)
                    new = [FindEntry(fi, done=not fi.is_dir())
                           for fi in fis]
                    await filter(new)
                    pending += new
                    pending.sort(key=lambda x: x.fi.abspath())

                    for fi in fis:
                        if fi.is_dir():
                            await swarm.put(self._async_list,
                                            task_key = fi.abspath(),
                                            response_queue = response_queue,
                                            path = fi.abspath())

                # finally report entries ready from the top
                while pending and pending[0].done:
                    e = pending.pop(0)
                    logging.debug(f"_async_back control() passing up {e.fi.abspath}, good: {e.good}")
                    await update_cb(entry=e, max_entries=entries_found, done=entries_found-len(pending))
            logging.debug("_async_find control() done")

        await swarm.run_while(control())

    def find(self, path, update_cb, filter_cb=None, predicates={}):
        self._run_with_session(self._async_find,
                               self._resolve(path),
                               self._asynchronize(update_cb),
                               self._asynchronize(filter_cb),
                               predicates=predicates)

    def _needs_sync(self, local_path, fi):
        """Checks wheter the file at local_path is older or has a different
        size than the one in fi.
        """
        try:
            st = os.stat(local_path)
            logging.debug(f"comparing file {local_path} with stats {st} with fi {fi}")
            if st.st_size != fi.size():
                return True
            if st.st_mtime_ns  < fi.mtime() * 1000000:
                return True
            return False
        except:
            return True

    async def _async_rget(self, session, src, target, overwrite=False, sync=False,
                          update_cb=None, filter_cb=None, predicates={}):
        src = self._resolve(src)

        if sync:
            overwrite=True

        # We use two worker queues here, low is for the low level
        # requests. We pass it to the _self_get method without ever
        # using it directly. high_queue is for parallelizing the
        # _self_get calls.

        low_swarm = self._make_swarm(name="rget-low")
        high_swarm = self._make_swarm(name="rget-high")
        response_queue = asyncio.Queue()

        entries = 1 # those are the entries seen so far as reported by find
        active_gets = {}

        async def update(entry):
            if update_cb:
                await update_cb(entry=entry, max_entries=entries, done=entries-len(active_gets))

        async def update_after_get(relpath, ex):
            entry = active_gets.pop(relpath)
            entry.ex = ex
            logging.debug("Exception caught during file get")
            await update(entry)

        async def empty_response_queue():
            try:
                while True:
                    (relpath, _, ex) = response_queue.get_nowait()
                    await update_after_get(relpath, ex)
            except asyncio.QueueEmpty:
                return

        async def find_cb(entry, max_entries, done):
            nonlocal entries

            # We empty the response_queue everytime this function is called.
            await empty_response_queue()

            fi = entry.fi
            relpath = fi.relpath(src)
            local_path = os.path.abspath(os.path.join(target, relpath))
            entries = max_entries
            if fi.is_dir():
                if os.path.isdir(local_path):
                    logging.debug(f"directory already exists for {relpath}")
                    entry.good = False
                if entry.good: # mkdir
                    logging.debug(f"making dir for {relpath}")
                    try:
                        fastdbfs.util.mkdirs(local_path)
                    except Exception as ex:
                        if not os.path.isdir(local_path):
                            logging.warn(f"{relpath}: mkdirs failed. {ex}")
                            entry.ex = ex
                else:
                    logging.debug(f"discarding dir {relpath}")
            else:
                if sync and entry.good and not self._needs_sync(local_path, fi):
                        logging.debug(f"file {relpath} doesn't need synchronization")
                        entry.good = False
                if entry.good:
                    logging.debug(f"queueing download of {relpath}")
                    await high_swarm.put(self._async_get,
                                         task_key=relpath,
                                         response_queue=response_queue,
                                         low_swarm=low_swarm,
                                         src=fi.abspath(), target=local_path,
                                         overwrite=overwrite,
                                         mkdirs=True)
                    active_gets[relpath] = entry
                    return # don't report the entry yet!
                else:
                    logging.debug(f"discarding file {relpath}")

            await update(entry)

        # This code is a bit hairy because we run the control code and
        # both swarms concurrently:
        async def control():
            await high_swarm.run_while(self._async_find(session, src,
                                                        update_cb=find_cb,
                                                        filter_cb=filter_cb,
                                                        predicates=predicates))

            while active_gets:
                logging.debug(f"_async_rget control, {len(active_gets)} gets currently active")
                (relpath, _, ex) = await response_queue.get()
                await update_after_get(relpath, ex)

        await low_swarm.run_while(control())

    def rget(self, src, target,
             overwrite=False, sync=False,
             update_cb=None, filter_cb=None, predicates={}):
        self._run_with_session(self._async_rget,
                               src, target,
                               overwrite=overwrite,
                               sync=sync,
                               filter_cb=self._asynchronize(filter_cb),
                               update_cb=self._asynchronize(update_cb),
                               predicates=predicates)

    async def _async_rput(self, session, src, target, overwrite=False, update_cb=None):
        target = self._resolve(target)
        local_root_fi = FileInfo.from_local(src)

        swarm = self._make_swarm()
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
                        await swarm.put(self._async_put,
                                        task_key=local_child_path,
                                        src=local_child_path,
                                        target=remote_child_path,
                                        overwrite=overwrite,
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

        await swarm.run_while(control())

    def rput(self, src, target, overwrite=False, update_cb=None):
        self._run_with_session(self._async_rput, src, target,
                               overwrite=overwrite,
                               update_cb=self._asynchronize(update_cb))

