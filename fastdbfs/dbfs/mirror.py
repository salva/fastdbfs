import os
import logging
import asyncio
import fastdbfs.util
from fastdbfs.walker import WalkerEntry
from fastdbfs.fileinfo import FileInfoLocal

class Mirror:

    def __init__(self, dbfs):
        self._dbfs = dbfs

    async def find(self, session, path, update_cb, filter_cb=None, predicates={}):
        raise NotImplementedError()

    async def transfer(self, session, low_swarm, src, target,
                       overwrite=False, mkdirs=False, update_db=None):
        raise NotImplementedError()

    def needs_low_swarm(self):
        return False

    async def mirror(self, session, src, target,
                     overwrite=False, sync=False,
                     update_cb=None, filter_cb=None, predicates={}):

        if sync:
            overwrite=True

        src = self.resolve_src(src)
        target = self.resolve_target(target)

        # We use two worker queues here, low is for the low level
        # requests. We pass it to the transfer method without ever
        # using it directly. high_queue is for running multiple
        # transfer calls concurrently.

        low_workers = None if self.needs_low_swarm() else 1 # None allocates the default
        low_swarm = self._dbfs._make_swarm(name="mirror-low", workers=low_workers)
        high_swarm = self._dbfs._make_swarm(name="mirror-high")
        response_queue = asyncio.Queue()

        entries = 1 # those are the entries seen so far as reported by find
        active_transfers = {}

        if update_cb:
            def update(entry):
                return update_cb(entry=entry, max_entries=entries, done=entries-len(active_transfers))
        else:
            async def update(_):
                pass

        def update_after_transfer(relpath, ex):
            entry = active_transfers.pop(relpath)
            entry.ex = ex
            if ex is not None:
                logging.debug(f"Exception caught during file transfer: {ex}")
            return update(entry) # returns a future

        async def empty_response_queue():
            try:
                while True:
                    (relpath, _, ex) = response_queue.get_nowait()
                    await update_after_transfer(relpath, ex)
            except asyncio.QueueEmpty:
                return

        async def find_cb(entry, max_entries, done):
            nonlocal entries

            # We empty the response_queue everytime this function is called.
            await empty_response_queue()

            fi = entry.fi
            relpath = fi.relpath(src)
            target_path = self.resolve_target(target, relpath)
            entries = max_entries
            if fi.is_dir():
                if entry.good:
                    logging.debug(f"making dir for {relpath}")
                    try:
                        await self.mkdirs(session, target_path)
                    except Exception as ex:
                        logging.warn(f"{relpath}: mkdirs failed. {ex}")
                        entry.ex = ex
                else:
                    logging.debug(f"discarding dir {relpath}")
            else:
                if sync and entry.good and not await self.needs_sync(session, fi, target_path):
                    logging.debug(f"file {relpath} doesn't need synchronization")
                    entry.good = False
                if entry.good:
                    logging.debug(f"queueing download of {relpath}")
                    await high_swarm.put(self.transfer,
                                         task_key=relpath,
                                         response_queue=response_queue,
                                         low_swarm=low_swarm,
                                         src=fi.abspath(), target=target_path,
                                         overwrite=overwrite)
                    active_transfers[relpath] = entry
                    return # don't report the entry yet!
                else:
                    logging.debug(f"discarding file {relpath}")

            await update(entry)

        # This code is a bit hairy because we run the control code and
        # both swarms concurrently:
        async def control():
            await high_swarm.run_while(self.find(session, src,
                                                 update_cb=find_cb,
                                                 filter_cb=filter_cb,
                                                 predicates=predicates))

            while active_transfers:
                logging.debug(f"mirror control, {len(active_transfers)} transfers currently active")
                (relpath, _, ex) = await response_queue.get()
                await update_after_transfer(relpath, ex)

        await low_swarm.run_while(control())

class RGetter(Mirror):
    def needs_low_swarm(self):
        return True

    def resolve_src(self, path, *more):
        return self._dbfs._resolve(path, *more)

    def resolve_target(self, path, *more):
        return self._dbfs._resolve_local(path, *more)

    def find(self, *args, **kwargs): # returns future
        return self._dbfs._async_find(*args, **kwargs)

    def transfer(self, *args, **kwargs): # returns future
        return self._dbfs._async_get(*args, **kwargs, mkdirs=True)

    async def needs_sync(self, session, fi, target_path):
        """Checks wheter the file at local_path is older or has a different
        size than the one in fi.
        """
        try:
            st = os.stat(target_path)
            logging.debug(f"comparing file {local_path} with stats {st} with fi {fi}")
            if st.st_size != fi.size():
                return True
            if st.st_mtime_ns  < fi.mtime() * 1000000:
                return True
            return False
        except:
            return True

    async def mkdirs(self, session, target_path):
        fastdbfs.util.mkdirs(target_path)

class RPutter(Mirror):
    def resolve_src(self, path, *more):
        return self._dbfs._resolve_local(path, *more)

    def resolve_target(self, path, *more):
        return self._dbfs._resolve(path, *more)

    async def find(self, session, path, update_cb, filter_cb=None, predicates={}):
        path = self.resolve_src(path)
        logging.debug(f"root path: {path}")
        done = 0
        entries_found = 1

        async def update(paths):
            nonlocal done
            entries = [WalkerEntry(FileInfoLocal.from_path(p)) for p in paths]
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

            for entry in entries:
                done += 1
                await update_cb(entry=entry, max_entries=entries_found, done=done)

        await update([path])

        for (dir, subdirs, filenames) in os.walk(path):
            all = subdirs + filenames
            logging.debug(f"dirs and files returned by os.walk: {all}")
            entries_found += len(all)
            await update([os.path.join(dir, fn) for fn in all])

        return done

    def transfer(self, *args, low_swarm=None, **kwargs): # returns future
        return self._dbfs._async_put(*args, **kwargs, mkdirs=True)

    async def needs_sync(self, session, fi, target_path):
        logging.debug(f"entering needs_sync {fi}, {target_path}")
        try:
            target_fi = await self._dbfs._async_get_status(session, target_path)
            logging.debug(f"target size: {target_fi.size()} src size: {fi.size()}")
            if target_fi.size() != fi.size():
                return True

            logging.debug(f"target mtime: {target_fi.mtime()} src mtime: {fi.mtime()}")
            if target_fi.mtime() < fi.mtime():
                return True
            return False
        except:
            return True

    def mkdirs(self, session, target_path):
        return self._dbfs._async_mkdir(session, target_path)
