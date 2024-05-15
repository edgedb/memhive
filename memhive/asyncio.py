import asyncio
import inspect
import threading

from . import memhive


class AsyncMemHive:

    def __init__(self):
        self._hive = memhive.MemHive()
        self._active = False
        self._listening = False
        self._messages = None

    def _ensure_active(self):
        if not self._active:
            raise RuntimeError("AsyncMemHive hasn't been activated")

    def __getitem__(self, key):
        self._ensure_active()
        return self._hive[key]

    def __contains__(self, key):
        self._ensure_active()
        return key in self._m_hiveem

    def __setitem__(self, key, val):
        self._ensure_active()
        self._hive[key] = val

    def add_worker(self, **kwargs):
        self._hive.add_worker(**kwargs)

    def add_async_worker(self, *, setup=None, main):
        def new_main(sub, main_code=main.__code__, main_name=main.__name__):
            import asyncio
            import types
            main = types.FunctionType(main_code, globals(), main_name)
            asyncio.run(main(sub))

        self.add_worker(setup=setup, main=new_main)

    def push(self, *args):
        self._hive.push(*args)

    def broadcast(self, *args):
        self._hive.broadcast(*args)

    async def __aenter__(self):
        if self._active:
            raise RuntimeError('already running')

        self._hive.__enter__() # XXX: obviously, fix it.

        self._active = True
        self._messages = asyncio.Queue()

        self._listener_thread = threading.Thread(
            target=self._receiver, args=(asyncio.get_running_loop(),))
        self._listener_thread.start()

        return self

    async def __aexit__(self, *e):
        self._hive.close()
        self._listener_thread.join()

    async def listen(self):
        while True:
            msg = await self._messages.get()
            yield msg

    def _receiver(self, loop):
        while True:
            msg = self._hive.listen()
            loop.call_soon_threadsafe(
                self._messages.put_nowait,
                msg
            )
