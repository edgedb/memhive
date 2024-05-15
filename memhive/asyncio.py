import asyncio
import threading

from . import memhive
from . import core


class ListenerProxy:

    def __init__(self, hub):
        self._hub = hub
        self._messages = None
        self._listener_thread = None

    async def listen(self):
        try:
            while True:
                self._hub.process_refs()
                msg = await self._messages.get()
                if msg[0] == 'e':
                    raise msg[1]
                else:
                    yield msg[1]
        except core.ClosedQueueError:
            self.join()
            raise

    def start(self):
        self._messages = asyncio.Queue()
        self._listener_thread = threading.Thread(
            target=self._receiver, args=(asyncio.get_running_loop(),))
        self._listener_thread.start()

    def join(self):
        if self._listener_thread is not None:
            self._listener_thread.join()
            self._listener_thread = None

    def _receiver(self, loop):
        going = True
        while going:
            try:
                msg = ('d', self._hub.listen())
            except core.ClosedQueueError as ex:
                msg = ('e', ex)
                going = False
            except Exception as ex:
                msg = ('e', ex)

            loop.call_soon_threadsafe(
                self._messages.put_nowait,
                msg
            )


class AsyncMemHive:

    def __init__(self):
        if hasattr(core, 'enable_object_tracking'):
            core.enable_object_tracking()

        self._hive = memhive.MemHive()
        self._active = False
        self._listen_proxy = ListenerProxy(self._hive)

    def _ensure_active(self):
        if not self._active:
            raise RuntimeError("AsyncMemHive hasn't been activated")

    def __getitem__(self, key):
        self._ensure_active()
        return self._hive._mem[key]

    def __contains__(self, key):
        self._ensure_active()
        return key in self._hive._mem

    def __setitem__(self, key, val):
        self._ensure_active()
        self._hive._mem[key] = val

    def add_worker(self, **kwargs):
        self._hive.add_worker(**kwargs)

    def add_async_worker(self, *, setup=None, main):
        def new_main(sub, main_code=main.__code__, main_name=main.__name__):
            import asyncio
            import types
            main = types.FunctionType(main_code, globals(), main_name)

            from memhive.asyncio import AsyncMemSub

            async def main_wrapper():
                async with AsyncMemSub(sub) as new_sub:
                    await main(new_sub)

            asyncio.run(main_wrapper())

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
        self._listen_proxy.start()

        return self

    async def __aexit__(self, *e):
        self._hive.close()
        self._listen_proxy.join()

    async def listen(self):
        self._ensure_active()
        async for msg in self._listen_proxy.listen():
            yield msg


class AsyncMemSub:

    def __init__(self, sub):
        self._sub = sub
        self._listen_proxy = ListenerProxy(self._sub)
        self._active = False

    def _ensure_active(self):
        if not self._active:
            raise RuntimeError("AsyncMemHive hasn't been activated")

    def __getitem__(self, key):
        self._ensure_active()
        return self._sub[key]

    def __contains__(self, key):
        self._ensure_active()
        return key in self._sub

    def request(self, arg):
        self._sub.request(arg)

    async def __aenter__(self):
        self._active = True
        self._listen_proxy.start()
        return self

    async def __aexit__(self, *e):
        self._sub.close()
        self._active = False
        self._listen_proxy.join()

    async def listen(self):
        self._ensure_active()
        async for msg in self._listen_proxy.listen():
            yield msg
