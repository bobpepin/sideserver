import uvicorn
import asyncio
import concurrent.futures
import signal
import itertools
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import Response, PlainTextResponse


async def connect_blocking_to_async(blocking_receive, async_send):
    loop = asyncio.get_running_loop()
    def worker():
        while True:
#             print(f"Receiving from {blocking_receive!r}")
            val = blocking_receive()
#             print(f"Sending to {async_send!r}")
            coro = async_send(val)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            result = future.result()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        result = await loop.run_in_executor(pool, worker)
        return result

async def handle_request(request):
    app = request.app
    body = await request.body()
    msg_id = next(app.state.id_generator)
    msg = {"id": msg_id, "body": body}
    if app.state.master_request_queue:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            await loop.run_in_executor(pool, lambda: app.state.master_request_queue.put(msg))
    resp_queue = asyncio.Queue(1)
    app.state.response_queues[msg_id] = resp_queue
    resp = await resp_queue.get()
    del app.state.response_queues[msg_id]
    return PlainTextResponse(f"{resp}")

async def handle_messages(app, recv_queue):
    while True:
#         print("Waiting for message")
        msg = await recv_queue.get()
        msg_id = msg.get("id")
        resp_queue = app.state.response_queues.get(msg_id)
        if resp_queue:
            await resp_queue.put(msg)

async def lifespan(app, request_queue=None, response_queue=None):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    master_response_queue = response_queue
    app.state.master_request_queue = request_queue
    app.state.master_response_queue = response_queue
    app.state.response_queues = {}
    app.state.id_generator = itertools.count()
    response_queue = asyncio.Queue()
    app.state.response_queue = response_queue
    recv_task = asyncio.create_task(
        connect_blocking_to_async(master_response_queue.get, response_queue.put)
    )
    handle_task = asyncio.create_task(handle_messages(app, response_queue))

    yield

    if not handle_task.done():
        handle_task.cancel()
    if not recv_task.done():
        recv_task.cancel()
    await asyncio.gather(handle_task, recv_task, return_exceptions=True)

async def startup(app, **kwargs):
    ls = lifespan(app, **kwargs)
    app.state.lifespan = ls
    await ls.__anext__()
    
async def shutdown(app):
    try:
        await app.state.lifespan.__anext__()
    except StopAsyncIteration:
        pass

def run(**kwargs):
    routes = [Route("/{path:path}", handle_request, methods=["GET", "POST"])]
    app = Starlette(debug=True, routes=routes)
    async def on_startup():
        await startup(app, **kwargs)
    async def on_shutdown():
        await shutdown(app)
    app.add_event_handler("startup", on_startup)
    app.add_event_handler("shutdown", on_shutdown)
    uvicorn.run(app, port=4000, loop="asyncio", lifespan="on")
    