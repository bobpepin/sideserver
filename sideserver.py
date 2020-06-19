import logging

logger = logging.getLogger("sideserver")

# uvicorn notes:
# Access log LogRecord contains non-picklable in scope
# Can we document the Config() and serve()?
# Method to trigger shutdown?
# Set up signal handlers outside of serve(), same place as the reloader?
# disable sys.exit(1) ?
# Detect whether startup successful or not / event on successful bind


### Main
import multiprocessing, multiprocessing.managers
import re
import uvicorn
import asyncio
import os
import signal
import logging
import json
from collections import deque

import json as json_
class Request:
    def __init__(self, id, scope, body):
        self.id = id
        self.scope = scope
        self.body = body
    
    def json(self):
        return json.loads(self.body)
    
    
class StartupError (Exception):
    pass


class SideServer:
    
    def __init__(self, logger=None):
        self.logger = logger        
        self.request_main = self.request_side = None
        self.response_main = self.response_side = None
        self.process = None
        self.requests = deque()
        self.logging = []
        
    def start(self):
        ctx = multiprocessing.get_context("spawn")
        self.request_main, self.request_side = multiprocessing.Pipe(False)
        self.response_side, self.response_main = multiprocessing.Pipe(False)
        process = ctx.Process(
            target=run,
            kwargs={"request_conn": self.request_side,
                    "response_conn": self.response_side,
                    "close_conns": [self.request_main, self.response_main]}
        )
        process.daemon = True
        process.start()
        self.process = process
        self.request_side.close()
        self.response_side.close()
        self.wait_for_startup()

    def shutdown(self):
        self.flush()
        if self.process is not None:
            self.process.terminate()
        self.flush()
        self.request_main = self.request_side = None
        self.response_main = self.response_side = None
        
    def __del__(self):
        self.request_main.close()
        self.response_main.close()
        if self.process is not None:
            self.process.terminate()

    def process_msg(self, msg):
        if isinstance(msg, logging.LogRecord):
            self.logging.append(msg)
            if self.logger:
                self.logger.handle(msg)
        else:
            self.requests.append(msg)
        
    def flush(self, blocking=False):
        while self.request_main.poll():
            msg = self.request_main.recv()
            self.process_msg(msg)
            
        if blocking:
            while len(self.requests) == 0:
                msg = self.request_main.recv()
                self.process_msg(msg)
    
    def wait_for_startup(self, timeout=5):
        while self.request_main.poll(timeout):
            msg = self.request_main.recv()
            self.process_msg(msg)
            if isinstance(msg, logging.LogRecord): 
                if (msg.name == "sideserver" and 
                    re.search(r"completed with exit code 1$", msg.msg)):
                    raise StartupError(self.logging)
                elif (msg.name == "uvicorn.error" and
                     re.match(r"Uvicorn running on ", msg.msg)):
                    return
        raise StartupError(self.logging)

    def receive(self):
        self.flush(blocking=True)
        raw_req = self.requests.popleft()
        request = Request(raw_req["id"], raw_req["scope"], raw_req["body"])
        return request
    
    def reply(self, request, data=None, media_type=None, json=None):
        if json is not None:
            body = json_.dumps(json).encode("utf-8")
            media_type = "application/json"
        elif data is not None:
            body = data
            
        resp = {"id": request.id, "body": body, "media_type": media_type}
        self.response_main.send(resp)


### Sidecar
import uvicorn
import asyncio
import concurrent.futures
import signal
import itertools
import traceback
import logging
import pickle
from collections import namedtuple
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import Response, PlainTextResponse


class AsyncQueueHandler (logging.handlers.QueueHandler):
    def enqueue(self, record):
#         print(f"enqueue {record!r}")
        record.scope = None
        asyncio.create_task(self.queue.put(record))
    

def setup_logging(request_queue):
    queue_handler = AsyncQueueHandler(request_queue)
    logger.addHandler(queue_handler)
    uvicorn_logger = logging.getLogger("uvicorn")
    uvicorn_logger.addHandler(queue_handler)
    access_logger = logging.getLogger("uvicorn.access")
    access_logger.addHandler(queue_handler)


async def forward_blocking_to_async(blocking_receive, async_send):
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
        await loop.run_in_executor(pool, worker)
            

async def forward_async_to_blocking(async_receive, blocking_send, task_done=None):
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        while True:
            val = await async_receive()
            await loop.run_in_executor(pool, lambda: blocking_send(val))
            if task_done is not None:
                task_done()


async def handle_request(request):
    app = request.app
    body = await request.body()
    msg_id = next(app.state.id_generator)
    scope_keys = [
        'type',
        'http_version',
        'server',
        'client',
        'scheme',
        'method',
        'root_path',
        'path',
        'raw_path',
        'query_string',
        'headers',
    ]
    scope_data = {k: request.scope[k] for k in scope_keys}
    msg = {"id": msg_id, "body": body, "scope": scope_data}
    resp_queue = asyncio.Queue(1)
    app.state.response_queues[msg_id] = resp_queue
    await app.state.request_queue.put(msg)
    resp = await resp_queue.get()
    del app.state.response_queues[msg_id]
    return Response(resp["body"], media_type=resp.get("media_type"))


async def dispatch_responses(recv_queue, response_queues):
    while True:
#         print("Waiting for message")
        msg = await recv_queue.get()
        msg_id = msg.get("id")
        resp_queue = response_queues.get(msg_id)
        if resp_queue is not None:
            await resp_queue.put(msg)


def build_app(*args, request_queue=None, response_queue=None, response_queues=None, **kwargs):
    def on_startup():
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
    routes = [Route("/{path:path}", handle_request, methods=["GET", "POST"])]        
    app = Starlette(*args, 
                    routes=routes, 
                    on_startup=[on_startup],
                    **kwargs)

    app.state.request_queue = request_queue
    app.state.response_queue = response_queue
    app.state.response_queues = response_queues
    app.state.id_generator = itertools.count()

    return app


async def run1(host="127.0.0.1", port=4000,
               close_conns=[], request_conn=None, response_conn=None):

    # Make sure that the main process is the only one with these endpoints open. 
    # This is so that we can correctly detect when they are closed in the main process.
    for conn in close_conns:
        conn.close()
    
    request_queue = asyncio.Queue()
    response_queue = asyncio.Queue()
    response_queues = {}
    
    def send_request(val):
        print(f"Sending {val!r} to {request_conn!r}")
        buf = pickle.dumps(val)
        request_conn.send_bytes(buf)
    
    send_task = asyncio.create_task(
        forward_async_to_blocking(request_queue.get, 
                                  request_conn.send, 
                                  task_done=request_queue.task_done)
#         forward_async_to_blocking(request_queue.get, send_request)        
    )
    recv_task = asyncio.create_task(
        forward_blocking_to_async(response_conn.recv, response_queue.put)
    )
    dispatch_task = asyncio.create_task(dispatch_responses(response_queue, response_queues))

    helper_tasks = {send_task, recv_task, dispatch_task}
    
    app = build_app(
        request_queue=request_queue, 
        response_queue=response_queue,
        response_queues=response_queues,
        debug=True)
    
    config = uvicorn.config.Config(app, host=host, port=port, lifespan="on")
    server = uvicorn.Server(config=config)

    setup_logging(request_queue)
    
    async def run_server():
        try:
            await server.serve()
            return 0
        except SystemExit as exc:
            return exc.code
    
#     server_task = asyncio.create_task(server.serve())
    server_task = asyncio.create_task(run_server())

    tasks = {server_task, *helper_tasks}
    
#     print(f"Running {tasks!r}")
    
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    
    print(f"Running tasks: {asyncio.all_tasks()!r}")
    
    for task in done:
#         print(f"Task {task!r} finished")
        try:
            result = task.result()
            msg = f"Task {task!r} completed with exit code {result!r}"
            if result == 0:
                logger.info(msg)
            else:
                logger.error(msg)
        except Exception:
            exc_fmt = traceback.format_exc()
            msg = f"Task {task!r} terminated with exception:\n{exc_fmt}"
            logger.error(msg)

    if server_task not in done:
        server.should_exit = True
        done, pending = await asyncio.wait({server_task}, timeout=5)
        if server_task not in done:
            server.force_exit = True
            done, pending = await asyncio.wait({server_task}, timeout=5)
            if server_task not in done:
                server_task.cancel()

    await asyncio.wait({request_queue.join()}, timeout=5)
    
    for task in tasks:
        task.cancel()

    await asyncio.wait(tasks, timeout=5)
    
    response_conn.close()
    request_conn.close()            

    import sys
    sys.exit(0)
#     loop = asyncio.get_running_loop()
#     loop.stop()
#     loop.close()
    
def run(*args, **kwargs):
    return asyncio.run(
        run1(*args, **kwargs)
    )
