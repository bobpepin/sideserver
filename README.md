SideServer
===========

SideServer is a sidecar http server in a python module. Works by spawning a
subprocess running a http server (uvicorn+starlette). Http requests are sent to the master process
using pipes and then enqueued. Works well with Jupyter notebooks, not so much
when running on the command line (requires `if __name__ == "__main__"` and
doesn't deal well with failure).

Consider rewriting using threads instead of subprocesses. Drawback is GIL, advantage that it works better on windows. Could also use fork for unix and threads for windows.

Example usage:
```
import sideserver

if __name__ == '__main__':
    server = sideserver.SideServer()
    server.start()

    while True:
        msg = server.receive()
        content = msg.json()
        server.reply(msg, json=[content, content])

    server.shutdown()
```
