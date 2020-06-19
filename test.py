import sideserver

if __name__ == '__main__':
    server = sideserver.SideServer()
    server.start()

    while True:
        msg = server.receive()
        content = msg.json()
        server.reply(msg, json=[content, content])

    server.shutdown()
