import socket, json

s = socket.create_connection(("192.168.1.140", 8098))
buf = b""
while True:
    buf += s.recv(4096)
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        if not line:
            continue
        msg = json.loads(line.decode("utf-8"))
        print(msg)   # {"ts":..., "left":..., "right":...}
