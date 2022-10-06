import socket
from lib_server import PORT, BUF_SIZE
from lib_color import *
	
values: dict[str, str] = {}

try:
	addrs = socket.getaddrinfo(None, PORT, family=socket.AF_INET6,
		type=socket.SOCK_DGRAM, flags=socket.AI_PASSIVE)
	if len(addrs) < 0:
		raise OSError("No usable address, IPv6 may not be supported")
	(family, sock_type, proto, _, addr) = addrs[0]
	listener = socket.socket(family, sock_type, proto)
	listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
	listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	listener.bind(addr)
	
except OSError as e:
	print(f"{BRIGHT_RED}Could not start server on port {PORT}{RESET}:")
	print(e)
	exit(1)

print(f"{BRIGHT_GREEN}Listening for connections on port {PORT}{RESET}")

while True:
	try:
		(req, addr) = listener.recvfrom(BUF_SIZE)
	except KeyboardInterrupt:
		print()
		break
	
	req = req.decode("utf-8")
	parts = req.split("=", maxsplit=1)
	if len(parts) >= 2: # set
		key = parts[0]
		if key == "version":
			print("ignore version set")
		else:
			val = "=".join(parts[1:])
			print("set", repr(key), repr(val))
			values[key] = val
	else: # get
		key = req
		print("get", repr(key))
		if key == "version":
			val = "pyxyne's db"
		elif key in values:
			val = values[key]
		else:
			val = ""
		listener.sendto(f"{key}={val}".encode("utf-8"), addr)

print(f"{BRIGHT_MAGENTA}Stopping server{RESET}")
listener.close()