import socket
import select

PORT = 50_000
BUF_SIZE = 1024

listener = socket.create_server(("", PORT),
	family=socket.AF_INET6, dualstack_ipv6=True)
listener.listen(5)
print(f"Listening for connections on port {PORT}")

socks_by_fd = { listener.fileno(): listener }
addrs_by_fd = {}
poller = select.poll()
poller.register(listener, select.POLLIN)

stop_server = False
while not stop_server:
	try:
		events = poller.poll()
	except KeyboardInterrupt:
		break
	
	for (fd, event) in events:
		sock = socks_by_fd[fd]
		reached_eof = False
		if event & select.POLLIN:
			if sock == listener:
				(new_sock, (host, port, _, _)) = listener.accept()
				if host.startswith("::ffff:"): # ipv4
					addr = f"{host[7:]}:{port}"
				else: # ipv6
					addr = f"{host}:{port}"
				print(f"Accepted connection from {addr}")
				poller.register(new_sock, select.POLLIN)
				new_fd = new_sock.fileno()
				socks_by_fd[new_fd] = new_sock
				addrs_by_fd[new_fd] = addr
			else:
				total_read = 0
				while True:
					buffer = sock.recv(BUF_SIZE)
					if len(buffer) > 0:
						total_read += len(buffer)
						sock.sendall(buffer)
					if len(buffer) < BUF_SIZE:
						break
				if total_read == 0: # eof
					reached_eof = True
				else:
					print(f"[{addrs_by_fd[fd]}] Bounced {total_read} bytes")
		if reached_eof or (event & select.POLLERR):
			if sock != listener:
				print(f"[{addrs_by_fd[fd]}] ", end="")
			if reached_eof:
				print("Socket closed")
			elif event & select.POLLERR:
				print("Socket errorred")
			if sock == listener:
				stop_server = True
				break
			poller.unregister(sock)
			sock.close()
			del socks_by_fd[fd]
			del addrs_by_fd[fd]

print("Stopping server")
for sock in socks_by_fd.values():
	poller.unregister(sock)
	sock.close()
