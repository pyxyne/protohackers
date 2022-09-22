import socket
import select
from typing import Type
from lib_color import *


PORT = 50_000
BUF_SIZE = 4096


client_count = 0
def get_client_name():
	global client_count
	client_count += 1
	return f"client_{client_count}"


class ByteClient:
	def __init__(self, sock: socket.socket):
		self._sock = sock
		self._buffer = bytearray()
		self._closed = False
		self.name = get_client_name()
	
	def on_open(self): pass
	
	def chunk(self, buf: bytes) -> int:
		return len(buf)
	
	def on_bytes(self, buf: bytes): pass
	
	def on_eof(self): pass
	
	def _recv(self):
		buf = self._sock.recv(BUF_SIZE)
		if len(buf) == 0:
			self.on_eof()
			if len(self._buffer) > 0:
				self.warn("Unfinished message:", bytes(self._buffer))
			self.close()
			return
		
		self._buffer.extend(buf)
		while len(self._buffer) > 0:
			msg_len = self.chunk(bytes(self._buffer))
			if msg_len < 0: break
			msg, self._buffer = bytes(self._buffer[:msg_len]), self._buffer[msg_len:]
			self.on_bytes(msg)
			if self._closed:
				self._buffer.clear()
				return
	
	def log(self, *args):
		msg = " ".join(map(str, args))
		print(f"{DIM_WHITE}{self.name}{RESET} {msg}{RESET}")
	
	def warn(self, *args):
		self.log(f"{YELLOW}{args[0]}{RESET}", *args[1:])
	
	def send_bytes(self, buf):
		if self._closed:
			self.warn("Ignored write:", repr(buf))
			return
		self._sock.sendall(buf)
	
	def send_str(self, s):
		self.send_bytes(s.encode("utf-8"))
	def send_line(self, s):
		self.send_str(s + "\n")
	
	def close(self):
		if self._closed: return
		self._closed = True
		self._sock.shutdown(socket.SHUT_RDWR)
		self._sock.close()


class LineClient(ByteClient):
	def chunk(self, buf):
		i = buf.find(b"\n")
		return i + 1 if i != -1 else -1
	
	def on_bytes(self, buf: bytes):
		try:
			line = buf[:-1].decode(encoding="utf-8")
		except UnicodeDecodeError:
			self.warn("Received invalid UTF-8:", buf)
		else:
			self.on_line(line)
	
	def on_line(self, line: str): pass


def serve(Client: Type[ByteClient]):
	assert issubclass(Client, ByteClient)
	
	clients: dict[int, Client] = {}
	poller = select.poll()
	
	try:
		addrs = socket.getaddrinfo(None, PORT, family=socket.AF_INET6,
			type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)
		if len(addrs) < 0:
			raise OSError("No usable address, IPv6 may not be supported")
		(family, sock_type, proto, _, addr) = addrs[0]
		listener = socket.socket(family, sock_type, proto)
		listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
		listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		listener.bind(addr)
		listener.listen(5)
		poller.register(listener, select.POLLIN)
		
	except OSError as e:
		print(f"{BRIGHT_RED}Could not start server on port {PORT}{RESET}:")
		print(e)
		exit(1)
	
	print(f"{BRIGHT_GREEN}Listening for connections on port {PORT}{RESET}")
	
	should_stop = False
	while not should_stop:
		try:
			events = poller.poll()
		except KeyboardInterrupt:
			print()
			break
		
		for fd, event in events:
			if fd == listener.fileno():
				if event & select.POLLIN:
					sock, (host, port, _, _) = listener.accept()
					
					client = Client(sock)
					clients[sock.fileno()] = client
					poller.register(sock, select.POLLIN)
					
					if host.startswith("::ffff:"): # ipv4
						addr = f"{host[7:]}:{port}"
					else: # ipv6
						addr = f"[{host}]:{port}"
					client.log(f"{CYAN}New connection from {addr}")
					
					client.on_open()
				
				if event & select.POLLERR:
					print(f"{BRIGHT_RED}Listening socket errorred{RESET}")
					should_stop = True
					break
			
			else:
				client = clients[fd]
				
				if event & select.POLLIN:
					client._recv()
				
				if event & select.POLLERR:
					client.log(f"{RED}Socket errorred")
					client.close()
				
				if client._closed:
					del clients[fd]
					poller.unregister(fd)
					client.log(f"{MAGENTA}Connection closed")
	
	print(f"{BRIGHT_MAGENTA}Stopping server{RESET}")
	for client in list(clients.values()):
		client.close()
	listener.close()
