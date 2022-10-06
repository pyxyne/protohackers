import socket
import select
import time
from typing import Any, Tuple, Type
from lib_color import *


PORT = 50_000
BUF_SIZE = 4096 # bytes
UDP_TIMEOUT = 1 # seconds


start_time = time.monotonic()
def get_timestamp():
	return f"{time.monotonic() - start_time: >6.2f}s"

def log(s):
	print(f"{DIM_WHITE}{get_timestamp()}{RESET} {s}{RESET}")

client_count = 0
def get_client_name():
	global client_count
	client_count += 1
	return f"c{client_count:02d}"

Ipv6Addr = Tuple[str, int, Any, Any]
def get_addr_str(addr: Ipv6Addr):
	(host, port, _, _) = addr
	if host.startswith("::ffff:"): # ipv4
		return f"{host[7:]}:{port}"
	else: # ipv6
		return f"[{host}]:{port}"


class Conn:
	def send(self, buf: bytes):
		raise NotImplementedError
	def close(self):
		raise NotImplementedError

class TcpConn(Conn):
	def __init__(self, sock: socket.socket):
		self.sock = sock
	
	def send(self, buf):
		self.sock.sendall(buf)
	
	def close(self):
		self.sock.shutdown(socket.SHUT_RDWR)
		self.sock.close()

class UdpConn(Conn):
	def __init__(self, sock: socket.socket, addr: Ipv6Addr):
		self.sock = sock
		self.addr = addr
		self.update_timeout()
	
	def update_timeout(self):
		self.timeout = time.monotonic() + UDP_TIMEOUT
	
	def send(self, buf):
		while True:
			sent = self.sock.sendto(buf, self.addr)
			if sent == len(buf): break
			buf = buf[sent:]
	
	def close(self):
		pass


class ByteClient:
	def __init__(self, conn: Conn):
		self.conn = conn
		self.buffer = bytearray()
		self.closed = False
		self.name = get_client_name()
	
	def on_open(self): pass
	
	def open(self, addr: Ipv6Addr):
		self.log(f"{CYAN}New connection from {get_addr_str(addr)}")
		self.on_open()
	
	def chunk(self, buf: bytes) -> int:
		return len(buf)
	
	def on_bytes(self, buf: bytes): pass
	
	def on_eof(self): pass
	
	def eof(self):
		self.on_eof()
		if len(self.buffer) > 0:
			self.warn("Unfinished message:", bytes(self.buffer))
		self.close()
	
	def recv(self, buf: bytes):
		self.buffer.extend(buf)
		while True:
			msg_len = self.chunk(bytes(self.buffer))
			if msg_len < 0: break
			msg, self.buffer = bytes(self.buffer[:msg_len]), self.buffer[msg_len:]
			self.on_bytes(msg)
			if self.closed:
				self.buffer.clear()
			if len(self.buffer) == 0:
				break
	
	def log(self, *args):
		msg = " ".join(map(str, args))
		log(f"{DIM_WHITE}{self.name}{RESET} {msg}")
	
	def warn(self, *args):
		self.log(f"{YELLOW}{args[0]}{RESET}", *args[1:])
	
	def send_bytes(self, buf):
		if self.closed:
			self.warn("Ignored write:", repr(buf))
			return
		self.conn.send(buf)
	
	def send_str(self, s):
		self.send_bytes(s.encode("utf-8"))
	def send_line(self, s):
		self.send_str(s + "\n")
	
	def close(self):
		if self.closed: return
		self.closed = True
		self.conn.close()


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


def serve(Client: Type[ByteClient], udp=False):
	assert issubclass(Client, ByteClient)
	
	clients: dict[Ipv6Addr if udp else int, Client] = {}
	poller = select.poll()
	
	try:
		sock_type = socket.SOCK_DGRAM if udp else socket.SOCK_STREAM
		addrs = socket.getaddrinfo(None, PORT, family=socket.AF_INET6,
			type=sock_type, flags=socket.AI_PASSIVE)
		if len(addrs) < 0:
			raise OSError("No usable address, IPv6 may not be supported")
		(family, sock_type, proto, _, addr) = addrs[0]
		listener = socket.socket(family, sock_type, proto)
		listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
		listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		listener.bind(addr)
		if not udp:
			listener.listen(5)
		poller.register(listener, select.POLLIN)
		
	except OSError as e:
		print(f"{BRIGHT_RED}Could not start server on port {PORT}{RESET}:")
		print(e)
		exit(1)
	
	log(f"{BRIGHT_GREEN}Listening for connections on port {PORT}")
	
	should_stop = False
	while not should_stop:
		try:
			timeout = None
			if udp and len(clients) > 0:
				now = time.monotonic()
				next_timeout = min(client.conn.timeout for client in clients.values())
				timeout = (next_timeout - now) * 1000
			events = poller.poll(timeout)
		except KeyboardInterrupt:
			print()
			break
		
		for fd, event in events:
			if fd == listener.fileno(): # listening socket
				if event & select.POLLERR:
					log(f"{BRIGHT_RED}Listening socket errorred")
					should_stop = True
					break
				
				if event & select.POLLIN:
					if udp:
						(buf, addr) = listener.recvfrom(BUF_SIZE)
						
						if addr not in clients:
							conn = UdpConn(listener, addr)
							client = Client(conn)
							clients[addr] = client
							
							client.open(addr)
							
						else:
							client = clients[addr]
							client.conn.update_timeout()
						
						client.recv(buf)
						
						if client.closed:
							del clients[addr]
							client.log(f"{MAGENTA}Connection closed")
						
					else:
						sock, addr = listener.accept()
						conn = TcpConn(sock)
						client = Client(conn)
						clients[sock.fileno()] = client
						poller.register(sock, select.POLLIN)
						
						client.open(addr)
			
			else: # client socket (tcp only)
				assert not udp
				client = clients[fd]
				
				if event & select.POLLERR:
					client.log(f"{RED}Socket errorred")
					client.close()
				
				elif event & select.POLLIN:
					buf = client.conn.sock.recv(BUF_SIZE)
					if len(buf) == 0:
						client.eof()
					else:
						client.recv(buf)
				
				if client.closed:
					del clients[fd]
					poller.unregister(fd)
					client.log(f"{MAGENTA}Connection closed")
		
		if udp: # check for timeouts
			now = time.monotonic()
			for key, client in list(clients.items()):
				if client.conn.timeout <= now:
					del clients[key]
					client.close()
					client.log(f"{MAGENTA}Client timed out")
	
	log(f"{BRIGHT_MAGENTA}Stopping server")
	for client in list(clients.values()):
		client.close()
	listener.close()
