import socket
import select
from lib_color import *

class ByteConn:
	def __init__(self, server, sock, fd, addr):
		self.server = server
		self.sock = sock
		self.fd = fd
		self.addr = addr
		self.closed = False
	
	def ready(self):
		pass
	
	def got_bytes(self, buf):
		pass
	
	def got_eof(self):
		pass
	
	def send_bytes(self, buf):
		assert not self.closed
		self.sock.sendall(buf)
	
	def send_str(self, s):
		assert not self.closed
		self.sock.sendall(s.encode("utf-8"))
	
	def log(self, *args):
		msg = " ".join(map(str, args))
		print(f"{DIM_WHITE}{self.addr}{RESET} {msg}{RESET}")
	
	def close(self, skip_shutdown=False):
		if self.closed: return
		self.closed = True
		if not skip_shutdown:
			self.sock.shutdown(socket.SHUT_RDWR)
		self.server.poller.unregister(self.sock)
		self.sock.close()
		del self.server.conns[self.fd]

class LineConn(ByteConn):
	def __init__(self, *args):
		super().__init__(*args)
		self.buffer = bytearray()
	
	def got_line(self, line):
		pass
	
	def got_bytes(self, buf):
		i, j = 0, buf.find(b"\n")
		while j != -1:
			self.buffer.extend(buf[i:j])
			try:
				line = self.buffer.decode()
			except UnicodeDecodeError:
				self.log(f"{YELLOW}Line could not be decoded as UTF-8")
			else:
				self.got_line(line)
			self.buffer = bytearray()
			i, j = j+1, buf.find(b"\n", j+1)
		self.buffer.extend(buf[i:])
	
	def got_eof(self):
		if len(self.buffer) > 0:
			self.got_line(self.buffer.decode())

class TcpServer:
	def __init__(self, Conn, port=50_000, buf_size=4096):
		self.port = port
		self.buf_size = buf_size
		self.Conn = Conn
		
		self.conns = {}
		self.poller = select.poll()
		self.should_stop = False
	
	def listen(self):
		try:
			addrs = socket.getaddrinfo(None, self.port, family=socket.AF_INET6,
				type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)
			if len(addrs) < 0:
				raise OSError("No usable address; IPv6 may not be supported.")
			(family, sock_type, proto, _, addr) = addrs[0]
			self.listener = socket.socket(family, sock_type, proto)
			self.listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
			self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			self.listener.bind(addr)
			self.listener.listen(5)
			
		except OSError as e:
			print(f"{BRIGHT_RED}Could not start server on port {self.port}{RESET}:")
			print(e)
			exit(1)
		
		self.listener_fd = self.listener.fileno()
		self.poller.register(self.listener, select.POLLIN)
		
		print(f"{BRIGHT_GREEN}Listening for connections on port {self.port}{RESET}")
		
		while not self.should_stop:
			try:
				events = self.poller.poll()
			except KeyboardInterrupt:
				self.should_stop = True
				break
			
			for (fd, event) in events:
				if fd == self.listener_fd:
					if event & select.POLLIN:
						(new_sock, (host, port, _, _)) = self.listener.accept()
						if host.startswith("::ffff:"): # ipv4
							addr = f"{host[7:]}:{port}"
						else: # ipv6
							addr = f"[{host}]:{port}"
						
						self.poller.register(new_sock, select.POLLIN)
						new_fd = new_sock.fileno()
						conn = self.Conn(self, new_sock, new_fd, addr)
						self.conns[new_fd] = conn
						
						conn.log(f"{CYAN}Connection opened")
						conn.ready()
					
					if event & select.POLLERR:
						print(f"{BRIGHT_RED}Server errorred{RESET}")
						self.should_stop = True
				
				else:
					conn = self.conns[fd]
					
					reached_eof = False
					if event & select.POLLIN:
						buffer = conn.sock.recv(self.buf_size)
						if len(buffer) > 0:
							conn.got_bytes(buffer)
						else:
							conn.got_eof()
							conn.close()
							conn.log(f"{MAGENTA}Connection closed")
					
					if event & select.POLLERR:
						conn.log(f"{RED}Socket errorred")
						conn.close(skip_shutdown=True)
		
		print(f"{BRIGHT_MAGENTA}Stopping server{RESET}")
		for conn in list(self.conns.values()):
			conn.close()
		self.listener.close()
