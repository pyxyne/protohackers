import asyncio
import asyncio.transports
from collections import deque
import signal
import socket
import time
from abc import ABC, abstractmethod
import traceback
from typing import Callable, Coroutine, Dict, Generic, Tuple, TypeVar

from lib_color import *


PORT = 50_000
TCP_BACKLOG = 5 # max queued connections
UDP_TIMEOUT = 1 # seconds


def listen_ip(sock_type: socket.SocketKind, port: int):
	try:
		addr_info = socket.getaddrinfo(None, port, family=socket.AF_INET6,
			type=sock_type, flags=socket.AI_PASSIVE)
		if len(addr_info) < 0:
			raise OSError("No usable network configuration, IPv6 may not be supported.")
		(family, sock_type, proto, _, addr) = addr_info[0]
		sock = socket.socket(family, sock_type, proto)
		
		# Explicitly enable dualstack IPv4/IPv6
		sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
		if sock_type == socket.SOCK_STREAM:
			# To avoid address reuse timeout when server crashes
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		sock.bind(addr)
		
	except OSError as e:
		print(f"{BRIGHT_RED}Could not create socket on port {port}{RESET}:")
		print(e)
		exit(1)
	
	return sock

Addr = Tuple[str, int, int, int]
def get_addr_str(addr: Addr):
	(host, port, _, _) = addr
	if host.startswith("::ffff:"): # ipv4
		return f"{host[7:]}:{port}"
	else: # ipv6
		return f"[{host}]:{port}"

start_time = time.monotonic()
def log(*args):
	msg = " ".join(map(str, args))
	timestamp = f"{time.monotonic() - start_time: >6.2f}s"
	print(f"{DIM_WHITE}{timestamp}{RESET} {msg}{RESET}")

def shorten(s):
	if len(s) > 50:
		return s[:50] + "..."
	else:
		return s

T = TypeVar("T")
class Stream(Generic[T]):
	queue: deque
	eof: bool
	next: asyncio.Future | None
	
	def __init__(self):
		self.queue = deque()
		self.eof = False
		self.next = None
	
	def put(self, x: T):
		assert not self.eof
		if self.next is None:
			self.queue.append(x)
		else:
			self.next.set_result(x)
			self.next = None
	
	def put_eof(self):
		self.eof = True
		if len(self.queue) == 0 and self.next is not None:
			self.next.set_exception(EOFError())
			self.next = None
	
	def put_back(self, x: T):
		if self.next is None:
			self.queue.appendleft(x)
		else:
			self.next.set_result(x)
			self.next = None
	
	async def get(self) -> T:
		assert self.next is None
		if len(self.queue) == 0:
			if self.eof:
				raise EOFError
			else:
				self.next = asyncio.get_running_loop().create_future()
				return await self.next
		else:
			return self.queue.popleft()


class Peer:
	server: "Server"
	addr: Addr
	id: int
	
	def __init__(self, server: "Server", addr: Addr):
		self.server = server
		self.addr = addr
		self.id = server.new_peer_id()
		self.log(f"{CYAN}New connection from {get_addr_str(addr)}")
	
	def log(self, *args):
		log(f"{DIM_WHITE}peer{self.id}{RESET}", *args)
	
	def warn(self, *args):
		self.log(f"{YELLOW}{args[0]}{RESET}", *args[1:])
	
	def end(self, *args):
		self.log(f"{MAGENTA}{args[0]}{RESET}", *args[1:])
	
	def disconnect(self):
		pass


PeerT = TypeVar("PeerT")
Handler = Callable[[PeerT], Coroutine]
class Server(Generic[PeerT], ABC):
	handler: Handler
	timeout: float | None
	last_peer_id: int
	tasks: set[asyncio.Task]
	should_stop: asyncio.Future
	
	def __init__(self, handler: Handler, timeout: float | None):
		self.handler = handler
		self.timeout = timeout
		self.last_peer_id = 0
		self.tasks = set()
	
	async def serve(self, port: int):
		await self.open(port)
		self.start_time = time.monotonic()
		log(f"{BRIGHT_GREEN}Listening for connections on port {port}")
		
		loop = asyncio.get_running_loop()
		self.should_stop = loop.create_future()
		def sigint_handler():
			print()
			self.stop()
		loop.add_signal_handler(signal.SIGINT, sigint_handler)
		await self.should_stop
		
		log(f"{BRIGHT_MAGENTA}Stopping server")
		for task in self.tasks:
			task.cancel()
		await self.close()
	
	def new_peer_id(self) -> int:
		self.last_peer_id += 1
		return self.last_peer_id
	
	def new_peer(self, peer: Peer):
		async def safe_handler():
			try:
				await self.handler(peer)
			except EOFError:
				peer.log(f"{YELLOW}Unexpected EOF")
			except Exception as err:
				peer.log(f"{BRIGHT_RED}Error:")
				print(f"{BRIGHT_RED}{traceback.format_exc().rstrip()}")
				self.stop()
			finally:
				peer.disconnect()
				self.remove_peer(peer)
		self.run(safe_handler())
	
	def run(self, co: Coroutine):
		task = asyncio.create_task(co)
		self.tasks.add(task)
		task.add_done_callback(self.tasks.discard)
	
	def stop(self):
		if not self.should_stop.done():
			self.should_stop.set_result(None)
	
	@abstractmethod
	async def open(self, port: int):
		raise NotImplementedError
	
	@abstractmethod
	async def close(self):
		raise NotImplementedError
	
	def remove_peer(self, peer: Peer):
		pass


class TcpPeer(Peer):
	server: "TcpServer"
	chunks: Stream[bytes]
	trans: asyncio.Transport
	
	def __init__(self, server: "TcpServer", trans: asyncio.Transport):
		super().__init__(server, trans.get_extra_info("peername"))
		self.chunks = Stream()
		self.trans = trans
	
	def on_eof(self):
		self.chunks.put_eof()
	
	def on_bytes(self, data: bytes):
		self.chunks.put(data)
	
	async def get_bytes(self) -> bytes:
		try:
			return await asyncio.wait_for(self.chunks.get(), self.server.timeout)
		except EOFError:
			self.log(f"{MAGENTA}Peer closed connection")
			raise EOFError
		except asyncio.TimeoutError:
			self.log(f"{MAGENTA}Peer timed out")
			self.disconnect()
			raise EOFError
	
	async def get_bytes_until(self, stop_at: Callable[[bytearray], int]):
		buffer = bytearray()
		while (i := stop_at(buffer)) == -1:
			buffer.extend(await self.get_bytes())
		buffer = bytes(buffer)
		if i == len(buffer):
			return buffer
		else:
			msg, rest = buffer[:i], buffer[i:]
			self.chunks.put_back(rest)
			return msg
	
	async def get_line(self) -> str:
		def find_nl(b):
			i = b.find(b"\n")
			return -1 if i == -1 else i + 1
		line = await self.get_bytes_until(find_nl)
		return line[:-1].decode("utf-8")
	
	def send_bytes(self, data: bytes):
		self.trans.write(data)
	def send_str(self, s: str):
		self.send_bytes(s.encode("utf-8"))
	def send_line(self, s: str):
		self.send_str(s + "\n")
	def send_eof(self):
		self.trans.write_eof()
	
	def disconnect(self):
		self.trans.close()


TcpHandler = Callable[[TcpPeer], Coroutine]

class TcpProtocol(asyncio.Protocol):
	server: "TcpServer"
	peer: TcpPeer
	
	def __init__(self, server: "TcpServer"):
		self.server = server
	
	def connection_made(self, trans: asyncio.Transport):
		self.peer = TcpPeer(self.server, trans)
		self.server.new_peer(self.peer)
	
	def eof_received(self):
		self.peer.on_eof()
		return True # allow half-duplex shutdown
	
	def data_received(self, data: bytes):
		self.peer.on_bytes(data)
	
	def connection_lost(self, exc: Exception | None):
		if exc is not None:
			self.peer.warn("Connection lost:", exc)
			self.peer.on_eof()

class TcpServer(Server[TcpPeer]):
	server: asyncio.Server
	backlog: int
	
	def __init__(self, handler: TcpHandler, timeout: float | None, backlog: int):
		super().__init__(handler, timeout)
		self.backlog = backlog
	
	async def open(self, port: int):
		sock = listen_ip(socket.SOCK_STREAM, port)
		loop = asyncio.get_running_loop()
		self.server = await loop.create_server(lambda: TcpProtocol(self), sock=sock, backlog=self.backlog)
	
	async def close(self):
		self.server.close()
		await self.server.wait_closed()

def serve_tcp(handler: TcpHandler, port=PORT, timeout: float | None = None, backlog=TCP_BACKLOG, debug=False):
	asyncio.run(TcpServer(handler, timeout, backlog).serve(port), debug=debug)


class UdpPeer(Peer):
	server: "UdpServer"
	dgrams: Stream[bytes]
	
	def __init__(self, server: "UdpServer", addr: Addr):
		super().__init__(server, addr)
		self.dgrams = Stream()
		server.peers[addr] = self
	
	def on_dgram(self, data: bytes):
		self.dgrams.put(data)
	
	async def get_dgram(self) -> bytes:
		try:
			return await asyncio.wait_for(self.dgrams.get(), self.server.timeout)
		except asyncio.TimeoutError:
			self.log(f"{MAGENTA}Peer timed out.")
			raise EOFError
	
	def send_dgram(self, data: bytes):
		self.server.trans.sendto(data, self.addr)
	
	def disconnect(self):
		del self.server.peers[self.addr]


UdpHandler = Callable[[UdpPeer], Coroutine]

class UdpServer(Server, asyncio.DatagramProtocol):
	peers: Dict[Addr, UdpPeer]
	trans: asyncio.DatagramTransport
	
	def __init__(self, handler: UdpHandler, timeout: float | None):
		super().__init__(handler, timeout)
		self.peers = {}
	
	async def open(self, port: int):
		sock = listen_ip(socket.SOCK_DGRAM, port)
		loop = asyncio.get_running_loop()
		await loop.create_datagram_endpoint(lambda: self, sock=sock)
	
	def connection_made(self, trans: asyncio.DatagramTransport):
		self.trans = trans
	
	async def close(self):
		self.trans.abort()
	
	def datagram_received(self, data: bytes, addr: Addr):
		if addr not in self.peers:
			peer = UdpPeer(self, addr)
			self.new_peer(peer)
		self.peers[addr].on_dgram(data)
	
	def connection_lost(self, exc: Exception | None):
		if exc is not None:
			log(f"{BRIGHT_RED}Connection lost: {exc}")
			self.stop()

def serve_udp(handler: UdpHandler, port=PORT, timeout: float | None = UDP_TIMEOUT, debug=False):
	asyncio.run(UdpServer(handler, timeout).serve(port), debug=debug)
