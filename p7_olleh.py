import asyncio
from typing import Dict
from lib_aserve import UdpPeer, serve_udp, shorten
from lib_color import *

retrans_timeout = 3
expiry_timeout = 60

sessions: Dict[int, "Session"] = {}

def encode(s: str):
	return s.replace("\\", "\\\\").replace("/", "\\/")

class Session:
	def __init__(self, id: int, peer: UdpPeer):
		self.id = id
		self.peer = peer
		self.received = 0
		self.sent = 0
		self.acknowledged = 0
		self.unacknowledged = ""
		self.retrans_task = None
		self.buffer = ""
	
	async def retransmit(self):
		while True:
			await asyncio.sleep(retrans_timeout)
			if self.acknowledged < self.sent:
				data = self.unacknowledged
				self.peer.log(f"[{self.id}] retransmitting last {self.sent - self.acknowledged} bytes")
				chunk_size = 400
				for chunk_i in range(len(data) // chunk_size + 1):
					chunk = data[chunk_i*chunk_size:(chunk_i+1)*chunk_size]
					encoded = encode(chunk)
					self.peer.send_dgram(f"/data/{self.id}/{self.acknowledged + chunk_i*chunk_size}/{encoded}/".encode("ascii"))
			else:
				break
		self.retrans_task = None
	
	def app_send(self, data):
		self.peer.log(f"[{self.id}] app sent: " + repr(data))
		
		chunk_size = 400
		for chunk_i in range(len(data) // chunk_size + 1):
			chunk = data[chunk_i*chunk_size:(chunk_i+1)*chunk_size]
			encoded = chunk.replace("\\", "\\\\").replace("/", "\\/")
			
			self.peer.debug("sending chunk: " + repr(encoded))
			
			self.peer.send_dgram(f"/data/{self.id}/{self.sent}/{encoded}/".encode("ascii"))
			self.sent += len(chunk)
			self.unacknowledged += chunk
			
			if self.retrans_task is None:
				self.retrans_task = asyncio.Task(self.retransmit())
	
	def app_receive(self, data: str):
		self.peer.log(f"[{self.id}] app received: " + repr(data))
		self.buffer += data
		
		i = self.buffer.find("\n")
		while i != -1:
			line = self.buffer[:i]
			
			self.app_send(line[::-1] + "\n")
			
			self.buffer = self.buffer[i+1:]
			i = self.buffer.find("\n")
	
	def data(self, position: int, data: str):
		if position == self.received:
			self.app_receive(data)
			self.received += len(data)
		self.peer.send_dgram(f"/ack/{self.id}/{self.received}/".encode("ascii"))
	
	def close(self):
		global sessions
		self.peer.send_dgram(f"/close/{self.id}/".encode("ascii"))
		if self.retrans_task is not None:
			self.retrans_task.cancel()
		del sessions[self.id]
	
	def ack(self, length: int):
		if length <= self.acknowledged:
			return
		
		if length > self.sent:
			self.close()
		
		newly_acknowledged = length - self.acknowledged
		self.unacknowledged = self.unacknowledged[newly_acknowledged:]
		self.acknowledged = length
		
		if self.acknowledged < self.sent:
			encoded = encode(self.unacknowledged)
			self.peer.debug("responding to ack with retransmission: " + repr(encoded))
			self.peer.send_dgram(f"/data/{self.id}/{self.acknowledged}/{encoded}/".encode("ascii"))

class InvalidMsg(Exception):
	pass

async def olleh_handler(peer: UdpPeer):
	def invalid(reason: str):
		raise InvalidMsg(reason)
	def ensure(cond: bool, reason: str):
		if not cond:
			invalid(reason)
	
	while True:
		try:
			msg = await peer.get_dgram()
		except EOFError:
			break
		
		try:
			ensure(len(msg) < 1000, "too long")
			msg = msg.decode("ascii")
			ensure(msg.startswith("/"), "not starting with /")
			
			fields = []
			escaped = False
			for c in msg:
				if escaped:
					fields[-1] += c
					escaped = False
				elif c == "/":
					fields.append("")
				elif c == "\\":
					escaped = True
				else:
					fields[-1] += c
			ensure(fields[-1] == "", "unfinished message")
			fields.pop()
			
			peer.debug("received: " + shorten(repr(fields)))
			
			ensure(len(fields) >= 1, "no message type")
			
			msg_ty = fields[0]
			
			def arg_cnt(n: int):
				ensure(len(fields) == n+1, f"'{msg_ty}' expects {n} arguments")
			def int_arg(i: int):
				try:
					val = int(fields[i])
					if val >= 2147483648:
						raise ValueError
					return val
				except ValueError:
					invalid("invalid integer argument")
			
			if msg_ty == "connect":
				arg_cnt(1)
				session = int_arg(1)
				if session not in sessions:
					sessions[session] = Session(session, peer)
				peer.send_dgram(f"/ack/{session}/0/".encode("ascii"))
			
			elif msg_ty == "data":
				arg_cnt(3)
				session = int_arg(1)
				position = int_arg(2)
				ensure(position >= 0, "position is negative")
				data = fields[3]
				
				if session not in sessions:
					peer.send_dgram(f"/close/{session}/".encode("ascii"))
					continue
				
				sessions[session].data(position, data)
				
			elif msg_ty == "ack":
				arg_cnt(2)
				session = int_arg(1)
				length = int_arg(2)
				ensure(length >= 0, "length is negative")
				
				if session not in sessions:
					peer.send_dgram(f"/close/{session}/".encode("ascii"))
					continue
				
				sessions[session].ack(length)
			
			elif msg_ty == "close":
				arg_cnt(1)
				session = int_arg(1)
				if session in sessions:
					sessions[session].close()
				else:
					peer.send_dgram(f"/close/{session}/".encode("ascii"))
				
			else:
				invalid("unknown message type")
			
		except InvalidMsg as e:
			peer.warn(f"invalid message: {e}")

serve_udp(olleh_handler, timeout=expiry_timeout)
