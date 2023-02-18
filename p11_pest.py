import asyncio
from socket import AF_INET
import struct
from typing import Callable, Coroutine, Literal
from collections import defaultdict

from lib_aserve import serve_tcp, TcpPeer, TcpServer, get_addr_str

AS_HOST = "pestcontrol.protohackers.com"
AS_PORT = 20547


## Protocol logic

WRAPPER_SIZE = 6 # type byte (1) + msg length (4) + checksum (1)
MAX_LENGTH = 1024*1024 # 1 MiB

def hex_repr(my_bytes: bytes):
	return " ".join(chr(byte) if 0x41 <= byte <= 0x7e else f"{byte:02x}" for byte in my_bytes)

class ProtoError(Exception):
	pass

class OutMsg:
	def __init__(self, msg_ty: int):
		self.buf = bytearray([msg_ty, 0,0,0,0])
		self.wrapped = False
	
	def add_u8(self, n: int):
		self.buf.append(n)
	
	def add_u32(self, n: int):
		self.buf.extend(struct.pack(">I", n))
	
	def add_str(self, s: str):
		b = s.encode("ascii")
		self.add_u32(len(b))
		self.buf.extend(b)
	
	def send(self, peer: TcpPeer):
		if not self.wrapped:
			self.buf[1:5] = int.to_bytes(len(self.buf) + 1, 4, byteorder="big")
			byte_sum = sum(self.buf) % 256
			checksum = 0 if byte_sum == 0 else 256 - byte_sum
			self.add_u8(checksum)
			self.wrapped = True
		
		b = bytes(self.buf)
		peer.debug(f"-> {hex_repr(b)}")
		peer.send_bytes(b)

hello_msg = OutMsg(0x50) # Hello
hello_msg.add_str("pestcontrol")
hello_msg.add_u32(1)

class InMsg:
	def __init__(self, msg_ty, msg_buf):
		self.ty = msg_ty
		self.buf = msg_buf
		self.i = 0
	
	def get_bytes(self, n: int) -> bytes:
		if self.i > len(self.buf) - n:
			raise ProtoError("content exceeds declared length")
		b = self.buf[self.i : self.i + n]
		self.i += n
		return b
	
	def get_u32(self) -> int:
		return struct.unpack(">I", self.get_bytes(4))[0]
	
	def get_str(self) -> str:
		size = self.get_u32()
		b = self.get_bytes(size)
		return b.decode("ascii")
	
	def check_end(self):
		if self.i != len(self.buf):
			raise ProtoError("unused bytes in message")
	
	@staticmethod
	async def recv(peer: TcpPeer) -> "InMsg":
		(msg_ty, msg_len) = await peer.get_struct(">BI")
		if msg_len >= MAX_LENGTH:
			raise ProtoError("message is too long")
		msg_buf = await peer.get_n_bytes(msg_len - WRAPPER_SIZE)
		checksum = await peer.get_struct("B")
		if (msg_ty + sum(struct.pack(">I", msg_len)) + sum(msg_buf) + checksum) % 256 != 0:
			raise ProtoError("invalid checksum")
		return InMsg(msg_ty, msg_buf)

async def prot_handler(peer: TcpPeer, on_msg: Callable[[TcpPeer, InMsg], Coroutine]):
	hello_msg.send(peer)
	
	got_hello = False
	while True:
		try:
			msg = await InMsg.recv(peer)
			if msg.ty == 0x50: # Hello
				got_hello = True
				protocol, version = msg.get_str(), msg.get_u32()
				msg.check_end()
				peer.debug(f"<- Hello {{ protocol: {repr(protocol)}, version: {version} }}")
				if protocol != "pestcontrol" or version != 1:
					raise ProtoError("unexpected protocol or version")
			else:
				if not got_hello:
					raise ProtoError("did not get Hello")
				if msg.ty == 0x51: # Error
					err_msg = msg.get_str()
					msg.check_end()
					peer.debug(f"<- Error {{ message: {repr(err_msg)} }}")
				elif msg.ty == 0x52: # OK
					msg.check_end()
				else:
					await on_msg(peer, msg)
		except ProtoError as e:
			peer.warn(f"protocol error: {e}")
			msg = OutMsg(0x51)
			msg.add_str(str(e))
			msg.send(peer)
		except EOFError:
			break



## Application logic

Targets = dict[str, tuple[int, int]]
target_pops: dict[int, Targets] = {}
target_pop_future: dict[int, asyncio.Future[Targets]] = {}

class Policy:
	def __init__(self, site: int, species: str, ty: Literal["cull"] | Literal["conserve"]):
		self.site = site
		self.species = species
		self.ty = ty
		self.id = None
		self.state = "pending"
	
	def set_id(self, id: int, as_conn: TcpPeer):
		assert self.state != "exists"
		self.id = id
		if self.state == "deleted":
			self.send_delete(as_conn)
		elif self.state == "pending":
			self.state = "exists"
	
	def send_delete(self, as_conn: TcpPeer):
		assert self.id is not None
		del_msg = OutMsg(0x56) # DeletePolicy
		del_msg.add_u32(self.id)
		del_msg.send(as_conn)
	
	def delete(self, as_conn: TcpPeer):
		if self.state == "pending":
			self.state = "deleted"
		elif self.state == "exists":
			self.state = "deleted"
			self.send_delete(as_conn)
		elif self.state == "deleted":
			return

pending_policies: dict[int, list[Policy]] = defaultdict(list) # site → list[Policy]
policies: dict[tuple[int, str], Policy] = {} # (site, species) → Policy

async def as_handler(peer: TcpPeer, msg: InMsg, site: int):
	if msg.ty == 0x54: # TargetPopulations
		site2 = msg.get_u32()
		pop_cnt = msg.get_u32()
		targets: Targets = {}
		for _ in range(pop_cnt):
			species = msg.get_str()
			min_pop = msg.get_u32()
			max_pop = msg.get_u32()
			if species in targets and targets[species] != (min_pop, max_pop):
				raise ProtoError(f"conflicting target for species '{species}'")
			targets[species] = (min_pop, max_pop)
		msg.check_end()
		peer.debug(f"<- TargetPopulations {{ site: {site2}, populations: {repr(targets)} }}")
		assert site == site2
		
		target_pops[site2] = targets
		target_pop_future[site2].set_result(targets)
	
	elif msg.ty == 0x57: # PolicyResult
		policy_id = msg.get_u32()
		msg.check_end()
		pol = pending_policies[site].pop(0)
		pol.set_id(policy_id, peer)
		
	else:
		raise ProtoError(f"unexpected message type {msg.ty:02x}")

as_conns: dict[int, TcpPeer] = {}
async def get_site_data(server: TcpServer, site: int) -> tuple[TcpPeer, Targets]:
	if site in target_pops:
		return as_conns[site], target_pops[site]
	elif site in target_pop_future:
		targets = await target_pop_future[site]
		return as_conns[site], targets
	else:
		loop = asyncio.get_running_loop()
		target_pop_future[site] = loop.create_future()
		
		async def my_as_handler(peer: TcpPeer):
			await prot_handler(peer, lambda peer, msg: as_handler(peer, msg, site))
		peer = await server.add_external_peer(AS_HOST, AS_PORT, AF_INET, "as", my_as_handler)
		as_conns[site] = peer
		
		msg = OutMsg(0x53) # DialAuthority
		msg.add_u32(site)
		msg.send(peer)
		
		targets = await target_pop_future[site]
		
		return peer, targets


async def client_handler(peer: TcpPeer, msg: InMsg):
	if msg.ty == 0x58: # SiteVisit
		site = msg.get_u32()
		pop_cnt = msg.get_u32()
		populations: dict[str, int] = {}
		for _ in range(pop_cnt):
			species = msg.get_str()
			count = msg.get_u32()
			if species in populations and count != populations[species]:
				raise ProtoError(f"conflicting counts for species '{species}'")
			populations[species] = count
		msg.check_end()
		peer.debug(f"<- SiteVisit {{ site: {site}, populations: {repr(populations)} }}")
		
		as_conn, targets = await get_site_data(peer.server, site)
		
		for species, (min_pop, max_pop) in targets.items():
			pop = populations.get(species, 0)
			need_policy = None
			if pop < min_pop:
				need_policy = "conserve"
			if pop > max_pop:
				need_policy = "cull"
			
			cur_policy = policies.get((site, species), None)
			cur_policy = None if cur_policy is None else cur_policy.ty
			if need_policy == cur_policy:
				continue
			
			if (site, species) in policies:
				policy = policies[(site, species)]
				del policies[(site, species)]
				policy.delete(as_conn)
			
			if need_policy is not None:
				cre_msg = OutMsg(0x55) # CreatePolicy
				cre_msg.add_str(species)
				cre_msg.add_u8({ "cull": 0x90, "conserve": 0xa0 }[need_policy])
				cre_msg.send(as_conn)
				policy = Policy(site, species, need_policy)
				policies[(site, species)] = policy
				pending_policies[site].append(policy)
		
	else:
		raise ProtoError(f"unexpected message type {msg.ty:02x}")

serve_tcp(lambda peer: prot_handler(peer, client_handler))
