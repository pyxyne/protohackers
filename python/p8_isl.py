from typing import Literal, Tuple
from lib_aserve import TcpPeer, log, serve_tcp, shorten
from lib_color import *

def app(line: str):
	toys = line.split(",")
	busiest_toy = max(toys, key=lambda toy: int(toy.split("x ")[0]))
	return busiest_toy

CypherT = list[
	Tuple[Literal["rev"], None]
	| Tuple[Literal["xor"] | Literal["add"], int | Literal["pos"]]
]

def rev_bits(b: int):
	return (b&1)<<7 | (b&2)<<5 | (b&4)<<3 | (b&8)<<1 | (b&16)>>1 | (b&32)>>3 | (b&64)>>5 | (b&128)>>7

def encode(cypher: CypherT, out_pos: int, b: int):
	for op, arg in cypher:
		if op == "rev":
			b = rev_bits(b)
		elif op == "xor":
			assert arg is not None
			if arg == "pos": arg = out_pos % 256
			b = b ^ arg
		elif op == "add":
			assert arg is not None
			if arg == "pos": arg = out_pos % 256
			b = (b + arg) % 256
	return b

def decode(cypher: CypherT, in_pos: int, b: int):
	for op, arg in cypher[::-1]:
		if op == "rev":
			b = rev_bits(b)
		elif op == "xor":
			assert arg is not None
			if arg == "pos": arg = in_pos % 256
			b = b ^ arg
		elif op == "add":
			assert arg is not None
			if arg == "pos": arg = in_pos % 256
			b = (b - arg) % 256
	return b

async def isl_handler(peer: TcpPeer):
	cypher: CypherT = []
	while True:
		b = await peer.get_struct("B")
		if b == 0:
			break
		elif b == 1:
			cypher.append(("rev", None))
		elif b == 2:
			arg = await peer.get_struct("B")
			cypher.append(("xor", arg))
		elif b == 3:
			cypher.append(("xor", "pos"))
		elif b == 4:
			arg = await peer.get_struct("B")
			cypher.append(("add", arg))
		elif b == 5:
			cypher.append(("add", "pos"))
		else:
			peer.disconnect()
	peer.log(f"Cypher: {repr(cypher)}")
	
	is_noop = True
	for pos in range(256):
		for b in range(256):
			if encode(cypher, pos, b) != b:
				is_noop = False
				break
		if not is_noop: break
	if is_noop:
		peer.warn("Cypher is no-op, disconnecting.")
		peer.disconnect()
		return
	
	in_pos = 0
	in_buf = ""
	out_pos = 0
	while True:
		try:
			byte = await peer.get_struct("B")
		except EOFError:
			break
		ch = chr(decode(cypher, in_pos, byte))
		in_buf += ch
		in_pos += 1
		while "\n" in in_buf:
			line_end = in_buf.index("\n")
			line = in_buf[:line_end]
			in_buf = in_buf[line_end+1:]
			peer.log(f"Received {repr(line)}")
			res = app(line)
			peer.log(f"Sending back {repr(res)}")
			res = (res + "\n").encode("ascii")
			cyphertext = bytearray()
			for b in res:
				cyphertext.append(encode(cypher, out_pos, b))
				out_pos += 1
			peer.send_bytes(bytes(cyphertext))

serve_tcp(isl_handler, backlog=10)
