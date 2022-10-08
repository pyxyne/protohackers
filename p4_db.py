from lib_aserve import UdpPeer, serve_udp, shorten
from lib_color import *

values: dict[bytes, bytes] = {}

async def db_handler(peer: UdpPeer):
	while True:
		try:
			msg = await peer.get_dgram()
		except EOFError:
			break
		if b"=" in msg: # insert
			[key, val] = msg.split(b"=", maxsplit=1)
			if key == "version":
				peer.log(f"{YELLOW}Ignored insert into 'version'")
			else:
				peer.log(f"Inserted {repr(key)}: {shorten(repr(val))}")
				values[key] = val
		else: # retrieve
			key = msg
			if key == b"version":
				val = b"pyxyne's db"
			else:
				val = values.get(key, b"")
			peer.log(f"Retrieved {repr(key)}: {shorten(repr(val))}")
			peer.send_dgram(key + b"=" + val)

serve_udp(db_handler)
