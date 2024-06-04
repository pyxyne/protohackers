from socket import AF_INET
from lib_aserve import TcpPeer, serve_tcp
from lib_color import *
import asyncio
import re

async def proxy_handler(peer: TcpPeer):
	peer.debug("Connecting to chat server...")
	reader, writer = await asyncio.open_connection("chat.protohackers.com", 16963,
		family=AF_INET) # force ipv4. ipv6 is fucky for some reason
	peer.debug("Connection established")
	
	def rewrite(b: bytes) -> bytes:
		b2 = re.sub(rb"(^|(?<= ))7[a-zA-Z0-9]{25,34}($|(?= ))", b"7YWHMfk9JZe0LM0g1ZauHuiSxhI", b)
		if b != b2:
			peer.debug("Replaced", b, "with", b2)
		return b2
	
	async def upstream():
		while True:
			try:
				buf = await peer.get_raw_line()
			except EOFError:
				break
			peer.log("<-", buf)
			writer.write(rewrite(buf))
		writer.close()
		reader.feed_eof()
	
	async def downstream():
		while True:
			buf = await reader.readline()
			if len(buf) == 0:
				break
			peer.log("->", buf)
			peer.send_bytes(rewrite(buf))
		peer.send_eof()
		peer.on_eof()
	
	up_task = asyncio.Task(upstream())
	down_task = asyncio.Task(downstream())
	await asyncio.gather(up_task, down_task)
	await writer.wait_closed()
	peer.debug("All connections are closed")

serve_tcp(proxy_handler, backlog=10)
