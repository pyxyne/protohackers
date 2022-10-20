from lib_aserve import TcpPeer, serve_tcp
from lib_color import *
import asyncio
import re

async def proxy_handler(peer: TcpPeer):
	peer.log("Connecting to chat server...")
	reader, writer = await asyncio.open_connection("chat.protohackers.com", 16963)
	peer.log("Connection established")
	
	def rewrite(b: bytes) -> bytes:
		b2 = re.sub(rb"(^|(?<= ))7[a-zA-Z0-9]{25,34}($|(?= ))", b"7YWHMfk9JZe0LM0g1ZauHuiSxhI", b)
		if b != b2:
			peer.log("Replaced", b, "with", b2)
		return b2
	
	async def upstream():
		while True:
			try:
				bytes = await peer.get_raw_line()
			except EOFError:
				break
			peer.log("Upstream:", bytes)
			writer.write(rewrite(bytes))
			await writer.drain()
		peer.log("Upstream closed")
		writer.close()
		reader.feed_eof()
		await writer.wait_closed()
	
	async def downstream():
		while True:
			bytes = await reader.readline()
			if len(bytes) == 0:
				break
			peer.log("Downstream:", bytes)
			peer.send_bytes(rewrite(bytes))
		peer.log("Downstream closed")
		peer.send_eof()
		peer.on_eof()
	
	up_task = asyncio.Task(upstream())
	down_task = asyncio.Task(downstream())
	await asyncio.gather(up_task, down_task)

serve_tcp(proxy_handler, backlog=10)
