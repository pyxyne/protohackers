from lib_aserve import TcpPeer, serve_tcp

async def echo_handler(peer: TcpPeer):
	while True:
		try:
			buf = await peer.get_bytes()
		except EOFError:
			break
		peer.log(f"Bounced {len(buf)} bytes")
		peer.send_bytes(buf)

serve_tcp(echo_handler)
