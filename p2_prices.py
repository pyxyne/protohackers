from typing import Tuple
from lib_aserve import TcpPeer, serve_tcp
from bisect import insort, bisect_left, bisect_right

async def price_handler(peer: TcpPeer):
	prices: list[Tuple[int, int]] = []
	
	while True:
		try:
			ty, arg1, arg2 = await peer.get_struct("!cii")
		except EOFError:
			break
		
		if ty == b"I":
			timestamp, price = arg1, arg2
			peer.log(f"Inserting price {price} at timestamp {timestamp}")
			insort(prices, (timestamp, price), key=lambda x: x[0])
		elif ty == b"Q":
			min_time, max_time = arg1, arg2
			peer.log(f"Querying mean between {min_time} and {max_time}")
			i1 = bisect_left(prices, min_time, key=lambda x: x[0])
			i2 = bisect_right(prices, max_time, key=lambda x: x[0])
			selected = [p for (t,p) in prices[i1:i2]]
			mean = int(sum(selected) / len(selected)) if len(selected) > 0 else 0
			peer.send_struct("!i", mean)
		else:
			peer.warn("Invalid message type:", repr(ty))

serve_tcp(price_handler)
