from typing import Tuple
from lib_aserve import TcpPeer, serve_tcp
from bisect import insort, bisect_left, bisect_right

async def price_handler(peer: TcpPeer):
	prices: list[Tuple[int, int]] = []
	
	while True:
		try:
			msg = await peer.get_bytes_until(lambda b: 9 if len(b) >= 9 else -1)
		except EOFError:
			break
		ty = msg[0:1]
		arg1 = int.from_bytes(msg[1:5], byteorder="big", signed=True)
		arg2 = int.from_bytes(msg[5:9], byteorder="big", signed=True)
		
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
			peer.send_bytes(mean.to_bytes(4, byteorder="big", signed=True))
		else:
			peer.warn("Invalid message type:", repr(ty))

serve_tcp(price_handler)
