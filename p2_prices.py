from lib_tcp import serve, ByteClient
from bisect import insort, bisect_left, bisect_right

class PriceClient(ByteClient):
	def __init__(self, sock):
		super().__init__(sock)
		self.prices = []
	
	def chunk(self, buf):
		return 9 if len(buf) >= 9 else -1
	
	def on_bytes(self, msg):
		ty = msg[0:1]
		arg1 = int.from_bytes(msg[1:5], byteorder="big", signed=True)
		arg2 = int.from_bytes(msg[5:9], byteorder="big", signed=True)
		
		if ty == b"I":
			timestamp, price = arg1, arg2
			self.log(f"Inserting price {price} at timestamp {timestamp}")
			insort(self.prices, (timestamp, price), key=lambda x: x[0])
		elif ty == b"Q":
			min_time, max_time = arg1, arg2
			self.log(f"Querying mean between {min_time} and {max_time}")
			i1 = bisect_left(self.prices, min_time, key=lambda x: x[0])
			i2 = bisect_right(self.prices, max_time, key=lambda x: x[0])
			prices = [p for (t,p) in self.prices[i1:i2]]
			mean = int(sum(prices) / len(prices)) if len(prices) > 0 else 0
			self.send_bytes(mean.to_bytes(4, byteorder="big", signed=True))
		else:
			self.warn("Invalid message type:", repr(ty))

serve(PriceClient)
