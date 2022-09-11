from lib_tcp import TcpServer, ByteConn
from lib_color import *
from bisect import insort, bisect_left, bisect_right

class PriceConn(ByteConn):
	def __init__(self, *args):
		super().__init__(*args)
		self.buffer = bytearray()
		self.prices = []
	
	def got_bytes(self, buf):
		self.buffer.extend(buf)
		while len(self.buffer) >= 9:
			msg = self.buffer[:9]
			self.buffer = self.buffer[9:]
			
			ty = msg[0:1]
			arg1 = int.from_bytes(msg[1:5], byteorder="big", signed=True)
			arg2 = int.from_bytes(msg[5:9], byteorder="big", signed=True)
			
			if ty == b"I":
				timestamp, price = arg1, arg2
				self.log(f"Inserting price {price} at timestamp {timestamp})")
				insort(self.prices, (timestamp, price), key=lambda x: x[0])
			elif ty == b"Q":
				min_time, max_time = arg1, arg2
				self.log(f"Querying mean between {min_time} and {max_time})")
				i1 = bisect_left(self.prices, min_time, key=lambda x: x[0])
				i2 = bisect_right(self.prices, max_time, key=lambda x: x[0])
				prices = [p for (t,p) in self.prices[i1:i2]]
				mean = int(sum(prices) / len(prices)) if len(prices) > 0 else 0
				self.send_bytes(mean.to_bytes(4, byteorder="big", signed=True))
			else:
				self.log(f"{YELLOW}Invalid message type: {repr(ty)}")

TcpServer(PriceConn).listen()
