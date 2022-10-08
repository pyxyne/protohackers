from lib_server import serve, ByteClient
from lib_color import *
	
values: dict[bytes, bytes] = {}

class DbClient(ByteClient):
	def on_bytes(self, req: bytes):
		parts = req.split(b"=", maxsplit=1)
		if len(parts) >= 2: # insert
			[key, val] = parts
			if key == "version":
				self.log(f"{YELLOW}Ignored insert into 'version'")
			else:
				self.log(f"Inserted {repr(key)}: {repr(val)}")
				values[key] = val
		else: # retrieve
			key = req
			if key == b"version":
				val = b"pyxyne's db"
			else:
				val = values.get(key, b"")
			self.log(f"Retrieved {repr(key)}: {repr(val)}")
			self.send_bytes(key + b"=" + val)

serve(DbClient, udp=True)
