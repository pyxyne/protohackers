from lib_server import serve, ByteClient

class EchoClient(ByteClient):
	def on_bytes(self, buf):
		self.log(f"Bounced {len(buf)} bytes")
		self.send_bytes(buf)

serve(EchoClient)
