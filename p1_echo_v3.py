from lib_tcp import TcpServer, ByteConn

class EchoConn(ByteConn):
	def got_bytes(self, buf):
		self.log(f"Bounced {len(buf)} bytes")
		self.send_bytes(buf)

TcpServer(EchoConn).listen()
