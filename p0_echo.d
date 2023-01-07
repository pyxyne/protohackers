module p0_echo;

import async_d;

void main() {
	asyncRun({
		new TcpServer(50_000, (TcpConn conn) {
			scope(exit) conn.close();
			conn.infof("Client connected");
			while(true) {
				try {
					auto chunk = conn.readChunk();
					conn.infof("Bouncing %d bytes", chunk.length);
					conn.write(chunk);
				} catch(EofError) {
					conn.infof("Client disconnected");
					break;
				}
			}
		}).run(5);
	});
}
