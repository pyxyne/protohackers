module async_d.tcp;

import std.algorithm : findSplit;
import std.bitmanip : bigEndianToNative;
import std.logger;
import std.socket;
import std.format : format;
import std.json : JSONValue;
import std.typecons : nullable;
import core.stdc.errno;

import core.sys.linux.epoll;

import async_d.loop;
import async_d.log;

class EofError : Exception {
	this() {
		super("Unexpected EOF");
	}
}
class SocketError : Exception {
	this(string msg) {
		super("Socket error: " ~ msg);
	}
}

immutable size_t BUF_SIZE = 4096;

class TcpConn : PrettyLogger {
	int id;
	Socket sock;
	string addr;
	ubyte[] buffer;
	
	this(Socket sock, int id) {
		this.sock = sock;
		auto ipv6 = cast(Internet6Address) this.sock.remoteAddress;
		if(ipv6.addr[0..12] == [0,0,0,0,0,0,0,0,0,0,0xff,0xff]) {
			uint ipv4 = bigEndianToNative!uint(cast(ubyte[4]) ipv6.addr[12..$]);
			addr = format("%s:%d", InternetAddress.addrToString(ipv4), ipv6.port);
		} else {
			addr = format("%s", ipv6);
		}
		super(addr);
	}
	
	immutable(ubyte)[] readChunk() {
		awaitEvent(nullable(FileEvent(sock.handle, EPOLLIN)));
		ubyte[BUF_SIZE] chunk;
		long bytes = sock.receive(chunk);
		if(bytes == 0) throw new EofError();
		if(bytes == Socket.ERROR) {
			if(errno == EPIPE || errno == ECONNRESET)
				throw new EofError();
			throw new SocketError(format("Socket error %d", errno));
		}
		return chunk[0..bytes].idup;
	}
	
	immutable(ubyte)[] readLine() {
		while(true) {
			if(auto split = buffer.findSplit("\n")) {
				buffer = split[2];
				return split[0].idup;
			}
			buffer ~= readChunk();
		}
	}
	
	immutable(ubyte)[] readN(int N) {
		while(buffer.length < N) {
			buffer ~= readChunk();
		}
		auto chunk = buffer[0..N].idup;
		buffer = buffer[N..$];
		return chunk;
	}
	immutable(ubyte)[N] readN(int N)() {
		return readN(N);
	}
	
	void write(immutable(ubyte)[] buf) {
		long totalSent = 0;
		while(true) {
			long sent = sock.send(buf[totalSent..$]);
			if(sent == Socket.ERROR) {
				if(errno == EPIPE || errno == ECONNRESET)
					throw new EofError();
				throw new SocketError(format("Socket error %d", errno));
			}
			totalSent += sent;
			if(totalSent < buf.length) {
				awaitEvent(nullable(FileEvent(sock.handle, EPOLLOUT)));
			} else {
				break;
			}
		}
	}
	
	void close() {
		sock.shutdown(SocketShutdown.BOTH);
		sock.close();
	}
}

class TcpServer {
	void delegate(TcpConn) handler;
	ushort port;
	Socket sock;
	int nextClientId;
	
	this(ushort port, void delegate(TcpConn) handler) {
		this.handler = handler;
		this.port = port;
		nextClientId = 0;
		sock = new TcpSocket(AddressFamily.INET6);
		sock.blocking = false;
		sock.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, 1);
		sock.bind(new Internet6Address(Internet6Address.ADDR_ANY, port));
	}
	
	void run(int backlog = 10) {
		sock.listen(backlog);
		infof("Server is listening on port %s", port);
		asyncRun({
			while(true) {
				awaitEvent(nullable(FileEvent(sock.handle, EPOLLIN)));
				spawn(handlerDelegate(sock.accept()));
			}
		});
	}
	
	void close() {
		sock.shutdown(SocketShutdown.BOTH);
		sock.close();
	}
	
	private:
	void delegate() handlerDelegate(Socket sock) {
		return () { handler(new TcpConn(sock, nextClientId++)); };
	}
}
