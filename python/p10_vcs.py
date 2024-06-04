from lib_aserve import serve_tcp, TcpPeer
import string

class MalformedRequest(ValueError): pass

def parse_path(path: str):
	if len(path) == 0 or path[0] != "/":
		return None
	if path == "/":
		return []
	parts = path[1:].split("/")
	if any(part == "" or not all(c.isalnum() or c in "_.-" for c in part) for part in parts):
		return None
	return parts

def parse_file_path(path: str):
	parts = parse_path(path)
	if parts is None or len(parts) == 0:
		return None
	return "/".join(parts[:-1]), parts[-1]

def parse_dir_path(path: str):
	if len(path) > 1 and path[-1] == "/":
		path = path[:-1]
	parts = parse_path(path)
	if parts is None: return None
	return "/".join(parts)

dirs: dict[str, dict[str, list[str]]] = {}

async def vcs_handler(peer: TcpPeer):
	def send_line(line: str):
		peer.send_line(line)
		peer.debug("->", line)
	
	while True:
		send_line("READY")
		try:
			line = await peer.get_line()
		except EOFError:
			break
		
		peer.debug("<-", line)
		
		args = line.split()
		cmd = args[0].lower()
		
		if cmd == "help":
			send_line("OK usage: HELP|GET|PUT|LIST")
			
		elif cmd == "list":
			if len(args) != 2:
				send_line("ERR usage: LIST dir")
				continue
			path = parse_dir_path(args[1])
			if path is None:
				send_line("ERR illegal dir name")
				continue
			list = {}
			if path in dirs:
				for filename, revs in dirs[path].items():
					list[filename] = "r" + str(len(revs))
			prefix = path + "/" if len(path) > 0 else ""
			for dir_path in dirs:
				if dir_path.startswith(prefix) and dir_path != path:
					parts = dir_path[len(prefix):].split("/")
					dir_name = parts[0]
					if dir_name not in list:
						list[dir_name + "/"] = "DIR"
			send_line(f"OK {len(list)}")
			for name, value in sorted(list.items(), key=lambda p: p[0]):
				send_line(f"{name} {value}")
		
		elif cmd == "put":
			if len(args) != 3:
				send_line("ERR usage: PUT file length newline data")
				continue
			path = parse_file_path(args[1])
			if path is None:
				send_line("ERR illegal file name")
				continue
			dir_path, filename = path
			try:
				length = int(args[2])
			except ValueError:
				length = 0
			data = await peer.get_n_bytes(length)
			peer.debug("<-", repr(data))
			try:
				data = data.decode("ascii")
			except UnicodeDecodeError:
				send_line("ERR file is invalid ascii")
				continue
			if not all(c in string.printable for c in data):
				send_line("ERR file content is not printable")
				continue
			
			if dir_path not in dirs:
				dirs[dir_path] = {}
			dir = dirs[dir_path]
			if filename not in dir:
				dir[filename] = []
			file = dir[filename]
			if len(file) == 0 or file[-1] != data:
				file.append(data)
			send_line(f"OK r{len(file)}")
			
		elif cmd == "get":
			if len(args) != 2 and len(args) != 3:
				send_line("ERR usage: GET file [revision]")
				continue
			path = parse_file_path(args[1])
			if path is None:
				send_line("ERR illegal file name")
				continue
			dir_path, filename = path
			rev = None
			if len(args) == 3:
				rev = args[2]
				if rev[0] == "r":
					rev = rev[1:]
				try:
					rev = int(rev)
				except ValueError:
					send_line("ERR no such revision")
					continue
			if dir_path not in dirs or filename not in dirs[dir_path]:
				send_line("ERR no such file")
				continue
			file = dirs[dir_path][filename]
			if rev is None:
				rev = len(file)
			if rev < 1 or rev > len(file):
				send_line("ERR no such revision")
				continue
			data = file[rev-1]
			send_line(f"OK {len(data)}")
			peer.send_bytes(data.encode("ascii"))
			
		else:
			send_line(f"ERR illegal method: {cmd}")
			return

serve_tcp(vcs_handler)
