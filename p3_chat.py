from lib_aserve import TcpPeer, log, serve_tcp
from lib_color import *

users: dict[TcpPeer, str] = {}

def broadcast(msg, except_peer=None):
	log(f"{CYAN}Broadcasting:{RESET}", msg)
	for peer in users.keys():
		if peer is not except_peer:
			peer.send_line(msg)

async def chat_handler(peer: TcpPeer):
	peer.send_line("Welcome to budgetchat! What shall I call you?")
	
	name = await peer.get_line()
	if len(name) == 0 or not all(c.isascii() and c.isalnum() for c in name):
		peer.send_line("Username is invalid")
		peer.end("Sent invalid username:", repr(name))
		return
	
	peer.send_line("* The room contains: " + ", ".join(name for name in users.values()))
	broadcast(f"* {name} has entered the room")
	
	users[peer] = name
	
	while True:
		try:
			line = await peer.get_line()
		except EOFError:
			break
		broadcast(f"[{name}] {line}", except_peer=peer)
	
	del users[peer]
	broadcast(f"* {name} has left the room")

serve_tcp(chat_handler)
