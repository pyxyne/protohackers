from lib_tcp import serve, LineClient
from lib_color import *

users = []

def broadcast(msg, except_user=None):
	print(f"{CYAN}Broadcasting:{RESET}", msg)
	for user in users:
		if user is not except_user:
			user.send_str(msg + "\n")

class ChatClient(LineClient):
	def __init__(self, *args):
		super().__init__(*args)
		self.joined = False
	
	def on_open(self):
		self.send_str("Welcome to budgetchat! What shall I call you?\n")
	
	def on_line(self, line):
		if not self.joined:
			if len(line) < 1 or not all(c.isascii() and c.isalnum() for c in line):
				self.warn("Sent invalid username:", repr(line))
				self.send_line("Username is invalid")
				self.close()
				return
			self.name = line
			
			self.send_line("* The room contains: " + ", ".join(user.name for user in users))
			broadcast(f"* {self.name} has entered the room")
			
			self.joined = True
			users.append(self)
		
		else:
			broadcast(f"[{self.name}] {line}", except_user=self)
	
	def on_eoi(self):
		if self.joined:
			users.remove(self)
			broadcast(f"* {self.name} has left the room")

serve(ChatClient)
