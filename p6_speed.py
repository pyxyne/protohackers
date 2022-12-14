import asyncio
import  bisect
from collections import defaultdict
import struct
from typing import Dict, Tuple
from lib_aserve import TcpPeer, serve_tcp, shorten
from lib_color import *

class ProtocolError(Exception): pass

road_limit: Dict[int, int] = {} # road → limit
observations: Dict[Tuple[int, bytes], list[Tuple[int, int]]] = defaultdict(list) # (road, plate) → (timestamp, mile)
road_dispatchers: Dict[int, set[TcpPeer]] = defaultdict(set) # road → set(peers)
pending_tickets: Dict[int, list[bytes]] = defaultdict(list) # road → list(ticket messages)
ticketed_on_days: Dict[bytes, set[int]] = defaultdict(set) # plate → days ticketed

async def speed_handler(peer: TcpPeer):
	async def read_str() -> bytes:
		len = await peer.get_struct("!B", True)
		return await peer.get_n_bytes(len, True)
	
	def err(msg: str):
		raise ProtocolError(msg)
	
	async def heartbeat(interval: int):
		while not peer.is_eof():
			peer.send_bytes(b"\x41")
			await asyncio.sleep(interval / 10)
	
	def dispatch_ticket(plate: bytes, road: int, mile1: int, timestamp1: int, mile2: int, timestamp2: int, speed: int):
		day1 = timestamp1 // 86400
		day2 = timestamp2 // 86400
		for day in range(day1, day2 + 1):
			if day in ticketed_on_days[plate]:
				# already ticketed, don't send another one
				return
		for day in range(day1, day2 + 1):
			ticketed_on_days[plate].add(day)
		
		peer.log(f"{YELLOW}Sending ticket: {repr(plate)} {road} {mile1} {timestamp1} {mile2} {timestamp2} {speed}")
		
		msg = struct.pack("!BB", 0x21, len(plate)) + plate \
			+ struct.pack("!HHIHIH", road, mile1, timestamp1, mile2, timestamp2, speed)
		if len(road_dispatchers[road]) > 0:
			disp = next(iter(road_dispatchers[road]))
			disp.send_bytes(msg)
		else:
			pending_tickets[road].append(msg)
	
	heartbeat_task = None
	camera_pos: Tuple[int, int] | None = None
	dispatcher_roads: list[int] | None = None
	identified = False
	
	try:
		while True:
			try:
				msg_ty = await peer.get_struct("!B")
			except EOFError:
				break
			
			if msg_ty == 0x20: # Plate
				if camera_pos is None: err("client not a camera")
				plate = await read_str()
				timestamp = await peer.get_struct("!I", True)
				road, mile = camera_pos[0], camera_pos[1]
				peer.log(f"Plate {repr(plate)} {timestamp}")
				
				obs = observations[(road, plate)]
				bisect.insort(obs, (timestamp, mile), key=lambda x: x[0])
				if len(obs) >= 2:
					for (t1, x1), (t2, x2) in zip(obs[:-1], obs[1:]):
						speed = abs(x2 - x1) / (t2 - t1) * 3600 # mph
						if speed > road_limit[road] + 0.25: # 0.25 mph margin of error
							dispatch_ticket(plate, road, x1, t1, x2, t2, int(speed * 100))
				
			elif msg_ty == 0x40: # WantHeartbeat
				if heartbeat_task is not None: err("heartbeat already set")
				interval = await peer.get_struct("!I", True)
				peer.log(f"WantHeartbeat {interval}")
				if interval == 0: continue
				heartbeat_task = asyncio.Task(heartbeat(interval))
				
			elif msg_ty == 0x80: # IAmCamera
				if identified: err("client already identified")
				road, mile, limit = await peer.get_struct("!HHH", True)
				new_name = f"camera({road},{mile})"
				peer.log(f"{DIM_WHITE}-> {new_name}{RESET} IAmCamera {road} {mile} {limit}")
				peer.name = new_name
				camera_pos = (road, mile)
				road_limit[road] = limit
				identified = True
				
			elif msg_ty == 0x81: # IAmDispatcher
				if identified: err("client already identified")
				num_roads = await peer.get_struct("!B", True)
				roads = []
				for _ in range(num_roads):
					roads.append(await peer.get_struct("!H", True))
				new_name = f"dispatcher{peer.id}"
				peer.log(f"{DIM_WHITE}-> {new_name}{RESET} IAmDispatcher {shorten(repr(roads))}")
				peer.name = new_name
				dispatcher_roads = roads
				identified = True
				
				for road in roads:
					road_dispatchers[road].add(peer)
					
					if len(pending_tickets[road]) > 0:
						tickets = pending_tickets[road]
						pending_tickets[road] = []
						for ticket in tickets:
							peer.send_bytes(ticket)
				
			else:
				err("invalid message type")
				
	except ProtocolError as e:
		err_msg = " ".join(e.args)
		peer.log(f"{RED}Protocol error: {err_msg}")
		err_msg = err_msg.encode("utf-8")
		assert len(err_msg) <= 255
		peer.send_struct("!BB", 0x10, len(err_msg))
		peer.send_bytes(err_msg)
		peer.disconnect()
	
	if dispatcher_roads is not None:
		for road in dispatcher_roads:
			road_dispatchers[road].remove(peer)
	if heartbeat_task is not None:
		heartbeat_task.cancel()

serve_tcp(speed_handler, backlog=150)
