import asyncio
from typing import Any, Callable, Coroutine, Generic, TypeVar
from lib_aserve import serve_tcp, TcpPeer
import json
from collections import defaultdict

def heap_parent(i: int):
	return (i - 1) // 2
def heap_left(i: int):
	return i*2 + 1
def heap_right(i: int):
	return i*2 + 2

T = TypeVar("T")
class MaxHeap(Generic[T]):
	name: str
	arr: list[tuple[T, int]]
	waiting: list[asyncio.Future[tuple[str, T, int]]]
	
	def __init__(self, name):
		self.name = name
		self.arr = []
		self.waiting = []
	
	def __len__(self):
		return len(self.arr)
	
	def pri(self, i: int):
		return self.arr[i][1]
	
	def sift_up(self, i: int):
		while i > 0 and self.pri(i) > self.pri(pi := heap_parent(i)):
			self.arr[i], self.arr[pi] = self.arr[pi], self.arr[i]
			i = pi
	
	def add(self, x: T, pri: int):
		if len(self) == 0 and len(self.waiting) != 0:
			self.waiting[0].set_result((self.name, x, pri))
			self.waiting.pop(0)
			return
		self.arr.append((x, pri))
		self.sift_up(len(self.arr) - 1)
	
	def peek(self):
		return self.arr[0] if len(self) > 0 else None
	
	def remove(self, i: int):
		assert 0 <= i < len(self)
		self.arr[i] = self.arr[-1]
		self.arr.pop()
		if len(self) == 0: return
		while True:
			biggest = i
			li, ri = heap_left(i), heap_right(i)
			if li < len(self) and self.pri(li) > self.pri(biggest):
				biggest = li
			if ri < len(self) and self.pri(ri) > self.pri(biggest):
				biggest = ri
			if biggest == i: break
			self.arr[i], self.arr[biggest] = self.arr[biggest], self.arr[i]
			i = biggest
	
	def pop(self):
		assert len(self) > 0
		pair = self.arr[0]
		self.remove(0)
		return pair
	
	def wait(self, future: asyncio.Future[tuple[str, T, int]]):
		self.waiting.append(future)
		
	def __repr__(self):
		return f"MaxHeap({repr(self.arr)})"


class BadRequest(ValueError): pass

next_id = 0

Job = tuple[int, Any] # (job_id, job)
queues: dict[str, MaxHeap[Job]] = {}
worked_on: dict[int, tuple[int, str, Job, int]] = {} # job_id -> peer_id, queue, job, pri

def get_queue(name: str):
	if name not in queues:
		queues[name] = MaxHeap(name)
	return queues[name]

async def job_handler(peer: TcpPeer):
	global next_id
	
	working_on = set()
	
	def send_json(x: Any):
		# peer.debug("->", x)
		peer.send_line(json.dumps(x))
	
	def parse_job_id(x: Any) -> int:
		if isinstance(x, int):
			return x
		raise BadRequest("invalid job id")
	
	while True:
		try:
			line = await peer.get_line()
		except EOFError:
			break
		
		try:
			try:
				req = json.loads(line)
			except json.JSONDecodeError:
				raise BadRequest("invalid JSON")
			
			# peer.debug("<-", line)
			
			if "request" not in req or req["request"] not in ["put", "get", "delete", "abort"]:
				raise BadRequest(f"invalid request type")
			
			op = req["request"]
			
			if op == "put":
				if "queue" not in req or "job" not in req or "pri" not in req:
					raise BadRequest(f"field missing: 'queue', 'job', or 'pri'")
				if not isinstance(req["queue"], str):
					raise BadRequest(f"invalid 'queue' field")
				if not isinstance(req["pri"], int):
					raise BadRequest("invalid 'pri' field")
				
				job_id = next_id
				next_id += 1
				
				get_queue(req["queue"]).add((job_id, req["job"]), req["pri"])
				
				send_json({ "status": "ok", "id": job_id })
			
			elif op == "get":
				if "queues" not in req:
					raise BadRequest("field missing: 'queues'")
				if not isinstance(req["queues"], list) or not all(isinstance(x, str) for x in req["queues"]):
					raise BadRequest("invalid 'queues' field")
				
				best_pri = None
				best_queue: str | None = None
				for queue_name in req["queues"]:
					job = get_queue(queue_name).peek()
					if job is not None and (best_pri is None or job[1] > best_pri):
						best_pri = job[1]
						best_queue = queue_name
				
				if best_queue is None and not ("wait" in req and req["wait"] == True):
					send_json({
						"status": "no-job"
					})
					continue
				
				if best_queue is not None:
					((job_id, job), pri) = queues[best_queue].pop()
					
				else:
					loop = asyncio.get_running_loop()
					future: asyncio.Future[tuple[str, Job, int]] = loop.create_future()
					for queue_name in req["queues"]:
						queues[queue_name].wait(future)
					best_queue, (job_id, job), pri = await future
				
				worked_on[job_id] = (peer.id, best_queue, job, pri)
				working_on.add(job_id)
				send_json({
					"status": "ok",
					"id": job_id,
					"job": job,
					"pri": pri,
					"queue": best_queue,
				})
			
			elif op == "delete":
				if "id" not in req:
					raise BadRequest("field missing: 'id'")
				job_id = parse_job_id(req["id"])
				
				done = False
				for queue in queues.values():
					for i in range(len(queue.arr)):
						if queue.arr[i][0][0] == job_id:
							queue.remove(i)
							done = True
							break
					if done: break
				
				if job_id in worked_on:
					del worked_on[job_id]
					done = True
				
				if done:
					send_json({ "status": "ok" })
				else:
					send_json({ "status": "no-job" })
			
			elif op == "abort":
				if "id" not in req:
					raise BadRequest("field missing: 'id'")
				job_id = parse_job_id(req["id"])
				
				if job_id not in worked_on:
					send_json({ "status": "no-job" })
				else:
					(peer_id, queue_name, job, pri) = worked_on[job_id]
					if peer_id != peer.id:
						raise BadRequest("trying to abort someone else's job")
					del worked_on[job_id]
					working_on.remove(job_id)
					queues[queue_name].add((job_id, job), pri)
					send_json({ "status": "ok" })
			
		except BadRequest as err:
			peer.warn(f"Got malformed request ({err}):")
			peer.warn("  " + repr(line))
			send_json({
				"status": "error",
				"error": str(err)
			})
	
	for job_id in working_on:
		if job_id in worked_on:
			(peer_id, queue_name, job, pri) = worked_on[job_id]
			if peer_id != peer.id: continue
			del worked_on[job_id]
			queues[queue_name].add((job_id, job), pri)

serve_tcp(job_handler, backlog=1000, debug=False)
