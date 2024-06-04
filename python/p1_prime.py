from lib_aserve import serve_tcp, TcpPeer
import json

def is_prime(n):
	if isinstance(n, float) and not n.is_integer():
		return False
	if n <= 1: return False
	if n == 2: return True
	if n % 2 == 0: return False
	
	i = 3
	while i*i <= n:
		if n % i == 0:
			return False
		i += 2
	
	return True

class MalformedRequest(ValueError): pass

async def prime_handler(peer: TcpPeer):
	while True:
		try:
			line = await peer.get_line()
		except EOFError:
			break
		
		try:
			try:
				req = json.loads(line)
			except json.JSONDecodeError:
				raise MalformedRequest("invalid JSON")
			for field in ["method", "number"]:
				if field not in req:
					raise MalformedRequest(f"missing required field '{field}'")
			if req["method"] != "isPrime":
				raise MalformedRequest("invalid method, expected 'isPrime'")
			n = req["number"]
			if isinstance(n, bool) or (not isinstance(n, int) and not isinstance(n, float)):
				raise MalformedRequest("invalid number, expected integer or float")
			
		except MalformedRequest as err:
			peer.warn(f"Got malformed request ({err}):")
			peer.warn("  " + repr(line))
			res = { "error": str(err) }
			peer.send_line(json.dumps(res))
		else:
			res = { "method": "isPrime", "prime": is_prime(n) }
			peer.log(f"isPrime({n}) == {res['prime']}")
			peer.send_line(json.dumps(res))

serve_tcp(prime_handler)
