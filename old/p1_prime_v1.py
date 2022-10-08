from lib_server import serve, LineClient
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

class PrimeClient(LineClient):
	def on_line(self, line):
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
			self.warn(f"Got malformed request ({err}):")
			self.warn("  " + repr(line))
			res = { "error": str(err) }
			self.send_line(json.dumps(res))
		else:
			res = { "method": "isPrime", "prime": is_prime(n) }
			self.log(f"isPrime({n}) == {res['prime']}")
			self.send_line(json.dumps(res))

serve(PrimeClient)
