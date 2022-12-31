import std.algorithm;
import std.container : BinaryHeap, Array;
import std.array : array;
import std.format : format;
import std.json;
import core.exception : RangeError;

import loop;
import tcp;
import log;

class BadRequest : Exception {
	this(string msg) {
		super(msg);
	}
}

class Job {
	int id;
	int prio;
	string queue;
	JSONValue payload;
	int workerId;
	
	this(int id, int prio, string queue, JSONValue payload) {
		this.id = id;
		this.prio = prio;
		this.queue = queue;
		this.payload = payload;
		this.workerId = -1;
	}
}

class JobQueue {
	BinaryHeap!(Array!Job, "a.prio < b.prio") jobs;
	Queue!Job waiting;
	
	this() {
		waiting = new Queue!Job();
	}
	
	void add(Job job) {
		if(waiting.waiting) {
			waiting.put(job);
		} else {
			jobs.insert(job);
		}
	}
}

T expect(T)(T[string] dict, string key) {
	if(key !in dict) {
		throw new BadRequest(format("field '%s' is missing", key));
	}
	return dict[key];
}

void main() {
	PrettyLogger.setup();
	
	int nextJobId = 0;
	int nextClientId = 0;
	Job[int] jobs;
	JobQueue[string] queues;
	ref JobQueue getQueue(string name) {
		if(name !in queues) {
			queues[name] = new JobQueue();
		}
		return queues[name];
	}
	
	asyncRun({
		new TcpServer(50_000, (TcpConn conn) {
			scope(exit) conn.close();
			
			int clientId = nextClientId++;
			
			int[] workingOn = [];
			scope(exit) {
				foreach(jobId; workingOn) {
					if(jobId !in jobs) continue;
					auto job = jobs[jobId];
					if(job.workerId == clientId) {
						job.workerId = -1;
						queues[job.queue].add(job);
					}
				}
			}
			
			void respond(JSONValue val) {
				string valStr = val.toString;
				conn.write(cast(immutable(ubyte)[]) (valStr ~ "\n"));
				conn.infof("-> %s", valStr);
			}
			
			conn.infof("Client connected");
			while(true) {
				try {
					string line = cast(string) conn.readLine();
					conn.infof("<- %s", line);
					
					try {
						try {
							JSONValue req = parseJSON(line);
							string op = req.object.expect("request").str;
							switch(op) {
								case "put":
									string queue = req.object.expect("queue").str;
									int pri = cast(int) req.object.expect("pri").integer;
									int jobId = nextJobId++;
									Job job = new Job(jobId, pri, queue, req.object.expect("job"));
									jobs[jobId] = job;
									getQueue(queue).add(job);
									respond(JSONValue([
										"status": JSONValue("ok"),
										"id": JSONValue(jobId),
									]));
									break;
								
								case "get":
									string[] selectedQueues = req.object.expect("queues").array
										.map!"a.str".array;
									
									int highestPrio = -1;
									string bestQueue;
									foreach(string queueName; selectedQueues) {
										JobQueue queue = getQueue(queueName);
										if(queue.jobs.length > 0) {
											if(queue.jobs.front.prio > highestPrio) {
												highestPrio = queue.jobs.front.prio;
												bestQueue = queueName;
											}
										}
									}
									
									bool wait = "wait" in req.object && req.object["wait"].boolean;
									Job job;
									if(highestPrio == -1) {
										if(!wait) {
											respond(JSONValue([
												"status": "no-job"
											]));
											continue;
										} else {
											auto f = new Future!Job();
											foreach(string queueName; selectedQueues) {
												queues[queueName].waiting.getInto(f);
											}
											job = f.await();
										}
									} else {
										job = queues[bestQueue].jobs.front();
										queues[bestQueue].jobs.popFront();
									}
									
									job.workerId = clientId;
									workingOn ~= [job.id];
									
									respond(JSONValue([
										"status": JSONValue("ok"),
										"id": JSONValue(job.id),
										"job": JSONValue(job.payload),
										"pri": JSONValue(job.prio),
										"queue": JSONValue(job.queue),
									]));
									break;
								
								case "delete":
									int jobId = cast(int) req.object.expect("id").integer;
									if(jobId !in jobs) {
										respond(JSONValue([
											"status": "no-job"
										]));
										continue;
									}
									Job job = jobs[jobId];
									if(job.workerId == -1) {
										Array!Job queueJobs = queues[job.queue].jobs.release();
										size_t i = queueJobs.data.countUntil(job);
										queueJobs[i] = queueJobs[$-1];
										queueJobs.removeBack();
										queueJobs.data.sort!"a.prio > b.prio"();
										queues[job.queue].jobs.assume(queueJobs);
									}
									job.workerId = -1;
									jobs.remove(jobId);
									respond(JSONValue([
										"status": "ok"
									]));
									break;
								
								case "abort":
									int jobId = cast(int) req.object.expect("id").integer;
									if(jobId !in jobs) {
										respond(JSONValue([
											"status": "no-job"
										]));
										continue;
									}
									Job job = jobs[jobId];
									if(job.workerId != clientId) {
										throw new BadRequest("client aborting job it is not working on");
									}
									job.workerId = -1;
									queues[job.queue].add(job);
									respond(JSONValue([
										"status": "ok"
									]));
									break;
								
								default:
									throw new BadRequest("invalid request type");
							}
						} catch(JSONException) {
							throw new BadRequest("request field has invalid type");
						} catch(RangeError) {
							throw new BadRequest("request field is missing");
						}
					} catch(BadRequest bre) {
						conn.warningf("Bad request: %s", bre.msg);
						conn.warningf("  %s", line);
						respond(JSONValue([
							"status": "error",
							"error": bre.msg,
						]));
					}
				} catch(EofError) {
					conn.infof("Client disconnected");
					break;
				}
			}
		}).run(1000);
	});
}
