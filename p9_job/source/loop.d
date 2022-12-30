module loop;

import std.algorithm.mutation : remove;
import std.datetime : MonoTime, msecs;
import std.format : format;
import std.logger;
import std.math : ceil;
import std.typecons : Nullable;

import core.stdc.errno;
import core.thread.fiber;

import core.sys.linux.epoll;

int awaitFd(int fd, int events, int timeout = -1) {
	auto f = cast(AsyncFiber) Fiber.getThis();
	assert(f !is null, "Can only await inside an AsyncFiber");
	f.awaitedFd = fd;
	f.events = events;
	if(timeout == -1) {
		f.timeout.nullify();
	} else {
		f.timeout = MonoTime.currTime + timeout.msecs;
	}
	f.result = -1;
	Fiber.yield();
	return f.result;
}

class Waiter {
	AsyncFiber f;
	
	void await(int timeout = -1) {
		f = cast(AsyncFiber) Fiber.getThis();
		awaitFd(-1, 0, timeout);
	}
	
	void wake() {
		assert(f !is null, "Waking non-awaited Waiter");
		f.loop.resolve(f, 0);
		f = null;
	}
}

class Future(T) {
	Waiter waiter;
	Nullable!T result;
	
	this() {
		waiter = new Waiter();
	}
	
	@property bool resolved() {
		return !result.isNull;
	}
	
	T await(int timeout = -1) {
		waiter.await(timeout);
		assert(resolved, "Future was awaited twice");
		return result.get;
	}
	
	void resolve(T val) {
		assert(!resolved, "Future was resolved twice");
		result = val;
		waiter.wake();
	}
}

class Queue(T) {
	Future!T[] futures;
	
	void getInto(Future!T f) {
		futures ~= [f];
	}
	
	@property bool waiting() {
		while(futures.length > 0 && futures[0].resolved) {
			futures = futures[1 .. $];
		}
		return futures.length > 0;
	}
	
	void put(T val) {
		assert(waiting, "Queue is not awaited");
		futures[0].resolve(val);
		futures = futures[1 .. $];
	}
}

void spawn(void delegate() dg) {
	auto f = cast(AsyncFiber) Fiber.getThis();
	assert(f !is null, "Can only spawn inside an AsyncFiber");
	f.loop.spawn(dg);
}

class AsyncFiber : Fiber {
	EventLoop loop;
	int awaitedFd;
	int events;
	Nullable!MonoTime timeout;
	int result;
	
	this(EventLoop loop, void delegate() dg) {
		super(dg);
		this.loop = loop;
		this.awaitedFd = -1;
	}
	
	bool waiting() {
		return result == -1;
	}
	
	int msUntilTimeout(MonoTime t = MonoTime.currTime) {
		return timeout.isNull ? -1 : cast(int) ceil((timeout.get - t).total!"nsecs" / 1_000_000.0);
	}
}

class EventLoop {
	int poller;
	AsyncFiber[] fibers;
	int readyCnt;
	
	this() {
		poller = epoll_create1(0);
	}
	
	void spawn(void delegate() dg) {
		auto fiber = new AsyncFiber(this, dg);
		fibers ~= [fiber];
		readyCnt++;
		tracef("[%x] Fiber spawned (total: %d)", cast(void*) fiber, fibers.length);
	}
	
	void resolve(AsyncFiber f, int result) {
		assert(f.waiting, "cannot resolve AsyncFiber twice");
		if(f.awaitedFd != -1) {
			epoll_ctl(poller, EPOLL_CTL_DEL, f.awaitedFd, null);
		}
		f.result = result;
		readyCnt++;
		tracef("[%x] Fiber resolved with result %d", cast(void*) f, result);
	}
	
	void step() {
		auto t = MonoTime.currTime;
		
		if(readyCnt == 0) {
			foreach(AsyncFiber f; fibers) {
				if(f.waiting && !f.timeout.isNull && f.timeout.get <= t) {
					resolve(f, 0);
				}
			}
		}
		
		if(readyCnt == 0) {
			int timeout = -1;
			foreach(AsyncFiber f; fibers) {
				if(f.waiting && !f.timeout.isNull) {
					int fTimeout = f.msUntilTimeout(t);
					if(timeout == -1 || fTimeout < timeout)
						timeout = fTimeout;
				}
			}
			tracef("[event loop] Polling with timeout %d...", timeout);
			
			epoll_event ev;
			int eventCnt = epoll_wait(poller, &ev, 1, timeout);
			
			if(eventCnt == -1)
				throw new Exception(format("Error during polling: %d", errno));
			
			if(eventCnt == 0) { // timeout
				tracef("[event loop] Polling timed out");
			}
			if(eventCnt == 1) { // event
				foreach(AsyncFiber f; fibers) {
					if(f.waiting && f.awaitedFd == ev.data.fd && (f.events & ev.events) != 0) {
						resolve(f, f.events & ev.events);
					}
				}
			}
			
		} else {
			int i = 0;
			while(i < fibers.length) {
				auto f = fibers[i];
				if(!f.waiting) {
					tracef("[%x] Running fiber...", cast(void*) f);
					readyCnt--;
					f.call();
					if(f.state == Fiber.State.TERM) {
						fibers = remove(fibers, i);
						tracef("[%x] Fiber terminated (total: %d)", cast(void*) f, fibers.length);
						continue;
					}
					tracef("[%x] Fiber yielded with FD %d and timeout %d",
						cast(void*) f, f.awaitedFd, f.msUntilTimeout);
					if(f.awaitedFd != -1) {
						epoll_data_t data;
						data.fd = f.awaitedFd;
						auto ev = epoll_event(f.events, data);
						int res = epoll_ctl(poller, EPOLL_CTL_ADD, f.awaitedFd, &ev);
						if(res == -1)
							throw new Exception(format("Error polling file descriptor: %d", errno));
					}
				}
				i++;
			}
		}
	}
	
	void run() {
		while(fibers.length > 0) {
			step();
		}
	}
}

void asyncRun(void delegate() dg) {
	auto loop = new EventLoop();
	loop.spawn(dg);
	loop.run();
}
