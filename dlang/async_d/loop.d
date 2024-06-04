module async_d.loop;

import std.array : insertInPlace;
import std.algorithm : remove;
import std.datetime : MonoTime, msecs;
import std.format : format;
import std.logger;
import std.math : ceil;
import std.range : assumeSorted;
import std.typecons : Nullable;
import core.stdc.errno;
import core.thread.fiber;

import core.sys.linux.epoll;

int awaitEvent(Nullable!FileEvent fileEvent = Nullable!FileEvent.init, int timeoutMs = -1) {
	auto f = cast(AsyncFiber) Fiber.getThis();
	assert(f !is null, "Can only await inside an AsyncFiber");
	assert(!f.waiting);
	f.fileEvent = fileEvent;
	if(timeoutMs == -1) {
		f.timeout.nullify();
	} else {
		f.timeout = MonoTime.currTime + timeoutMs.msecs;
	}
	f.waiting = true;
	Fiber.yield();
	assert(!f.waiting);
	return f.events;
}

AsyncFiber spawn(void delegate() dg) {
	auto f = cast(AsyncFiber) Fiber.getThis();
	assert(f !is null, "Can only spawn inside an event loop");
	return new AsyncFiber(f.loop, dg);
}

struct FileEvent {
	int fd;
	int eventMask;
}

class AsyncFiber : Fiber {
	EventLoop loop;
	union {
		struct {
			Nullable!FileEvent fileEvent;
			Nullable!MonoTime timeout;
		}
		struct {
			int events;
		}
	}
	bool waiting;
	
	this(EventLoop loop, void delegate() dg) {
		super(dg);
		this.loop = loop;
		waiting = false;
		events = 0;
		
		loop.ready ~= [this];
	}
	
	void resolve(int events) {
		assert(waiting);
		if(!fileEvent.isNull) {
			int fd = fileEvent.get.fd;
			epoll_ctl(loop.poller, EPOLL_CTL_DEL, fd, null);
		}
		this.events = events;
		waiting = false;
		loop.ready ~= [this];
	}
	
	int msUntilTimeout(MonoTime t = MonoTime.currTime) {
		assert(waiting && !timeout.isNull);
		return cast(int) ceil((timeout.get - t).total!"nsecs" / 1_000_000.0);
	}
}

class EventLoop {
	int poller;
	AsyncFiber[] ready;
	AsyncFiber[int] waitingOnFd;
	AsyncFiber[] timers;
	
	this() {
		poller = epoll_create1(0);
	}
	
	int checkTimeouts() {
		auto t = MonoTime.currTime;
		while(timers.length > 0 && timers[0].timeout.get <= t) {
			timers[0].resolve(0);
			timers = timers[1..$];
		}
		return timers.length > 0 ? timers[0].msUntilTimeout(t) : -1;
	}
	
	void run() {
		do {
			while(ready.length > 0) {
				AsyncFiber f = ready[0];
				ready = ready[1..$];
				assert(!f.waiting);
				
				f.call();
				
				if(f.state == Fiber.State.HOLD) {
					if(f.waiting) {
						if(!f.fileEvent.isNull) {
							auto ev = epoll_event(f.fileEvent.get.eventMask, epoll_data_t(cast(void*) f));
							int fd = f.fileEvent.get.fd;
							int res = epoll_ctl(poller, EPOLL_CTL_ADD, fd, &ev);
							waitingOnFd[fd] = f; // to avoid garbage collection
							if(res == -1)
								throw new Exception(format("Error polling file descriptor: %d", errno));
						}
						if(!f.timeout.isNull) {
							size_t i = assumeSorted!"a.timeout.get < b.timeout.get"(timers).lowerBound(f).length;
							timers.insertInPlace(i, f);
						}
					} else { // manual Fiber.yield
						ready ~= [f];
					}
				}
			}
			
			int nextTimeout = checkTimeouts();
			
			epoll_event ev;
			int eventCnt = epoll_wait(poller, &ev, 1, nextTimeout);
			if(eventCnt == -1)
				throw new Exception(format("Error during polling: %d", errno));
			if(eventCnt == 0) { // timeout
				checkTimeouts();
			} else if(eventCnt == 1) {
				AsyncFiber f = cast(AsyncFiber) ev.data.ptr;
				f.resolve(f.events & ev.events);
			}
		} while(ready.length > 0);
	}
}

void asyncRun(void delegate() dg) {
	auto loop = new EventLoop();
	new AsyncFiber(loop, dg);
	loop.run();
}
