module async_d.async;

import std.typecons : Nullable;
import core.thread.fiber : Fiber;

import async_d.loop : AsyncFiber, awaitEvent, FileEvent;

class Waiter {
	AsyncFiber f;
	
	void await(int timeout = -1) {
		f = cast(AsyncFiber) Fiber.getThis();
		awaitEvent(Nullable!FileEvent.init, timeout);
	}
	
	void wake() {
		assert(f !is null, "Waking non-awaited Waiter");
		f.resolve(0);
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
