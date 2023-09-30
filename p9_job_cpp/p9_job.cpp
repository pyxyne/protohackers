#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <string>
#include <queue>
#include <memory>
#include <optional>
#include <unordered_map>
#include <map>
#include <sstream>

#include <fmt/core.h>

#include "json.hpp"
#include "server.hpp"

struct Queue;

struct Job {
	int id;
	std::shared_ptr<Queue> queue;
	std::unique_ptr<Json> payload;
	int priority;
	int worker_id;
	
	Job(int id, std::shared_ptr<Queue> queue, std::unique_ptr<Json>&& payload, int priority)
		: id(id), queue(std::move(queue)), payload(std::move(payload)), priority(priority), worker_id(-1) {}
};

struct ComparePriority {
	bool operator()(std::weak_ptr<Job> wj1, std::weak_ptr<Job> wj2) {
		auto j1 = wj1.lock();
		auto j2 = wj2.lock();
		if(j1 && j2) {
			return j1->priority < j2->priority;
		} else {
			return (bool) j2;
		}
	}
};
using JobQueue = std::priority_queue<std::weak_ptr<Job>,
	std::vector<std::weak_ptr<Job>>, ComparePriority>;


struct Client;

struct Queue {
	std::string name;
	JobQueue pending;
	std::vector<std::weak_ptr<Client>> waiting;
	
	Queue(std::string name) : name(name) {}
};

uint32_t next_job_id = 0;
std::map<uint32_t, std::shared_ptr<Job>> jobs;
std::unordered_map<std::string, std::weak_ptr<Queue>> queues;

void queue_job(std::shared_ptr<Job> job);

struct Client final : public BaseClient, public std::enable_shared_from_this<Client> {
	std::vector<std::weak_ptr<Job>> working_on;
	std::vector<std::shared_ptr<Queue>> waiting_on;
	
	Client(int id, int fd) : BaseClient(id, fd) {}
	
	virtual void on_connect() {}
	
	void return_job(std::shared_ptr<Job> job) {
		working_on.emplace_back(std::move(job));
		
		std::string esc_name;
		json_escape_str(esc_name, std::string_view(job->queue->name));
		write(fmt::format(R"({{"status":"ok","queue":{},"pri":{},"id":{},"job":{}}})""\n",
			esc_name, job->priority, job->id, *job->payload));
		printf("[%03d] -> Job %d\n", id, job->id);
	}
	
	virtual bool on_request(std::unique_ptr<Json>&& json) {
		if(!waiting_on.empty()) {
			printf("[%03d] Got request while waiting on another!\n", id);
			return true;
		}
		
		Json* type = json->object_get("request");
		if(!type || type->type != JT_STRING) return false;
		
		if(type->vstring == "put") {
			Json* queue_attr = json->object_get("queue");
			if(!queue_attr || queue_attr->type != JT_STRING) return false;
			Json* pri = json->object_get("pri");
			if(!pri || pri->type != JT_NUMBER) return false;
			int pri_int = pri->vnumber;
			if((double) pri_int != pri->vnumber || pri_int < 0) return false;
			Json* job_attr = json->object_get("job");
			auto payload = std::make_unique<Json>(std::move(*job_attr));
			
			auto queue_name = queue_attr->vstring;
			std::shared_ptr<Queue> queue;
			if(queues.contains(queue_name)) {
				queue = queues[queue_name].lock();
			}
			if(!queue) {
				queue = std::make_shared<Queue>(queue_name);
				queues.emplace(queue_name, queue);
			}
			
			uint32_t job_id = next_job_id++;
			auto job = make_shared<Job>(job_id, queue, std::move(payload), pri_int);
			jobs.emplace(job_id, job);
			
			queue_job(std::move(job));
			
			write(fmt::format(R"({{"status":"ok","id":{}}})""\n", job_id));
			printf("[%03d] -> Job %d\n", id, job_id);
			
			return true;
			
		} else if(type->vstring == "get") {
			Json* queue_list = json->object_get("queues");
			if(!queue_list || queue_list->type != JT_ARRAY) return false;
			Json* wait_attr = json->object_get("wait");
			if(wait_attr && wait_attr->type != JT_BOOL) return false;
			bool wait = wait_attr && wait_attr->vbool;
			
			std::shared_ptr<Job> best_job;
			for(auto p = queue_list->varray.begin(); p != queue_list->varray.end(); p++) {
				if(p->type != JT_STRING) return false;
				auto queue_name = p->vstring;
				if(!queues.contains(queue_name)) continue;
				auto queue = queues[queue_name].lock();
				if(!queue) continue;
				
				std::shared_ptr<Job> j;
				while(!j && !queue->pending.empty()) {
					j = queue->pending.top().lock();
					if(!j) queue->pending.pop();
				}
				if(j && (!best_job || j->priority > best_job->priority)) {
					best_job = j;
				}
			}
			
			if(best_job) {
				best_job->queue->pending.pop();
				return_job(std::move(best_job));
				return true;
				
			} else if(wait) {
				for(auto p = queue_list->varray.begin(); p != queue_list->varray.end(); p++) {
					auto queue_name = p->vstring;
					std::shared_ptr<Queue> queue;
					if(queues.contains(queue_name)) {
						queue = queues[queue_name].lock();
					}
					if(!queue) {
						queue = std::make_shared<Queue>(queue_name);
						queues.emplace(queue_name, queue);
					}
					queue->waiting.push_back(weak_from_this());
					waiting_on.push_back(std::move(queue));
				}
				printf("[%03d] Waiting...\n", id);
				return true;
				
			} else {
				printf("[%03d] -> No job\n", id);
				write(R"({"status":"no-job"})""\n");
				return true;
			}
			
		} else if(type->vstring == "abort") {
			Json* id_attr = json->object_get("id");
			if(!id_attr || id_attr->type != JT_NUMBER) return false;
			int job_id = id_attr->vnumber;
			if((double) job_id != id_attr->vnumber) return false;
			
			for(size_t i = 0; i < working_on.size(); i++) {
				auto j = working_on[i].lock();
				if(!j) {
					working_on.erase(working_on.begin() + i);
					i--;
					continue;
				}
				if(j->id == job_id) {
					printf("[%03d] -> Aborted\n", id);
					write(R"({"status":"ok"})""\n");
					working_on.erase(working_on.begin() + i);
					queue_job(std::move(j));
					return true;
				}
			}
			
			printf("[%03d] -> No job\n", id);
			write(R"({"status":"no-job"})""\n");
			return true;
			
		} else if(type->vstring == "delete") {
			Json* id_attr = json->object_get("id");
			if(!id_attr || id_attr->type != JT_NUMBER) return false;
			int job_id = id_attr->vnumber;
			if((double) job_id != id_attr->vnumber) return false;
			
			size_t erased = jobs.erase(job_id);
			
			if(erased > 0) {
				printf("[%03d] -> Deleted\n", id);
				write(R"({"status":"ok"})""\n");
			} else {
				printf("[%03d] -> No job\n", id);
				write(R"({"status":"no-job"})""\n");
			}
			return true;
			
		} else {
			return false;
		}
	}
	
	void wake_up(std::shared_ptr<Job> job) {
		assert(!waiting_on.empty());
		for(auto queue : waiting_on) {
			auto wc = queue->waiting.begin();
			for(; wc != queue->waiting.end(); wc++) {
				if(auto c = wc->lock()) {
					if(c->id == id) break;
				}
			}
			assert(wc != queue->waiting.end());
			queue->waiting.erase(wc);
		}
		waiting_on.clear();
		return_job(std::move(job));
	}
	
	virtual void on_disconnect() {
		if(!working_on.empty()) {
			printf("[%03d] Implicitly aborting %lu jobs\n", id, working_on.size());
		}
		for(auto wj : working_on) {
			if(auto j = wj.lock()) {
				queue_job(std::move(j));
			}
		}
	}
};

void queue_job(std::shared_ptr<Job> job) {
	std::shared_ptr<Client> client;
	while(!client && !job->queue->waiting.empty()) {
		client = job->queue->waiting.front().lock();
		if(!client) job->queue->waiting.erase(job->queue->waiting.begin());
	}
	if(client) {
		client->wake_up(std::move(job));
	} else {
		job->queue->pending.emplace(job);
	}
}

int main() {
	run_server<Client>();
}
