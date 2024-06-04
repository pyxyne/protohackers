#ifndef SERVER_H
#define SERVER_H

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <memory>
#include <span>
#include <optional>

#include "json.hpp"

constexpr uint16_t PORT = 50000;

struct BaseClient {
	int id;
	int fd;
	std::string input;
	
	BaseClient(int id, int fd) : id(id), fd(fd) {}
	
	virtual void on_connect() {}
	virtual bool on_request(__attribute__((unused)) std::unique_ptr<Json>&& json) {
		return false;
	}
	virtual void on_disconnect() {}
	
	void on_read(std::span<uint8_t> data);
	void write(std::string_view str) const;
};

inline void error(const char* msg) {
	perror(msg);
	exit(EXIT_FAILURE);
}

template<typename Client>
void run_server() {
	int listener_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(listener_fd < 0) error("Error in socket()");
	int opt_val = 1;
	if(setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR, &opt_val, sizeof(int)) < 0)
		error("Error in setsockopt()");
	sockaddr_in listener_addr;
	listener_addr.sin_family = AF_INET;
	listener_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	listener_addr.sin_port = htons(PORT);
	if(bind(listener_fd, (sockaddr*) &listener_addr, sizeof(listener_addr)) < 0)
		error("Error in bind()");
	if(listen(listener_fd, 1500) < 0) error("Error in listen()");
	
	printf("Listening for connections on port %hu.\n", PORT);
	
	struct sigaction act;
	act.sa_handler = SIG_IGN;
	act.sa_flags = 0;
	sigemptyset(&act.sa_mask);
	sigaction(SIGPIPE, &act, nullptr);
	
	std::vector<pollfd> fds;
	std::vector<std::shared_ptr<Client>> clients;
	fds.push_back(pollfd { .fd = listener_fd, .events = POLLIN, .revents = 0 });
	clients.emplace_back();
	
	while(true) {
		int res = poll(fds.data(), fds.size(), -1);
		if(res < 0) error("Error in poll()");
		for(size_t i = 0; i < fds.size(); i++) {
			int fd = fds[i].fd;
			int revents = fds[i].revents;
			if(fd == -1 || revents == 0) continue;
			
			bool close_sock = 0;
			if(revents & POLLIN) {
				if(fd == listener_fd) {
					// Incoming connection
					sockaddr_in client_addr;
					unsigned client_addr_len = sizeof(client_addr);
					int client_fd = accept(listener_fd, (sockaddr*) &client_addr, &client_addr_len);
					if(client_fd < 0) error("Error in accept()");
					
					size_t j;
					for(j = 1; j < fds.size() && fds[j].fd != -1; j++);
					if(j == fds.size()) {
						fds.push_back(pollfd { .fd = client_fd, .events = POLLIN, .revents = 0 });
						clients.emplace_back();
					} else {
						fds[j].fd = client_fd;
					}
					clients[j] = std::make_shared<Client>(j, client_fd);
					
					uint32_t ip = ntohl(client_addr.sin_addr.s_addr);
					uint16_t port = ntohs(client_addr.sin_port);
					printf("[%03lu] Accepted connection from %d.%d.%d.%d:%hu\n",
						j, ip >> 24, ip >> 16 & 0xff, ip >> 8 & 0xff, ip & 0xff, port);
						
					clients[j]->on_connect();
					
				} else {
					// Incoming data
					Client& client = *clients[i];
					uint8_t buffer[1024];
					uint64_t at_eof = 1;
					while(true) {
						int bytes_read = read(fd, &buffer, sizeof(buffer));
						if(bytes_read < 0) {
							printf("[%03lu] Error in read(): %s\n", i, strerror(errno));
							close_sock = true;
							break;
						}
						if(bytes_read > 0) {
							at_eof = 0;
							client.on_read(std::span(buffer, bytes_read));
						}
						if(bytes_read < (int) sizeof(buffer))
							break;
					}
					if(at_eof) {
						printf("[%03lu] Reached EOF\n", i);
						close_sock = true;
					}
				}
			}
			if(revents & (POLLERR|POLLNVAL)) {
				int error = 0;
				socklen_t errlen = sizeof(error);
				getsockopt(fd, SOL_SOCKET, SO_ERROR, (void*) &error, &errlen);
				printf("[%03lu] Socket errored: %s\n", i, strerror(error));
				close_sock = true;
			}
			if(revents & POLLHUP) {
				printf("[%03lu] Socket closed\n", i);
				close_sock = true;
			}
			
			if(close_sock) {
				if(close(fd) < 0) error("Error in close()");
				fds[i].fd = -1;
				if(i == 0) {
					printf("Listening socket closed.\n");
					exit(EXIT_FAILURE);
				}
				clients[i]->on_disconnect();
				clients[i].reset();
			}
			
			fds[i].revents = 0;
			if(--res == 0) break;
		}
	}
}

#endif
