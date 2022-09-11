#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <vector>
#include <csignal>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>

constexpr uint16_t PORT = 50000;

volatile sig_atomic_t stop_server = 0;
void sigint_handler(int s) {
	stop_server = 1;
}

void print_addr(struct sockaddr_in* addr) {
	uint32_t ip = ntohl(addr->sin_addr.s_addr);
	printf("%d.%d.%d.%d:%hu", ip >> 24, ip >> 16 & 0xff, ip >> 8 & 0xff, ip & 0xff, ntohs(addr->sin_port));
}

int listener_fd = -1;
void error(const char* msg) {
	if(listener_fd >= 0)
		close(listener_fd);
	perror(msg);
	exit(EXIT_FAILURE);
}

int main(int argc, char *argv[]) {
	// Block SIGINT until we are in ppoll
	sigset_t mask_empty, mask_sigint;
	sigemptyset(&mask_empty);
	sigemptyset(&mask_sigint);
	sigaddset(&mask_sigint, SIGINT);
	sigprocmask(SIG_BLOCK, &mask_sigint, NULL);
	
	// Setup SIGINT handler
	struct sigaction sig_action;
	sig_action.sa_handler = sigint_handler;
	sig_action.sa_flags = 0;
	sigemptyset(&sig_action.sa_mask);
	sigaction(SIGINT, &sig_action, NULL);
	
	// Ignore SIGPIPE signal
	struct sigaction sig_ignore;
	sig_ignore.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sig_ignore, NULL);
	
	// Create listener socket
	listener_fd = socket(AF_INET, SOCK_STREAM, 0);
	if(listener_fd < 0)
		error("Error opening listener socket");
	struct sockaddr_in listener_addr;
	listener_addr.sin_family = AF_INET;
	listener_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	listener_addr.sin_port = htons(PORT);
	if(bind(listener_fd, (struct sockaddr*) &listener_addr, sizeof(listener_addr)) < 0)
		error("Error on bind");
	if(listen(listener_fd, 5) < 0)
		error("Error on listen");
	
	setlinebuf(stdout);
	printf("Listening for connections on port %hu.\n", PORT);
	
	std::vector<struct sockaddr_in> sock_addrs;
	std::vector<struct pollfd> poll_fds;
	sock_addrs.push_back(listener_addr);
	poll_fds.push_back({ listener_fd, POLLIN, 0 });
	
	while(!stop_server) {
		// Wait for events (or signal)
		int res = ppoll(poll_fds.data(), poll_fds.size(), NULL, &mask_empty);
		if(res < 0) { // Error
			if(errno == EINTR) continue; // Signal has been handled
			error("Error in ppoll");
		}
		
		for(unsigned i = 0; i < poll_fds.size(); i++) {
			short revents = poll_fds[i].revents;
			if(revents == 0) continue; // No event for this socket
			int sock_fd = poll_fds[i].fd;
			struct sockaddr_in* addr = &sock_addrs[i];
			
			bool reached_eof = false;
			if(revents & POLLIN) {
				if(i == 0) { // Listener socket → incoming connection
					struct sockaddr_in client_addr;
					unsigned int client_addr_len = sizeof(client_addr);
					int new_sock_fd = accept(listener_fd, (struct sockaddr*) &client_addr, &client_addr_len);
					if(new_sock_fd < 0)
						error("Error on accept");
					printf("Accepted connection from ");
					print_addr(&client_addr);
					printf("\n");
					
					sock_addrs.push_back(client_addr);
					poll_fds.push_back({ new_sock_fd, POLLIN, 0 });
					
				} else { // Client socket → incoming data
					// Read incoming data
					char buffer[1024];
					uint64_t total_read = 0;
					while(true) {
						int bytes_read = read(sock_fd, &buffer, sizeof(buffer));
						if(bytes_read < 0) 
							error("Error reading from socket");
						
						if(bytes_read > 0) {
							total_read += bytes_read;
							
							// Write data back
							if(write(sock_fd, &buffer, bytes_read) < 0)
								error("Error writing to socket");
						}
						
						if(bytes_read < (int) sizeof(buffer))
							break;
					}
					
					if(total_read == 0) { // EOF
						reached_eof = true;
						
					} else {
						printf("[");
						print_addr(addr);
						printf("] ");
						printf("Bounced %lu bytes\n", total_read);
					}
				}
			}
			
			if(reached_eof || (revents & POLLERR)) { // Connection closed or errorred
				printf("[");
				print_addr(addr);
				printf("] ");
				if(reached_eof) {
					printf("Socket closed\n");
				} else if(revents & POLLERR) {
					printf("Socket errorred\n");
				}
				
				if(i == 0) {
					stop_server = true;
					break;
				}
				
				if(close(sock_fd) < 0)
					error("Error closing socket");
				
				sock_addrs.erase(sock_addrs.begin() + i);
				poll_fds.erase(poll_fds.begin() + i);
				i--;
			}
		}
	}
	
	printf("Stopping server...\n");
	for(unsigned i = 0; i < poll_fds.size(); i++) {
		int sock_fd = poll_fds[i].fd;
		if(close(sock_fd) < 0)
			error("Error closing socket");
	}
	
	return EXIT_SUCCESS;
}
