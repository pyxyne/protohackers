#include "server.hpp"

#include <iostream>

void BaseClient::on_read(std::span<uint8_t> data) {
	size_t prev_length = input.size();
	input.insert(input.end(), data.begin(), data.end());

	auto buffer = input.begin();
	auto line_start = buffer;
	for(auto p = buffer + prev_length; p != input.end(); p++) {
		if(*p == '\n') {
			std::string_view line { line_start, p };
			printf("[%03d] <- ", id);
			std::cout << line << std::endl;
			
			bool ok;
			if(auto json = json_parse(line)) {
				ok = on_request(std::move(json));
			} else {
				ok = false;
			}
			if(!ok) {
				printf("[%03d] -> Error\n", id);
				write("{\"status\":\"error\"}\n");
			}
			
			line_start = p + 1;
		}
	}
	if(line_start != buffer) {
		input.erase(input.begin(), line_start);
	}
}

void BaseClient::write(std::string_view str) const {
	while(str.size() > 0) {
		ssize_t written = ::write(fd, str.data(), str.size());
		if(written < 0) {
			printf("[%03d] Error in write(): %s\n", id, strerror(errno));
			break;
		}
		str = str.substr(written);
	}
}
