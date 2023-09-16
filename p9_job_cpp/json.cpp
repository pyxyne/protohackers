#include "json.hpp"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <optional>

bool is_space(char c) {
	return c == ' ' || c == '\x0A' || c == '\x0D' || c == '\x09';
}
bool is_hex(char c) {
	return (c >= '0' && c <= '9')
		|| (c >= 'A' && c <= 'F')
		|| (c >= 'a' && c <= 'f');
}
uint16_t parse_hex(char c) {
	if(c >= 'A' && c <= 'F') {
		return (uint16_t)(c - 'A') + 0xA;
	} else if(c >= 'a' && c <= 'f') {
		return (uint16_t)(c - 'a') + 0xA;
	} else {
		return c - '0';
	}
}
bool is_number_comp(char c) {
	return (c >= '0' && c <= '9')
		|| c == '-' || c == '+'
		|| c == '.'
		|| c == 'e' || c == 'E';
}

struct Parser {
	std::string_view src;
	Json* slot;

	std::optional<std::string> parse_str() {
		std::string buf;
		bool escaped = false;
		while(true) {
			if(src.empty()) {
				printf("[json] Unclosed string\n");
				return {};
			}
			if(escaped) {
				if(src[0] == '\\' || src[0] == '"' || src[0] == '/') {
					buf += src[0];
				} else if(src[0] == 'b') {
					buf += '\b';
				} else if(src[0] == 'f') {
					buf += '\f';
				} else if(src[0] == 'n') {
					buf += '\n';
				} else if(src[0] == 'r') {
					buf += '\r';
				} else if(src[0] == 't') {
					buf += '\t';
				} else if(src[0] == 'u') {
					uint16_t cp = 0;
					for(int i = 0; i < 4; i++) {
						src = src.substr(1);
						if(src.empty()) {
							printf("[json] Unfinished escape sequence\n");
							return {};
						}
						if(!is_hex(src[0])) {
							printf("[json] Invalid hexadecimal digit\n");
							return {};
						}
						cp = (cp << 4) | parse_hex(src[0]);
					}
					char c = cp;
					if(cp > 0x7f) {
						printf("[json] Unexpected Unicode escape sequence\n");
						c = '?';
					}
					buf += c;
				} else {
					printf("[json] Invalid escape sequence\n");
					return {};
				}
				escaped = false;
			} else {
				if(src[0] == '\\') {
					escaped = true;
				} else if(src[0] == '"') {
					src = src.substr(1);
					break;
				} else {
					buf += src[0];
				}
			}
			src = src.substr(1);
		}
		
		buf.shrink_to_fit();
		return buf;
	}

	bool parse_terminal() {
		if(src[0] == '"') {
			src = src.substr(1);
			if(auto str = parse_str()) {
				slot->type = JT_STRING;
				new(&slot->vstring) std::string(std::move(*str));
			} else {
				return false;
			}
			
		} else if((src[0] >= '0' && src[0] <= '9') || src[0] == '-') {
			auto num_start = src.begin();
			while(!src.empty() && is_number_comp(src[0])) src = src.substr(1);
			size_t num_len = src.begin() - num_start;
			char* buf = new char[num_len + 1];
			memcpy(buf, num_start, num_len);
			buf[num_len] = '\0';
			char* end;
			double val = strtod(buf, &end);
			if(end != buf + num_len) {
				printf("[json] Invalid number\n");
				delete[] buf;
				return false;
			}
			delete[] buf;
			
			slot->type = JT_NUMBER;
			slot->vnumber = val;
			
		} else if(src[0] == 't') {
			if(!src.starts_with("true")) {
				printf("[json] Expected true\n");
				return false;
			}
			src = src.substr(4);
			slot->type = JT_BOOL;
			slot->vbool = true;
			
		} else if(src[0] == 'f') {
			if(!src.starts_with("false")) {
				printf("[json] Expected false\n");
				return false;
			}
			src = src.substr(5);
			slot->type = JT_BOOL;
			slot->vbool = false;
			
		} else if(src[0] == 'n') {
			if(!src.starts_with("null")) {
				printf("[json] Expected null\n");
				return false;
			}
			src = src.substr(4);
			slot->type = JT_NULL;
			
		} else {
			printf("[json] Unexpected character: '%c'\n", src[0]);
			return false;
		}
		
		return true;
	}

	void skip_ws() {
		while(!src.empty() && is_space(src[0])) {
			src = src.substr(1);
		}
	}

	bool parse_key(Json* object) {
		auto key = parse_str();
		if(!key) return false;
		skip_ws();
		
		if(src.empty() || src[0] != ':') {
			printf("[json] Expected ':'\n");
			return false;
		}
		src = src.substr(1);
		skip_ws();
		
		auto res = object->vobject.emplace(std::move(*key), Json());
		
		slot = &(*res.first).second;
		return true;
	}
};

std::unique_ptr<Json> json_parse(std::string_view src) {
	auto res = std::make_unique<Json>();
	
	Parser p;
	p.src = src;
	p.slot = &*res;
	
	std::vector<Json*> stack;
	
	p.skip_ws();
	while(p.slot) {
		if(p.src.empty()) {
			printf("[json] Unexpected EOF\n");
			return nullptr;
		}
		
		if(p.src[0] == '[') {
			p.src = p.src.substr(1);
			p.skip_ws();
			if(p.src.empty()) {
				printf("[json] Unclosed array\n");
				return nullptr;
			}
			
			p.slot->type = JT_ARRAY;
			new(&p.slot->varray) std::vector<Json>();
			
			if(p.src[0] == ']') {
				p.src = p.src.substr(1);
				p.skip_ws();
				
			} else {
				stack.push_back(p.slot);
				p.slot = &p.slot->varray.emplace_back();
				continue;
			}
		
		} else if(p.src[0] == '{') {
			p.src = p.src.substr(1);
			p.skip_ws();
			if(p.src.empty()) {
				printf("[json] Unclosed object\n");
				return nullptr;
			}
			
			p.slot->type = JT_OBJECT;
			new(&p.slot->vobject) std::unordered_map<std::string, Json>();
			
			if(p.src[0] == '}') {
				p.src = p.src.substr(1);
				p.skip_ws();
				
			} else if(p.src[0] == '"') {
				p.src = p.src.substr(1);
				stack.push_back(p.slot);
				if(!p.parse_key(p.slot)) return nullptr;
				continue;
				
			} else {
				printf("[json] Expected '}' or '\"'\n");
				return nullptr;
			}
			
		} else if(p.parse_terminal()) {
			p.skip_ws();
		} else {
			return nullptr;
		}
		
		// Finished filling slot, find next slot
		p.slot = nullptr;
		while(!p.slot && stack.size() > 0) {
			if(p.src.empty()) {
				printf("[json] Unclosed container\n");
				return nullptr;
			}
			
			auto container = stack.back();
			if(container->type == JT_ARRAY) {
				if(p.src[0] == ']') {
					container->varray.shrink_to_fit();
					stack.pop_back();
				} else if(p.src[0] == ',') {
					p.slot = &container->varray.emplace_back();
				} else {
					printf("[json] Expected ']' or ','\n");
					return nullptr;
				}
				p.src = p.src.substr(1);
				p.skip_ws();
				
			} else if(container->type == JT_OBJECT) {
				if(p.src[0] == '}') {
					p.src = p.src.substr(1);
					p.skip_ws();
					container->vobject.rehash(0);
					stack.pop_back();
				} else if(p.src[0] == ',') {
					p.src = p.src.substr(1);
					p.skip_ws();
					if(p.src.empty() || p.src[0] != '"') {
						printf("[json] Expected '\"'\n");
						return nullptr;
					}
					p.src = p.src.substr(1);
					if(!p.parse_key(container)) return nullptr;
				} else {
					printf("[json] Expected '}' or ','\n");
					return nullptr;
				}
				
			} else {
				assert(false);
			}
		}
	}
	
	if(!p.src.empty()) {
		printf("[json] Expected EOF\n");
		return nullptr;
	}
	
	return res;
}

void json_escape_str(std::string& out, std::string_view s) {
	out.push_back('\"');
	for(auto p = s.begin(); p != s.end(); p++) {
		if(*p == '\\') {
			out += "\\\\";
		} else if(*p == '"') {
			out += "\\\"";
		} else if(*p == '\n') {
			out += "\\n";
		} else if(*p == '\r') {
			out += "\\r";
		} else if(*p == '\t') {
			out += "\\t";
		} else if(*p < 0x20) {
			out += "\\u00";
			char c = *p >> 4;
			c += c < 0xa ? '0' : 'A';
			out.push_back(c);
			c = *p & 0xf;
			c += c < 0xa ? '0' : 'A';
			out.push_back(c);
		} else {
			out.push_back(*p);
		}
	}
	out.push_back('\"');
}

Json::Json() : type(JT_NULL) {}
Json::Json(const Json& other) : type(other.type) {
	switch(type) {
	case JT_STRING:
		new(&vstring) std::string(other.vstring);
		break;
	case JT_ARRAY:
		new(&varray) std::vector<Json>(other.varray);
		break;
	case JT_OBJECT:
		new(&vobject) std::unordered_map<std::string, Json>(other.vobject);
		break;
	default:
		break;
	}
}
Json::Json(Json&& other) : type(other.type) {
	switch(type) {
	case JT_STRING:
		new(&vstring) std::string(std::move(other.vstring));
		break;
	case JT_ARRAY:
		new(&varray) std::vector<Json>(std::move(other.varray));
		break;
	case JT_OBJECT:
		new(&vobject) std::unordered_map<std::string, Json>(std::move(other.vobject));
		break;
	default:
		break;
	}
}
Json& Json::operator=(const Json& other) {
	this->~Json();
	new(this) Json(other);
	return *this;
}
Json& Json::operator=(Json&& other) {
	this->~Json();
	new(this) Json(std::move(other));
	return *this;
}

Json::~Json() {
	switch(type) {
	case JT_STRING:
		vstring.~basic_string<char>();
		break;
	case JT_ARRAY:
		varray.~vector<Json>();
		break;
	case JT_OBJECT:
		vobject.~unordered_map<std::string, Json>();
		break;
	default:
		break;
	}
	type = JT_NULL;
}

Json* Json::object_get(std::string key) {
	if(type != JT_OBJECT) return nullptr;
	auto it = vobject.find(key);
	if(it == vobject.end()) return nullptr;
	return &it->second;
}

