#ifndef JSON_H
#define JSON_H

#include <cstddef>

#include <memory>
#include <string>
#include <span>
#include <unordered_map>
#include <vector>
#include <format>

enum JsonType {
	JT_NULL,
	JT_BOOL,
	JT_NUMBER,
	JT_STRING,
	JT_ARRAY,
	JT_OBJECT,
};

struct Json {
	JsonType type;
	union {
		bool vbool;
		double vnumber;
		std::string vstring;
		std::vector<Json> varray;
		std::unordered_map<std::string, Json> vobject;
	};
	
	Json();
	Json(const Json&);
	Json(Json&&);
	Json& operator=(const Json&);
	Json& operator=(Json&&);
	~Json();
	
	Json* object_get(std::string key);
};

std::unique_ptr<Json> json_parse(std::string_view src);
void json_escape_str(std::string& out, std::string_view s);

template<>
struct std::formatter<Json> {
	constexpr auto parse(std::format_parse_context& ctx) {
		return ctx.begin();
	}
	auto format(const Json& json, std::format_context& ctx) const {
		auto&& out = ctx.out();
		switch(json.type) {
			case JT_NULL:
				return std::format_to(out, "null");
			case JT_BOOL:
				return std::format_to(out, "{}", json.vbool ? "true" : "false");
			case JT_NUMBER:
				return std::format_to(out, "{:.17g}", json.vnumber);
			case JT_STRING: {
				std::string buf;
				json_escape_str(buf, json.vstring);
				return std::format_to(out, "{}", buf);
			}
			case JT_ARRAY: {
				std::format_to(out, "[");
				auto begin = json.varray.begin();
				for(auto p = begin; p != json.varray.end(); p++) {
					if(p != begin) std::format_to(out, ",");
					std::format_to(out, "{}", *p);
				}
				return std::format_to(out, "]");
			}
			case JT_OBJECT: {
				std::format_to(out, "{{");
				auto begin = json.vobject.begin();
				for(auto p = begin; p != json.vobject.end(); p++) {
					if(p != begin) std::format_to(out, ",");
					std::string buf;
					json_escape_str(buf, p->first);
					std::format_to(out, "{}:{}", buf, p->second);
				}
				return std::format_to(out, "}}");
			}
		}
	}
};

#endif
