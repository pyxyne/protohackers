module log;

import std.logger;
import std.stdio : stdout;
import std.concurrency : Tid;
import std.datetime.systime : SysTime;

immutable string RESET         = "\u001b[0m";
immutable string DIM_WHITE     = "\u001b[37;2m";
immutable string BRIGHT_RED    = "\u001b[31;1m";
immutable string BRIGHT_GREEN  = "\u001b[32;1m";
immutable string BRIGHT_YELLOW = "\u001b[33;1m";

class PrettyLogger : FileLogger {
	string prefix;
	
	this(string prefix = "", LogLevel logLevel = stdThreadLocalLog.logLevel) {
		super(stdout, logLevel);
		this.prefix = prefix;
	}
	
	override protected void beginLogMsg(
			string file, int line, string funcName, string prettyFuncName, string moduleName, LogLevel logLevel,
			Tid threadId, SysTime timestamp, Logger logger) @safe {
		import std.string : indexOf, lastIndexOf;
		import std.conv : to;
		import std.format : formattedWrite;
		ptrdiff_t fnIdx = file.lastIndexOf('/') + 1;
		string datetime = timestamp.toISOExtString();
		size_t timeIdx = datetime.indexOf('T') + 1;
		size_t timeEndIdx = timeIdx + 12;
		if(timeEndIdx > datetime.length) timeEndIdx = datetime.length;
		auto lt = this.file_.lockingTextWriter();
		string msgColor;
		switch(logLevel) {
			case LogLevel.error, LogLevel.critical, LogLevel.fatal:
				msgColor = BRIGHT_RED;
				break;
			case LogLevel.warning:
				msgColor = BRIGHT_YELLOW;
				break;
			case LogLevel.trace:
				msgColor = DIM_WHITE;
				break;
			default:
				msgColor = RESET;
		}
		formattedWrite(lt, "%s%s %s:%u%s%s ",
			DIM_WHITE,
			datetime[timeIdx .. timeEndIdx],
			file[fnIdx .. $], line,
			prefix.length == 0 ? "" : " " ~ prefix,
			msgColor);
	}
	
	override protected void finishLogMsg() {
		this.file_.lockingTextWriter().put(RESET ~ "\n");
    this.file_.flush();
	}
	
	static void setup(LogLevel logLevel = LogLevel.info) {
		stdThreadLocalLog = new PrettyLogger("", logLevel);
	}
}