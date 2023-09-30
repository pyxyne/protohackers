CXXFLAGS = -std=c++20 -Wall -Wextra
LDLIBS += -lfmt

CPPFLAGS += -O3
#CXXFLAGS += -O0 -g
#CXXFLAGS += -fsanitize=address
#LDFLAGS += -fsanitize=address

%.o: %.cpp *.hpp
p9_job: p9_job.o server.o json.o
	$(CXX) $(LDFLAGS) -o $@ $^ $(LDLIBS)

.PHONY: clean
clean:
	rm *.o p9_job
