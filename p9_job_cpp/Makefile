CXX = clang++-16

CXXFLAGS = -std=c++2b -stdlib=libstdc++ -Wall -Wextra
LDFLAGS = -lstdc++ -lm

#CPPFLAGS += -O3
CXXFLAGS += -O0 -g
CXXFLAGS += -fsanitize=address
LDFLAGS += -fsanitize=address

%.o: %.cpp *.hpp
p9_job: p9_job.o server.o json.o
	$(CC) -o $@ $^ $(LDFLAGS)

.PHONY: clean
clean:
	rm *.o p9_job
