CXX = g++
BOOST_DIR = src/boost_1_90_0

CXXFLAGS = -I${BOOST_DIR}/ -std=c++20 -Wall -Wextra
LDFLAGS = 

SRC = src/lobos.cpp src/s3http/server.cpp src/index/index.cpp
OBJ = $(SRC:.cpp=.o)

BOOST_LIBS = -L$(BOOST_DIR)/stage/lib -lboost_filesystem -lboost_url

TARGET = lobos

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS) $(BOOST_LIBS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)

.PHONY: all clean