.PHONY: default clean
default: pfs_common.o

%.o: %.cpp %.hpp pfs_config.hpp
	$(CXX) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -f *.o
