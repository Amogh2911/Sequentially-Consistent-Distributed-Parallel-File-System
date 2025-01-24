.PHONY: default clean
default: pfs_api.o pfs_cache.o

%.o: %.cpp %.hpp ../pfs_common/pfs_config.hpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(LDLIBS)

clean:
	rm -f *.o
