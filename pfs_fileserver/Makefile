.PHONY: default clean
default: pfs_fileserver pfs_fileserver_api.o

pfs_fileserver: pfs_fileserver.o ../pfs_common/pfs_common.o ../pfs_proto/pfs_fileserver.pb.o ../pfs_proto/pfs_fileserver.grpc.pb.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LDLIBS)

%.o: %.cpp %.hpp ../pfs_common/pfs_config.hpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(LDFLAGS) $(LDLIBS)

clean:
	rm -f pfs_fileserver *.o
