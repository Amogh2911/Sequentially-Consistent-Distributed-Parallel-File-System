.PHONY: default clean
default: pfs_metaserver pfs_metaserver_api.o

pfs_metaserver: pfs_metaserver.o ../pfs_common/pfs_common.o ../pfs_proto/pfs_metaserver.pb.o ../pfs_proto/pfs_metaserver.grpc.pb.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LDLIBS)

%.o: %.cpp %.hpp ../pfs_common/pfs_config.hpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(LDFLAGS) $(LDLIBS)

clean:
	rm -f pfs_metaserver *.o
