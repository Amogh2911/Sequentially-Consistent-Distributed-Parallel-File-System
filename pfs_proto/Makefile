PROTOC = $(PFS_GRPC_DIR)/bin/protoc
GRPC_CPP_PLUGIN_PATH ?= $(PFS_GRPC_DIR)/bin/grpc_cpp_plugin

PROTO_FILES = pfs_metaserver pfs_fileserver
PROTO_STUB_OUTPUT = $(addsuffix .pb.o,$(PROTO_FILES)) $(addsuffix .grpc.pb.o,$(PROTO_FILES))

.PHONY: default clean
default: $(PROTO_STUB_OUTPUT)

%.o: %.cc %.h
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(LDLIBS)

%.pb.cc %.pb.h: %.proto
	$(PROTOC) --cpp_out=. $<

%.grpc.pb.cc %.grpc.pb.h: %.proto
	$(PROTOC) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

clean:
	rm -f *.pb.cc *.pb.h *.o
