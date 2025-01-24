PFS_SUBDIRS = pfs_client pfs_metaserver pfs_fileserver
CLIENT_SUBDIRS = client-1
CLEAN_SUBDIRS = clean-pfs_proto clean-pfs_common $(addprefix clean-,$(PFS_SUBDIRS)) $(addprefix clean-,$(CLIENT_SUBDIRS))

GCC_DIR = /home/software/gcc/gcc-11.3.0
PFS_GRPC_DIR = /home/other/CSE511-FA24/grpc
INC_DIR = -I. -I.. -I$(PFS_GRPC_DIR)/include

CXX = $(GCC_DIR)/bin/g++
CXXFLAGS = "-std=c++17 -Wall $(INC_DIR)"
LDFLAGS = "-L$(GCC_DIR)/lib64 -L$(PFS_GRPC_DIR)/lib64 -L$(PFS_GRPC_DIR)/lib -Wl,-rpath=$(GCC_DIR)/lib64"
LDLIBS = -pthread
GRPC_LDLIBS = "$(LDLIBS) -lprotobuf -labsl_leak_check -labsl_die_if_null -labsl_log_initialize -lutf8_validity -lutf8_range -labsl_strings -lgrpc++ -lgrpc -labsl_statusor -lgpr -labsl_log_internal_check_op -labsl_flags_internal -labsl_flags_reflection -labsl_flags_private_handle_accessor -labsl_flags_commandlineflag -labsl_flags_commandlineflag_internal -labsl_flags_config -labsl_flags_program_name -labsl_raw_hash_set -labsl_hashtablez_sampler -labsl_flags_marshalling -labsl_log_internal_conditions -labsl_log_internal_message -labsl_examine_stack -labsl_log_internal_format -labsl_log_internal_proto -labsl_log_internal_nullguard -labsl_log_internal_log_sink_set -labsl_log_internal_globals -labsl_log_sink -labsl_log_entry -labsl_log_globals -labsl_hash -labsl_city -labsl_low_level_hash -labsl_vlog_config_internal -labsl_log_internal_fnmatch -labsl_random_distributions -labsl_random_seed_sequences -labsl_random_internal_pool_urbg -labsl_random_internal_randen -labsl_random_internal_randen_hwaes -labsl_random_internal_randen_hwaes_impl -labsl_random_internal_randen_slow -labsl_random_internal_platform -labsl_random_internal_seed_material -labsl_random_seed_gen_exception -labsl_status -labsl_cord -labsl_cordz_info -labsl_cord_internal -labsl_cordz_functions -labsl_exponential_biased -labsl_cordz_handle -labsl_crc_cord_state -labsl_crc32c -labsl_crc_internal -labsl_crc_cpu_detect -labsl_bad_optional_access -labsl_strerror -labsl_str_format_internal -labsl_synchronization -labsl_graphcycles_internal -labsl_kernel_timeout_internal -labsl_stacktrace -labsl_symbolize -labsl_debugging_internal -labsl_demangle_internal -labsl_malloc_internal -labsl_time -labsl_civil_time -labsl_strings -labsl_strings_internal -labsl_string_view -labsl_base -lrt -labsl_spinlock_wait -labsl_int128 -labsl_throw_delegate -labsl_time_zone -labsl_bad_variant_access -labsl_raw_logging_internal -labsl_log_severity -lssl -lcrypto -lupb -lcares -lz -lre2 -laddress_sorting -lgrpc++_reflection"

.PHONY: default script pfs_proto pfs_common $(PFS_SUBDIRS) $(CLIENT_SUBDIRS) clean $(CLEAN_SUBDIRS)
default: script $(PFS_SUBDIRS) $(CLIENT_SUBDIRS)

script: launcher.sh
	chmod +x launcher.sh

pfs_proto:
	+$(MAKE) -C $@ CXX=$(CXX) CXXFLAGS=$(CXXFLAGS) PFS_GRPC_DIR=$(PFS_GRPC_DIR)

pfs_common: pfs_proto
	+$(MAKE) -C $@ CXX=$(CXX) CXXFLAGS=$(CXXFLAGS) LDLIBS=$(LDLIBS)

pfs_client: pfs_metaserver pfs_fileserver

$(PFS_SUBDIRS): pfs_proto pfs_common
	+$(MAKE) -C $@ CXX=$(CXX) CXXFLAGS=$(CXXFLAGS) LDFLAGS=$(LDFLAGS) LDLIBS=$(GRPC_LDLIBS)

$(CLIENT_SUBDIRS): pfs_common $(PFS_SUBDIRS)
	+$(MAKE) -C $@ CXX=$(CXX) CXXFLAGS=$(CXXFLAGS) LDFLAGS=$(LDFLAGS) LDLIBS=$(GRPC_LDLIBS)

clean: $(CLEAN_SUBDIRS)
	rm -f *.dump client*.txt output*.txt

$(CLEAN_SUBDIRS): clean-%:
	+$(MAKE) -C $* clean
