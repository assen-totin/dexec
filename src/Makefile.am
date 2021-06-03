includedir = @includedir@/mammothdb

# Daemon
sbin_PROGRAMS = mdb-dexec-daemon
mdb_dexec_daemon_SOURCES = fs.h murmur3.h decimal.h timestamp.h ieee754.h functions-common.h common.h fs.c fifo.c murmur3.c ieee754.c stats.c btree.c common.c decimal.c timestamp.c functions-common.c functions.c messages.c network.c sender.c daemon-thread.c daemon.c
mdb_dexec_daemon_CFLAGS = $(AM_CFLAGS)
if USE_SYSTEMD
mdb_dexec_daemon_LDADD = -lsystemd
endif

# Client library
lib_LTLIBRARIES = libdexecclient.la
libdexecclient_la_SOURCES = fs.h murmur3.h common.h lib.h decimal.h ieee754.h functions-common.h fs.c murmur3.c ieee754.c fifo.c btree.c common.c functions-common.c decimal.c messages.c network.c sender.c lib.c
libdexecclient_la_CFLAGS = -fvisibility=hidden
if USE_SYSTEMD
libdexecclient_la_LIBADD = -lsystemd
endif

include_HEADERS = lib.h

install-data-hook:
	mv $(DESTDIR)$(includedir)/lib.h $(DESTDIR)$(includedir)/dexec.h


