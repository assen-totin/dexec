AC_PREREQ([2.63])
AC_INIT([mdb-dexec], [0.0.1], [assen.totin@mammothdb.com])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE([-Wall])

# Fedora 23 shit at its best
m4_pattern_allow([AM_MSG_ERROR])
m4_pattern_allow([AM_PROG_CC_C_O])

# RHEL-6 has older automake wihout this macro, so allow it
m4_pattern_allow([AM_PROG_AR])

# We're going to build a shared library
AC_CONFIG_MACRO_DIR([m4])
AM_PROG_AR
LT_INIT

# prefix
if test "x$prefix" = "xNONE" ; then
	prefix="/usr/local"
fi
if test "x$exec_prefix" = "xNONE" ; then
	exec_prefix=$prefix
fi
PREFIX=$prefix
AC_SUBST([PREFIX])

BINDIR=$exec_prefix/bin
AC_SUBST([BINDIR])
SBINDIR=$exec_prefix/sbin
AC_SUBST([SBINDIR])
DATAROOTDIR=$prefix/share
AC_SUBST([DATAROOTDIR])
LIBEXECDIR=$prefix/libexec
if test "$libexecdir" != '${exec_prefix}/libexec'; then
        LIBEXECDIR=$libexecdir
fi
AC_SUBST([LIBEXECDIR])

if test "x$localstatedir" = "xNONE" ; then
	LOCALSTATEDIR="$prefix/var"
elif test "x$localstatedir" = 'x${prefix}/var' ; then
	LOCALSTATEDIR="$prefix/var"
else
	LOCALSTATEDIR="$localstatedir"
fi
SOCKDIR=$LOCALSTATEDIR/lib/mammothdb
AC_DEFINE_UNQUOTED([SOCKDIR],["$SOCKDIR"],[Socket directory files])

LOCALEDIR=$prefix/share/locale
AC_SUBST([LOCALEDIR])
AC_DEFINE_UNQUOTED([LOCALEDIR],["$LOCALEDIR"],[Locale files])

# gettext
AM_GNU_GETTEXT_VERSION(0.17)
#AM_GNU_GETTEXT_VERSION(0.19)
AM_GNU_GETTEXT([external])
if test $USE_NLS != "yes" ; then
        echo "*** gettext not found. You're losing internationalization."
fi
AC_DEFINE_UNQUOTED([LOCALEDIR], ["$LOCALEDIR"], ["Locale directory"])

# C compiler in C99 mode
AC_PROG_CC
AC_PROG_CC_STDC
AM_PROG_CC_C_O

# Check for SystemD
echo -n "Checking for SystemD... "
if test -e /usr/lib/systemd ; then
	HAVE_SYSTEMD=1
	AC_DEFINE_UNQUOTED([HAVE_SYSTEMD],[$HAVE_SYSTEMD],[SystemD is present])
	echo "yes"
else
	echo "no"
fi
AM_CONDITIONAL([USE_SYSTEMD], [test x$HAVE_SYSTEMD != 'x'])

# Check for threads
AX_PTHREAD([
	LIBS="$PTHREAD_LIBS $LIBS"
	CFLAGS="$CFLAGS $PTHREAD_CFLAGS"
	CC="$PTHREAD_CC"
	], [AC_MSG_ERROR(["*** Could not find pthreads support"])
])

# Check for MySQL
AC_MSG_CHECKING(for mysql_config executable)
mysql_config > /dev/null 2>&1
if test $? -gt 1 ; then		# NB: Older versions exit with 1 when invoked without params; missing binary will return 127
	echo 
	echo "*** MySQL config utility not found."
	exit 1
fi
echo "yes"
CPPFLAGS="$ADDFLAGS "`mysql_config --cflags`
LDFLAGS="$LDFLAGS "`mysql_config --libs_r`

# Chek for MammothDB stats header
AC_CHECK_HEADER([mammothdb/stats.h],[
	AC_DEFINE_UNQUOTED([HAVE_MDB_STATS], ["1"], ["MammothDB stats found"])
],[
	echo "*** MammothDB stats is disabled."
])

# Makefiles
AC_CONFIG_FILES([Makefile src/Makefile po/Makefile.in po/Makefile])

# Grand finale
AC_OUTPUT

echo
echo "Configuration complete."
echo "Now, run make."
echo 

