# MammothDB Distributed Execution subsystem 

# Version and Release should come from command line, e.g.: --define '_mdb_version 0.32.0' --define '_mdb_release 1'
# If they do not, assume some generic defaults
%{!?_mdb_version:%define _mdb_version 0.0.0}
%{!?_mdb_release:%define _mdb_release 0}

Summary: Distributed Execution subsystem for MammothDB
Name: mammothdb-dexec
Version: %{_mdb_version}
%if "%{?dist:%{dist}}%{!?dist:0}" == ".rel"
Release: %{_mdb_release}%{?dist}.el%{rhel}
%else
Release: 0.%{_mdb_release}%{?dist}.el%{rhel}
%endif
Vendor: MammothDB
URL: http://www.mammothdb.com
Packager: MammothDB <root@mammothdb.com>
Group: MammothDB
License: BSD
BuildArch: x86_64
Requires: mysql-libs
Requires: systemd
BuildRequires: glibc-headers
BuildRequires: mysql-devel
%{?systemd_requires}
BuildRequires: systemd systemd-devel

%description
Distributed Execution subsystem for MammothDB

%prep
#%setup -q

%build

%define _datadir %{_prefix}/share

# Part A. Build the C code
chmod 755 autogen.sh
./autogen.sh
%configure
make

%install
# Part A. Install build from our C soiurces
make install DESTDIR="${RPM_BUILD_ROOT}"

mkdir -p ${RPM_BUILD_ROOT}%{_unitdir}
cp ${RPM_BUILD_DIR}/support-files/systemd/* ${RPM_BUILD_ROOT}%{_unitdir}

mkdir -p ${RPM_BUILD_ROOT}/etc/logrotate.d
cp ${RPM_BUILD_DIR}/support-files/logrotate.d/* ${RPM_BUILD_ROOT}/etc/logrotate.d

mkdir -p ${RPM_BUILD_ROOT}/etc/rsyslog.d
cp ${RPM_BUILD_DIR}/support-files/rsyslog.d/* ${RPM_BUILD_ROOT}/etc/rsyslog.d

mkdir -p ${RPM_BUILD_ROOT}/etc/sysconfig
cp ${RPM_BUILD_DIR}/support-files/sysconfig/* ${RPM_BUILD_ROOT}/etc/sysconfig

%clean
rm -rf $RPM_BUILD_ROOT $RPM_BUILD_DIR

%files
%defattr(-, root, root)
%attr(0755,root,root) /usr/include/mammothdb/*
%attr(0755,root,root) /usr/lib64/*
%attr(0755,root,root) /usr/sbin/*
%attr(0644,root,root) %{_unitdir}/*
%attr(0644,root,root) /etc/logrotate.d/*
%attr(0644,root,root) /etc/rsyslog.d/*
%attr(0644,root,root) %config(noreplace) /etc/sysconfig/*
%{_datadir}/doc/mdb-dexec/

%pre

%post
if [ $1 = 1 ]; then
	# Enable daemon
	systemctl daemon-reload
	systemctl enable mdb-dexec.service

	# Restart rsyslog
	systemctl restart rsyslog
fi

%preun
if [ $1 = 0 ]; then
	%systemd_preun mdb-dexec.service
fi

%postun
if [ $1 = 0 ]; then
	# Restart rsyslog
	systemctl restart rsyslog
fi

# NB: Changelog records the changes in this spec file. For changes in the packaged product, use the ChangeLog file.
%changelog
* Wed Sep 2 2015 MammothDB <root@mammothdb.com>
- Release 0.0.1

