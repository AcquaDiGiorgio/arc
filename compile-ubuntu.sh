#!/bin/sh
set +x -e

# LDAP
export DEBIAN_FRONTEND=noninteractive
export DEBCONF_NONINTERACTIVE_SEEN=true

echo 7.1.2 > VERSION

echo !!!! SETUP

# wget -O /tmp/DEB-GPG-KEY-nordugrid-7.asc  http://download.nordugrid.org/DEB-GPG-KEY-nordugrid-7.asc
# echo "deb [signed-by=/tmp/DEB-GPG-KEY-nordugrid-7.asc] http://download.nordugrid.org/repos/7/ubuntu/ jammy main" | tee /etc/apt/sources.list.d/nordugrid.list
apt-get update --allow-unauthenticated -y

echo <<EOF >> /tmp/slapd.preseed
slapd slapd/domain string ldaptest.local
slapd shared/organization string 'LDAP Test'
slapd slapd/password1 password root_password
slapd slapd/password2 password root_password
slapd slapd/allow_ldap_v2 boolean false
slapd slapd/invalid_config boolean false
EOF

debconf-set-selections /tmp/slapd.preseed

# echo !!!! AUTOGEN

# ./autogen.sh

# mkdir build 
# cd build

# echo !!!! CONFIGURE

# ../configure

# echo !!!! MAKE

# make dist

cd build
mv nordugrid-arc-7.1.2.tar.gz nordugrid-arc_7.1.2.orig.tar.gz
tar -z -x -f nordugrid-arc_7.1.2.orig.tar.gz

cd nordugrid-arc-7.1.2

echo !!!! BUILDPACKAGE

dpkg-buildpackage -S -us -uc -d
apt-get build-dep ../nordugrid-arc_7.1.2-1.dsc -y
dpkg-buildpackage -b -us -uc

echo !!!! APTGET

rm -f ../*-dbgsym_*
apt-get install ../*.deb -y
# apt-get install ../nordugrid-arc-client*.deb -y
# apt-get install ../nordugrid-arc-plugins-internal*.deb -y
