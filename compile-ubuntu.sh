#!/bin/sh
set +x -e

# LDAP
export DEBIAN_FRONTEND=noninteractive
export DEBCONF_NONINTERACTIVE_SEEN=true

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

cd build
mv nordugrid-arc-$ARC_VERSION.tar.gz nordugrid-arc_$ARC_VERSION.orig.tar.gz
tar -z -x -f nordugrid-arc_$ARC_VERSION.orig.tar.gz

cd nordugrid-arc-$TAR_VERSION

echo !!!! BUILDPACKAGE

dpkg-buildpackage -S -us -uc -d
apt-get build-dep ../nordugrid-arc_$ARC_VERSION-1.dsc -y
dpkg-buildpackage -b -us -uc

echo !!!! APTGET

rm -f ../*-dbgsym_*
apt-get install ../*.deb -y
# apt-get install ../nordugrid-arc-client*.deb -y
# apt-get install ../nordugrid-arc-plugins-internal*.deb -y
