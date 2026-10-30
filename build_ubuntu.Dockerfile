
FROM ubuntu:22.04

RUN apt-get update -y
RUN apt-get install -y \
build-essential \
apt-utils \
emacs \
wget \
unzip \
lsof \
software-properties-common \
autoconf \
autopoint \
libjson-xs-perl \
doxygen \
apt-transport-https \
dpkg-dev \
git \
vim \
curl \
inotify-tools \
# arc dev requirements
libldap2-dev \
libsasl2-dev \
libtool-bin \
pkg-config \
m4 \
gettext \
debconf-utils \
debhelper \
dh-make \
dh-python \
libglibmm-2.4-dev \
nordugrid-arc-dev

WORKDIR /arc

COPY ./test-files/.                     /arc/

COPY ./compile-ubuntu.sh                /arc/compile.sh
COPY ./configure_arc.sh                 /arc/configure.sh

RUN chmod u+x ./compile.sh
RUN chmod u+x ./configure.sh
RUN chmod u+x ./run_group.sh

COPY ./build/nordugrid-arc-7.1.2.tar.gz /arc/build/nordugrid-arc-7.1.2.tar.gz
RUN ./compile.sh
# RUN ./configure.sh