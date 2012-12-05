#!/bin/sh

topdir=`dirname $0`
topdir=`realpath $topdir`
cd ${topdir}/..
VERSION=`grep "VERSION = '" swiftly/__init__.py | cut -d"'" -f2`
mkdir -p rpmbuild/SOURCES
git archive --format tar.gz --prefix swiftly-${VERSION}/ -o rpmbuild/SOURCES/swiftly-${VERSION}.tar.gz master
cd rpmbuild
rpmbuild --define "_topdir $topdir" -v -ba --clean SPECS/swiftly.spec
