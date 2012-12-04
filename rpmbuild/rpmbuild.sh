#!/bin/sh

cd `dirname $0`
mkdir -p BUILD RPMS SOURCES SRPMS
cd ..
VERSION=`grep "VERSION = '" swiftly/__init__.py | cut -d"'" -f2`
tar zcf rpmbuild/SOURCES/swiftly-${VERSION}.tar.gz --exclude=rpmbuild ./
cd `dirname $0`
rpmbuild -v -ba --clean SPECS/swiftly.spec
