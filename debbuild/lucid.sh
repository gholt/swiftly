#!/bin/sh

cd `dirname $0`
VERSION=`grep "VERSION = '" ../swiftly/__init__.py | cut -d"'" -f2`
mkdir -p lucid_dist/swiftly-$VERSION
cd ..
git archive --format tar --prefix debbuild/lucid_dist/swiftly-${VERSION}/ HEAD | tar x
cd debbuild/lucid_dist/swiftly-$VERSION
mv debbuild/lucid debian
debuild -us -uc -d
