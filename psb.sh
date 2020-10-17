#!/usr/bin/env bash

# shellcheck disable=SC2155
export VER=$(grep -E '<version>' pom.xml | head -1 | gawk 'match($0,/<version>([^<]*)<\/version>/,a){print a[1]}')
echo "Building version ${VER}"

[ -d dist ] && rm dist/*

nice mvn -DskipTests=true -Dgpg.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true clean install &&
  python setup.py bdist_egg &&
  python setup.py bdist_wheel
