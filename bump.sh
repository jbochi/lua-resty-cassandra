#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <new>"
  exit 1
fi

pat="[0-9]*\.[0-9]*-[0-9]*"
if [[ $1 =~ $pat ]]; then
  echo Bumping to: $1
  sed -i '' -e s/$pat/$1/g src/cassandra.lua
  sed -i '' -e s/$pat/$1/g cassandra*.rockspec
  mv cassandra*.rockspec cassandra-$1.rockspec

  badge_version="${1/-/--}"
  sed -i '' -e s/version-[0-9]*\.[0-9]*--[0-9]*/version-$badge_version/g README.md
else
  echo Invalid version: $1
  exit 1
fi

echo "Don't forget to review the diff and update the CHANGELOG"
