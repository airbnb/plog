#!/bin/sh

set -e
cd "`dirname $0`"
groovysh -cp `./gradlew -q :plog-distro:cp`
