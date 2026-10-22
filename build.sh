#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
: "${SPARK_HOME:=/opt/spark}"
mkdir -p target/classes
find target/classes -type f -delete
javac -encoding UTF-8 --release 17 -cp "$SPARK_HOME/jars/*" -d target/classes src/main/java/lab/*.java
jar --create --file target/homework2.jar -C target/classes .
