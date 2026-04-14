#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

mvn -q -DskipTests compile
mvn -q -DincludeScope=runtime -Dmdep.outputFile=/tmp/database_runtime_cp.txt dependency:build-classpath >/dev/null

CP="$(cat /tmp/database_runtime_cp.txt):target/classes"
java -cp "$CP" language_parser.LanguageRepl
