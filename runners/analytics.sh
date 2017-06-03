#!/bin/bash
./gradlew shadowJar
if [[ -z "${1}" ]]; then
    java -Dspark.master="local"  -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.death_mining.Analytics
else
    echo "Running with $1 cores"
    java -Dspark.master="local[$1]"  -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.death_mining.Analytics
fi