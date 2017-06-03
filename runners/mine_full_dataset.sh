#!/bin/bash
./gradlew shadowJar
if [[ -z "${1}" ]]; then
    echo "No parameters specified"
    echo "Running with default parameters: 1 core, minsup=0.1 maxfreq=0 sampleprobability=1"
    java -Dspark.master="local"  -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.death_mining.EntireDataset
else
    echo "Running with $1 cores"

    if [[ -n "${2}" && -n "${3}" && -n "${4}" ]]; then
        echo "minsup = $2, maxfreq=$3, sampleProb=$4"
        java -Dspark.master="local[$1]"  -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.death_mining.EntireDataset $2 $3 $4
    else
        echo "No parameters specified. Running with default values."
        java -Dspark.master="local[$1]"  -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.death_mining.EntireDataset
    fi
fi