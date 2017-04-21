#!/bin/bash
./gradlew shadowJar
java -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson01funcprog.L04_WordCount
