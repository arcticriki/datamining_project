#!/bin/bash
unzip -o data/DeathRecords.zip -d data/
sed -n 1p data/DeathRecords.csv > data/columns.csv
sed -i 1d data/DeathRecords.csv