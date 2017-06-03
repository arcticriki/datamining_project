# Death Mining
This repository contains the code for the final project of the
2016/2017 course in Data Mining.

This project consists in applying Association Analysis techniques to a
dataset containing USA's mortality data for the year 2014, obtainable here
https://www.kaggle.com/cdc/mortality

## Requirements

- Java 8
- `sed` command
- `unzip` command
have the Java JDK installed on your system.

### Prepare dataset
After downloading the [dataset](https://www.kaggle.com/cdc/mortality) and
placing the `DeathRecords.zip` file in the `/data` directory, issue the folliwing
shell command
```
./runners/prepare_dataset.sh
```
This command will extract the archive and prepare all the necessary files.


### Mining the entire dataset
If you want to mine the entire dataset just call the following command
```
./runners/mine_full_dataset.sh
```
If no arguments are passed, the code will run on a single core, with the default
parameters for `minsup`, `maxfreq` and `sampleProbability` .

The first argument is always the number of cores you want to use
```
./runners/mine_full_dataset.sh 8
```
If no other argument is passed, the default simulation parameters will be used.
```
./runners/mine_full_dataset.sh 8 0.1 1 0.5
```
This command will mine the dataset using 8 cores, `minsup` = 0.1, `maxfreq`=1 and
sampling the dataset with a probability of `0.5`.

### Mining subgroups
If you want to extract rules for different subgroups, just call the above commands replacing
`./runners/mine_full_dataset.sh` with `./runners/mine_subgroups.sh`.


### Analytics
Generate basic statistics on the dataset by calling
```
./runners/analytics.sh
```
As before, you can pass the number of cores to use as a paramter, e.g.
```
./runners/analytics.sh 8
```