# Data Mining example code.

This repository contains examples for the laboratory lessons for the
2016/2017 course in Data Mining.

## Requirements

You will need Java 8 to run the examples and do the project. To verify
that you have a valid setup, issue the following command in a
terminal:

    javac -version

if this results in something along the lines of

    javac 1.8.0_121
    
then you are good to go. Otherwise, if the version is 1.7 or 1.6 then
you have a Java version that needs to be updated. If the command
results in an error like `command not found: javac` then you don't
have the Java JDK installed on your system.

To compile programs you use the command `./gradlew compileJava`
instead of the usual `javac`. Internally, this command calls the
`javac` compiler, but also handles some annoying things like
dependency management, classpath setup, etc.
Notice that the command is `./gradlew` and not simply `gradlew`!
This is because it's a script in the root directory of the project,
and not a program in you system's PATH. This way you don't need to
install additional tools.

If you are on Windows, you should use the `.\gradlew.bat compileJava`
command instead.

## Lesson 1: functional programming basics

### Key points

- Functional programming basics: functions can accept other functions
  as arguments. In fact, every time you pass an anonymous class which
  implements a single method to some function or method, you are
  actually passing a function.
- Java 8 introduces some cleaner notation to deal with this use case,
  namely lambda functions. The syntax for a lambda function with two
  arguments is the following
  
      (arg1, arg2) -> /* Do something with arg1 and arg2g */
  
  Note that the expression after the "arrow" does not end with a
  semicolon. If you need more statements in the body of the function
  then the syntax is as follows
  
      (arg1, arg2) -> {
        // Sequence of statements. Each statement terminates with a
        // semicolon, as usual.
        return ...;
      }
  
- We can conveniently transform collections by using lambda
  functions. This concept of transformation of a collection into
  another through the application of functions will also be used in
  Spark, which will be the topic of the next lesson.
  
### Running the examples

See classes in package `it.unipd.dei.dm2017.lesson01funcprog`. The
examples are numbered in increasing order. To run the examples, use
the bash scripts in the `runners` directory. For example

    runners/lesson01_l03.sh

will compile and run the code for the third example.

### Homework

Run the examples and verify that everything is working on your
machine. If there are errors, verify that your system is set up
appropriately, in particular that your Java compiler is at version 1.8

Bonus: play with the examples, modify them freely and see what happens.

## Lesson 2

In this lesson we study the basics of Spark.

### Key points

- RDDs are distributed collections of data which are _transformed_ by
  applying functions to their elements.
- A chain of transformations defines a _lineage_ and is _lazy_ in that
  it does not compute any result immediately.
- An _action_ is an operation on an RDD that returns some value on the
  central machine, triggering the evaluation of all the lineage.
- Caching is an important optimization technique.
- Use the scala.Tuple2<T1, T2> type to represent key-value pairs.
- In Java we use both JavaRDD and JavaPairRDD, depending on the
  operations we need.
  
### Running the examples.

This time our program needs to have all of Spark classes in the
classpath to run. The simplest way to access all these resources is to
bundle them all in a single jar file. The following command does just
that:

    ./gradlew shadowJar
    
if you are on Windows, use

    .\gradlew.bat shadowJar
    
This command creates a jar with all the needed classes in
`build/libs`, whose complete name is along the lines of

    build/libs/dm1617-1.0-SNAPSHOT-all.jar
    
To run the examples, use the following command lines

    java -Dspark.master="local" -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson02spark.L01_Primes 1000

    java -Dspark.master="local" -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson02spark.L02_WordCount data/wiki-sample.txt
    
    java -Dspark.master="local" -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson02spark.L03_BetterWordCount data/wiki-sample.txt
    
Replace `local` with `local[x]` where `x` is the number of cores of
your machine to run the program in parallel. Add
`-Dspark.default.parallelism=y` before the `-cp` flag to partition the
data in `y` partitions.

### Homework

Modify `it.unipd.dei.dm1617.lesson02spark.L03_BetterWordCount` so to

- Count only the words of length greater than a given l.
- As above, but shorter than l.
- Count only the words that are not stop-words
- .... any other variation you can think of.

## Lesson 3

In this lesson we optimize a couple of Spark programs.

### Key points

- Make the program correct first, fast second
- Perform as much local computation as possible
- Be aware of the traps of garbage collection
- Use broadcast variables if you have to access a large object in your lambda functions

### Running the examples

Compile the code as in lesson 2, then run the following command

    java -Dspark.master="local" -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson02spark.L01_Primes $VERSION $DATA

    java -Dspark.master="local" -cp build/libs/dm1617-1.0-SNAPSHOT-all.jar it.unipd.dei.dm1617.lesson02spark.L01_Primes $VERSION $DATA

where $VERSION is an integer denoting the version of the code you want to run, and
$DATA is the path to the dataset you want to use.

