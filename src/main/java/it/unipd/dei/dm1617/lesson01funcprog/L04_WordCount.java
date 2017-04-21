package it.unipd.dei.dm1617.lesson01funcprog;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// In this example, we are going to implement a simple word-count
// program with a functional approach.  In the next lesson, we will
// see an equivalent implementation on MapReduce, and we will develop
// a better one along the lines of the algorithm seen in class.
public class L04_WordCount {

  public static void main(String[] args) {

    // First, let's create a list of documents.
    List<String> documents = new LinkedList<>();
    documents.add("I am the first document.");
    documents.add("One issue with anonymous classes is that if the implementation of your anonymous class is very simple, such as an interface that contains only one method, then the syntax of anonymous classes may seem unwieldy and unclear. In these cases, you're usually trying to pass functionality as an argument to another method, such as what action should be taken when someone clicks a button. Lambda expressions enable you to do this, to treat functionality as method argument, or code as data.");
    documents.add("In 1957, Belgium, France, Italy, Luxembourg, the Netherlands and West Germany signed the Treaty of Rome, which created the European Economic Community (EEC) and established a customs union. They also signed another pact creating the European Atomic Energy Community (Euratom) for co-operation in developing nuclear energy. Both treaties came into force in 1958.");

    // We create the stream of words contained in the documents
    Stream<String> words = documents.stream()
      .flatMap((doc) -> Stream.of(doc.split(" "))) // Here we split the documents into words. The `flatMap` operation flattens a stream of streams into a single stream
      .map((word) -> word.toLowerCase()); // Then we map each word to lower-case

    // The following operations group together all the instances of
    // the same word, and then count the number of occurrences.
    Map<String, Long> counts = words.collect(Collectors.groupingBy(
      Function.identity(),
      Collectors.counting()));

    // Here we are sorting the stream of entries of the word counts
    // map by increasing number of occurences. The last forEach methor
    // applies the given lambda to each element of the stream. The
    // difference with the `map` method is that the result is ignored,
    // and the operation does not result in a new stream.
    counts.entrySet().stream()
      .sorted((a, b) -> a.getValue().compareTo(b.getValue()))
      .forEach((entry) -> {
        System.out.println(entry.getValue() + " :: " + entry.getKey());
      });
  }

}
