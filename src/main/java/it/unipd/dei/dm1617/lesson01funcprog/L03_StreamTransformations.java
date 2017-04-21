package it.unipd.dei.dm1617.lesson01funcprog;

import java.util.Vector;
import java.util.stream.Stream;

import static it.unipd.dei.dm1617.Utils.pcomment;

public class L03_StreamTransformations {

  public static void main(String[] args) {

    // First of all, let's define a vector in the
    // three-dimensional euclidean space
    Vector<Integer> vector = new Vector<>();
    vector.add(3);
    vector.add(2);
    vector.add(5);

    // Now, let's compute its norm using plain old for loops.
    double forSumOfSquares = 0.0;
    for (int i=0; i<vector.size(); i++) {
      int elem = vector.get(i);
      forSumOfSquares += elem*elem;
    }
    double forNorm = Math.sqrt(forSumOfSquares);
    pcomment(1, "The norm computed using for loops is " + forNorm);


    // Let's see how we can compute it using lambda functions.
    //
    // First, we need to transform the vector into a stream, which is
    // a collection that can be transformed using lambda functions.
    //
    // *ATTENTION* Stream here is not to be confused with what you
    // have seen in "Streaming algorithms", although streaming
    // algorithms can be implemented adhering to Java's Stream
    // interface
    Stream<Integer> stream = vector.stream();
    // Then, we can use the map and reduce methods of the stream
    // to compute the sum of squares
    double lambdaSumOfSquares = stream
      .map((elem) -> elem*elem) // With the map method, that acts element by element, we square each element.
      .reduce(0, (a, b) -> a + b); // The reduce method allows to aggregate the results of the previous step, yielding a single value.
    double lambdaNorm = Math.sqrt(lambdaSumOfSquares);
    pcomment(2, "The norm computed using the stream API is " + lambdaNorm);

  }


}
