package it.unipd.dei.dm1617.lesson01funcprog;

import java.util.function.BiFunction;

import static it.unipd.dei.dm1617.Utils.pcomment;


public class L02_LambdaFunctions {

  static void operateBinary(int a, int b, BiFunction<Integer, Integer, Integer> fn) {
    System.out.println(fn.apply(a, b));
  }

  public static void main(String[] args) {

    // Lambda functions come in two forms: expression lambdas
    // and statement lambdas.
    //
    // Expression lambdas are used for short functions and
    // are comprised of a single expression.
    pcomment(1, "Expression lambda, for short functions");
    operateBinary(2, 3, (a, b) -> a + b);

    // Statement lambdas are for longer functions and, instead of
    // a single expression, they are built with a sequence of statements
    // delimited by braces (just like any other code block) and
    // ending with a return statement.
    pcomment(2, "Statement lambda, for longer functions");
    operateBinary(2, 3, (a, b) -> {
      int squareA = a * a;
      int squareB = b * b;
      return squareA + squareB;
    });

  }

}
