package it.unipd.dei.dm1617.lesson01funcprog;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;

import static it.unipd.dei.dm1617.Utils.pcomment;

public class L01_FunctionArguments {

  // ============== Data type ==================
  //
  // This is the data type we are going to use throughout this
  // example. It encodes some information about a person.
  //
  // This class contains a simple implementation of the
  // Object.toString method.
  static class Person {
    public Person(String givenName, String familyName, Date birthday) {
      this.givenName = givenName;
      this.familyName = familyName;
      this.birthday = birthday;
    }

    public final String givenName;
    public final String familyName;
    public final Date birthday;

    @Override
    public String toString() {
      return givenName + " " + familyName;
    }
  }

  // 01. We can define this simple function to print a person to the
  // console, using the formatting provided by Person.toString.
  public static void printPerson(Person p) {
    System.out.println(p);
  }

  // 02. We can obtain more flexibility in the formatting by defining
  // a PersonFormatter interface with a single method `format` that
  // takes a single parameter of type Person and returns a string.
  interface PersonFormatter {
    public String format(Person p);
  }

  // Using the interface define above, we can write the following
  // function taking as input both a Person object and a formatter to
  // turn it into a string.
  public static void printPersonWithFormatter(Person p, PersonFormatter formatter) {
    System.out.println(formatter.format(p));
  }

  // This is an example implementation of the formatter (see the main
  // method below for a usage example)
  static class Formatter1 implements PersonFormatter {
    @Override
    public String format(Person p) {
      return p.givenName + " " + p.familyName + " (born on " + p.birthday + ")";
    }
  }

  // This is another PersonFormatter implementation.
  static class Formatter2 implements PersonFormatter {
    @Override
    public String format(Person p) {
      return p.familyName + " was born on " + p.birthday +
        " and was given the name " + p.givenName;
    }
  }

  // The example continues in the main method, at point 4.

  // 5. The interface PersonFormatter is a so-called "functional
  // interface", in that it actually implements a function, in this
  // case taking a single argument of type Person and returning a
  // string.
  //
  // As of Java 8, there is a shorter notation for using interfaces of
  // this kind: lambda functions. The package java.util.function
  // defines some interfaces like `Function` that can be used in
  // method definitions. For an example of how to use it, go to the
  // main method!
  public static void printPersonWithLambda(Person p, Function<Person, String> fn) {
    System.out.println(fn.apply(p));
  }

  public static void main(String[] args) throws ParseException {
    Person pp = new Person(
      "Pinco",
      "Pallino",
      new SimpleDateFormat("yyyy-MM-dd").parse("1980-12-12"));

    pcomment(1, "Function taking just the person and calling .toString");
    printPerson(pp);

    pcomment(2, "Function using the first custom formatter");
    printPersonWithFormatter(pp, new Formatter1());

    pcomment(3, "Function using the second custom formatter");
    printPersonWithFormatter(pp, new Formatter2());

    // Defining a new object for every formatter quickly becomes
    // boring. If we are going to use a given format only a single
    // time, we might as well define the implementation inline, as an anonymous class!
    pcomment(4, "Function printing with an anonymous Formatter instance");
    printPersonWithFormatter(pp, new PersonFormatter() {
      @Override
      public String format(Person p) {
        return "In year " + new SimpleDateFormat("yyyy").format(p.birthday) + " "
          + p.familyName + " " + p.givenName + " was born";
      }
    });
    // The example continues above, on point 5.


    // A lambda function is a very concise notation to define a function inline.
    pcomment(5, "Function printing with lambda function");
    printPersonWithFormatter(
      pp,
      (personArg) -> "Hello, I am " + personArg.givenName + " from lambda functions!");

    // Think about it: we now have functions taking functions as arguments!
  }

}
