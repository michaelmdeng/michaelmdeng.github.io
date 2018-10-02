---
layout: post
title:  "Functional pipelines in Spark and Scala"
---

I've gotten bored of the traditional script-like style that many ETL pipelines in Apache
Spark are written in. Since Spark jobs run as main methods of objects in Scala, this code
often looks like a recipe: a series of numbered transformation steps that eventually
result in the output. And while this is effective for cooking, where progress mostly
occurs linearly and the dependencies can be traced out as a single line, many data
transformations are more complex and involve an interconnected web of dependencies.
Writing these transformations as a procedure can mask these dependencies, and make it
hard to identify and fix bugs.

I've been wondering if there was a more expressive way to write these ETL pipelines, one
that made them not only easier to understand, but also easier to test and componentize.
Let's start with a quick example of what I'm trying to address.

{% highlight scala linenos %}
def main(args: Array[String]): Unit = {
  val spark: SparkSession = // get a SparkSession

  val a: A = spark.read(args, ...)
  val b: B = doSomething(b)
  val c: C = doSomethingElse(spark, a, b)

  c.write(args, ...) // and so on...
}
{% endhighlight %}

This main method describes a Spark job that reads in an input dataset, applies a couple
of transformations to it, and the writes the output. This, of course, is a simplified
model of my ETL jobs, but it will be sufficient to demonstrate my new approach. Taking a
closer look at this job, we see that:

* The `a` transformation requires a `SparkSession` and command-line arguments
* The `b` transformation only requires the output `a` from the previous step
* The `c` transformation requires both previous outputs `a`, and `b`, and the
`SparkSession`
* The `write` transformation requires the `c` output and the arguments

It is trivial in this example, but for larger jobs keeping track of which step requires
which inputs and where those inputs come from can get ugly very quickly.  This coding
style does work, but it often produces highly-coupled, hard-to-read and
hard-to-understand code. The data transformations are difficult to change and isolate,
since each statement is dependent on the previous statements, often in different and
subtle ways.

# Gathering our dependencies

Each of the data transformations in the example can depend on previous outputs,
information from the environment/program context, or both. In this scripting style, these
dependencies can be either implicit, like the dependence on the `SparkSession` to read in
a file, or explicit, like passing in previous outputs directly as arguments to the
transformations. The environment/program context that transformations depend on include
the `SparkSession`, command-line arguments, logging, and possibly much more.

Not only must we keep track of the outputs of the transformations and how they are used
in subsequent steps, we must also keep track of the environment context, how it is passed
between steps, and how steps modify or change the context. In addition, changes to the
context are often made implicitly through side-effecting calls, making it even harder to
keep track of how the context is changed.

If we consider the environment context from a functional programming perspective, we can
view it as a form of dependency injection. Our dependencies take the form of an
`Environment`, which encompasses all of the surrounding information each data
transformation might need.

# Functions, first and foremost

If the `Environment` is just another dependency we can pass to our data transformations,
we can express each of our transformations as functions and make the dependence explicit
by passing the environment in as an argument. With this in mind, we can rewrite our Spark
program as:

{% highlight scala linenos %}
def main(args: Array[String]): Unit = {
  val myEnv: Environment = // create this from the SparkSession, args, etc.

  val aTransform = (env: Environment) => env.spark.read(env.args, ...)
  val bTransform = (aResult: A) => doSomething(aResult)
  val cTransform = (env: Environment, aResult: A, bResult: B) => doSomethingElse(env.spark, aResult, bResult)
  val writeTransform = (env: Environment, cResult: C) => cResult.write(env.args, ...)

  val a: A = aTransform(myEnv)
  val b: B = bTransform(a)
  val c: C = cTransform(myEnv, a, b)
  writeTransform(myEnv, c)
}
{% endhighlight %}

By expressing each transformation as a function, we've made progress in two important
ways.

1. We have made our dependency on the surrounding environment explicit. If any of the
transformations modify the environment or require a modified environment, we will see it
right away in our function call.
2. We have componentized our code. The implementation of each transformation function is
completely independent of the other transformations.

However, this code also requires more boilerplate, since we define both the
transformations and the transformation calls. You'll notice that all of the
transformations looks very similar - they all rely on some combination of the `env` and
some other typed parameters.

# FP FTW

Some of you may have already recognized this form of dependency injection as a `Reader`
monad. **_A `Reader` wraps an anonymous function_** and takes two typed parameters, one
representing the input type and the other representing the output type.

{% highlight scala linenos %}
val f: A => B = // a function that transforms an A to a B
val reader: Reader[A, B] = Reader.apply(f)
{% endhighlight %}

You can apply the anonymous function just by calling the `run` method:
`reader.run(a)`.

The `Reader` also exposes a helpful method `ask`, which wraps an anonymous function that
just returns the input. This means the following two statements are equivalent.

{% highlight scala linenos %}
reader.ask
Reader.apply(a => a)
{% endhighlight %}

What have we gained by wrapping our anonymous function inside the `Reader`?  The `Reader`
is useful because it is a **_monad_** and can be chained using map/flatMap and for/yield
syntax.

{% highlight scala linenos %}
val aFcn: A => B = a => getB(a)
val reader = Reader[A, B](aFcn)

val bFcn: B => C = b => getB(b)
val chainedReader = for (b <- reader) yield (bFcn(b))
{% endhighlight %}

The `chainedReader` value wraps an anonymous function that converts an input of type `A`
into an output of type `C`. Calling `chainedReader.run(a)` will first call `aFcn` to
generate an output of type `B`, and then call `bFcn` to generate the final output of type
`C`.

This chaining process is very similar to the way our data transformations Spark form a
pipeline. Each ETL pipeline consists of a set of transformations , some of which are
inputs to other transformations, along with an `Environment` that may also be an
input to some of the transformations.

Let's try rewriting our Spark ETL job as a series of chained `Reader` monads.

{% highlight scala linenos %}
def main(args: Array[String]): Unit = {
  val aReader: Reader[Environment, A] =
    Reader(env: Environment => env.spark.read(env.args, ...))

  val bReader: Reader[Environment, B] =
    for {
      a <- aReader
    } yield {
      doSomething(a)
    }

  val cReader: Reader[Environment, C] =
    for {
      a <- aReader
      b <- bReader
      env <- bReader.ask
    } yield {
      doSomethingElse(env, a, b)
    }

  val writeReader: Reader[Environment, Unit] =
    for {
      c <- cReader
      env <- cReader.ask
    } yield {
      c.write(env.args, ...)
    }

  val env: Environment = // create this from the SparkSession, args, etc.
  writeReader.run(env)
}
{% endhighlight %}

Using the `Reader`, we've managed to keep the explicit dependency on the environment and
the componentization of the transforms, while making the code more readable and require
less boilerplate.

From a glance, it is easy to tell what each transformation is doing (just peek inside the
`yield`), and it is easy to tell how each transformation depends on other transformations
(just peek inside the `for`).

This code is also easier to test, because tests can call each `Reader` in isolation, or
can mock/stub different intermediate `Reader` steps of the pipeline without having to
worry about changing the code for the other steps.

# Meow

You may recognize the `Reader` monad as an instance of a `Kleisli`. The `Kleisli` is very
similar to the `Reader` except it also allows you to wrap the output of the anonymous
function in another type as well.

{% highlight scala linenos %}
val kleisli = Kleisli[F[_], A, B](f: A => F[B])
{% endhighlight %}

If you're not interested in wrapping your output type, you just wrap it in the `Id` type
(which stands for identity).

{% highlight scala linenos %}
Kleisli[Id, A, B] = Reader[A, B]
{% endhighlight %}

{% highlight scala linenos %}
def main(args: Array[String]): Unit = {
  val aKleisli: Kleisli[Id, Environment, A] =
    Kleisli(env: Environment => env.spark.read(env.args, ...))

  val bKleisli: Kleisli[Id, Environment, B] =
    for {
      a <- aKleisli
    } yield {
      doSomething(a)
    }

  val cKleisli: Kleisli[Id, Environment, C] =
    for {
      a <- aKleisli
      b <- bKleisli
      env <- Kleisli[Id, Environment].ask
    } yield {
      doSomethingElse(env, a, b)
    }

  val writeKleisli: Kleisli[Id, Environment, Unit] =
    for {
      c <- cKleisli
      env <- Kleisli[Id, Environment].ask
    } yield {
      c.write(env.args, ...)
    }

  val env: Environment = // create this from the SparkSession, args, etc.
  writeKleisli.run(env)
}
{% endhighlight %}

# Getting abstract

At a high-level, you can think of the `Reader` and the `Kleisli` as anonymous functions
that transform an `Environment` to an output value. The power of the `Reader` and the
`Kleisli` lies in their ability to chain. We can write two distinct anonymous functions
that take in the environment as an input:

* `Environment => A`
* `Environment => B`

But this route prevents us from reusing code, as each function will have to start from
scratch using the environment. With these functional structures, we could write two
anonymous functions, one with an `Environment` as an input, and the other with an `A` as
an input.

* `Environment => A`
* `A => B`

This way, we can reuse the work from defining the first function for the second function.
Under the hood, the `Reader` and the `Kleisli` take care of stitching everything together
for us, turning the set of anonymous functions here into the same thing as the set above.

# Functions all the way down

Let's finish off by taking a closer look at what happens exactly when the `Kleisli` is
run. We can start by remembering that for/yield is just syntactic sugar for map/flatMap
calls. Here's what our code looks like as maps and flatMaps.

{% highlight scala linenos %}
def main(args: Array[String]): Unit = {
  val aKleisli: Kleisli[Id, Environment, A] =
    Kleisli[Id, Environment, A](env: Environment => env.spark.read(env.args, ...))

  val bKleisli: Kleisli[Id, Environment, B] =
    aKleisli.map(a => doSomethingElse(a))

  val cKleisli: Kleisli[Id, Environment, C] =
    aKleisli.flatMap(a => {
      bKleisli.flatMap(b => {
        Kleisli.ask[Id, Environment].map(env => {
          doMore(env, a, b)
        })
      })
    })

  val writeKleisli: Kleisli[Id, Environment, Unit] =
    cKleisli.flatMap(c => {
      Kleisli.ask[Id, Environment].map(env => {
        c.write(env.args, ...)
      })
    })

  val env: Environment = // create this from the SparkSession, args, etc.
  writeKleisli.run(env)
}
{% endhighlight %}

Finally, let's take a closer look at the `writeReader`, and see what happens
when we `run` it. We can perform a variable substition for all the previous
steps of the pipeline to get a single `run` statement.

{% highlight scala linenos %}
val writeKleisli =
  Kleisli[Id, Environment, A](env: Environment => env.spark.read(env.args, ...))
    .flatMap(a => {
      Kleisli[Id, Environment, A](env: Environment => env.spark.read(env.args, ...))
        .map(a => doSomethingElse(a))
        .flatMap(b => {
          Kleisli.ask[Id, Environment].map(env => {
            doMore(env, a, b)
          })
        })
    }).flatMap(c => {
      Kleisli.ask[Id, Environment].map(env => {
        c.write(env.args, ...)
      })
    })

val env: Environment = // create this from the SparkSession, args, etc.
writeKleisli.run(env)
{% endhighlight %}

When we translate the code to map/flatMap calls, we can see the linear progression of the
code. First, `a` is generated, then `a` is generated again in order to generate `b`, then
both `a` and `b` are combined with the `env` to form `c`, and finally `c` is combined
with the `env` to perform the write.

By using `Reader`/`Kleisli`, we are able to wrap up each of our anonymous functions, and
then roll them up with each other using chaining. When we unroll the `Reader`/`Kleisli`
by running, we see that anonymous functions are evaluated like a stack. We define the
`writeKleisli`, which calls `cKleisli`, which in turn calls `bKleisli` and `aKleisli`.
These calls are evaluated in last-called-first-evaluated order, with `aKleisli` and
`bKleisli` evaluated first, followed by `cKleisli` and `writeKleisli`.

# Additional Resources

1. [Composing Spark data pipelines](https://www.stephenzoio.com/creating-composable-data-pipelines-spark/)

2. [Reader monad for dependency injection](https://blog.originate.com/blog/2013/10/21/reader-monad-for-dependency-injection/)
