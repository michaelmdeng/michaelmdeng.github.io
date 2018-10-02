---
layout: post
title:  "More error handling in Scala"
---

Recently, I've been using the ADLS Java SDK in order to interact with files in ways that
Apache Spark can't. The SDK has been especially useful when writing Spark jobs that
update existing tables rather than transforming data and writing it to a new location. In
this situation, the query planner will complain about the source table changing out from
underneath it. A common workaround is to:

1. Write the updated table to a temporary location
2. Read in the updated table from the temporary location
3. Write the updated version to the original location

This workaround requires twice the R/W overhead - it would be much more performant to use
the native file system operations to rename the table created in step 1 to the correct
final location. This can be done using the ADLS SDK, at the cost of further coupling the
Spark jobs with the underlying native file system.

# But is it functional?

It has been even more interesting incorporating this third-party library into an existing
Scala codebase that is largely functional in nature. In contrast, the ADKS Java library
handles errors similarly to other Java libraries - the ADLS calls will return a boolean
indicating whether or not the operation was a success, as well as having the ability to
throw exceptions on more catastrophic errors. A typical function signature might look
something like:

```
boolean doSomethingInADLS(...) throws IOException, ADLException
```

This makes it a little tricky to safely deal with both _result failures_ (calls that return
false) and _exception failures_ (calls that throw). Typical object-oriented code will
handle each these two types of failures differently - result failures can be handled by
keeping track of the results of each statement, while exception failures can be handled
by wrapping the whole code block in a `try...catch`.

# Exception hell

Let's consider our example from above, where want to use the ADLS SDK to rename a file
from a temporary location to overwrite an existing file.  In order to complete the
rename, we will need to:

1. Delete the destination, if it exists
2. Create the parent folder of the destination, if it doesn't exist
3. Rename the file
4. Reverse any or all of the above steps on failure

Some code for this rename (with simplified method/variable names) might look like:

```
val deleteSuccessful = false
val folderSuccessful = false
val renameSuccessful = false
try {
  if (fileExists(destination)) {
    deleteSuccessful = deleteFile(destination)
  } else {
  deleteSuccessful = true
  }

  if (deleteSuccessful && !fileExists(getParent(destination))) {
    folderSuccessful = createFolder(getParent(destination))
  else if (deleteSuccessful) {
    folderSuccessful = true
  }

  if (folderSuccessful) {
     = renameFile(source, destination)
    if (!renameSuccessful)) { 
      throw new ADLException("The rename failed!")
    }
  }
} catch (IOException ioException) {
  recoverFromIO()
} catch (ADLException adlException) {
  recoverFromADL()
} finally {
  cleanupFromFailure()

  // we made it!
}
```

Many of you may recognize this [exception hell](https://philipnilsson.github.io/Badness10k/escaping-hell-with-monads).

* This code quickly becomes hard to read, understand, and manage because of all the
possible failure places for the rename logic and the subsequent error handling. This
only gets worse as business logic becomes more complex and more operations are
added.
* Having all the error handling here forces a single mode of error handling. Callers that
want to handle errors slightly differently will have to duplicate this code in some
way.
* As more super- and sub-routines are added that call and are called by this code, it
becomes hard to remember that these exceptions are caught at this level. Once again,
developers that want to handle errors at a different level in the stack will have to
duplicate this code in some way.

# Error handling with `Try`

When I saw the way the ADLS library worked, I wanted to see how to make it conform the
the functional paradigm of a lot of our existing code.

The typical way to accomplish error-handling in functional Scala is with the `Try` type.
A `Try[A]` represents a computation that successfully completes and results in a value of
type `A`, or fails in the form of a `Throwable`. If the computation succeeds, we get a
`Success[A]`.  If it fails, we get a `Failure[Throwable]`. Since `Try` is a monad, we can
wrap all of our operations in the `Try`, pass them around, and compose them using our
favorite `for..yield` or `map/flatMap` syntax.

So let's wrap some of these ADLS calls in a Try and see what happens!

```
for {
  result1 <- Try(doSomethingInADLS())
  result2 <- Try(doSomethingElseInADLS(result1))
  result3 <- Try(doSomethingIndependentlyInADLS())
} yield {
  result4 <- Try(finishSomethingInADLS(result2, result3))
}
```

However, based on the design of the ADLS SDK, this code won't work correctly. We are
wrapping each ADLS action in a `Try`, resulting in a `Success[Boolean]` if the ADLS call
did not throw, and a `Failure[IOException]` or a `Failure[ADLException]` if it did. If
any of the ADLS calls returns `false`, representing a _result failure_, the evaluation of
the `Try` will return `Success(false)`. Any subsequent `Try` calls that depend on this
value will continue as though the previous call succeeded, even though it actually
didn't.  This means that there is not a 1-to-1 correspondence between what the ADLS
library considers a failure and what we will return as a `Failure` in our code.

# Trying harder

In order to fix this issue, we need transform any `Try` to make sure a `Success(false)`
actually becomes `Failure[Throwable]`. When we do this, we no longer care about the
result value that will be wrapped up by our `Try`, since it should always wrap `true`
because the previous call succeeded. This means we can change the output type of our ADLS
computations from `Try[Boolean]` to `Try[Unit]`.

```
def successAndTrue(tryResult: Try[Boolean]): Try[Unit] = {
  tryResult.filter(result => result)
}
```

Ahh... much better.

Calling `successAndTrue()` on a `Success[False]` will return a `Failure[Exception]`, just
as we desired. Using this helper, any outcomes that represent a successful ADLS call will
return a `Success(())`, and any outcomes that represent a failure will return a
`Failure[Exception]`.

Now let's try our renaming our file using the `Try` type combined with this result-based
error handling.

```
Try(fileExists(destination)).flatMap(exists => {
  if (exists) successAndTrue(Try(deleteFile(destination)))
  else Success(())
}).flatMap(unit => {
  (Try(fileExists(getParent(destination))))
}).flatMap(parentExists => {
  if (!parentExists) successAndTrue(Try(createFolder(getParent(destination))))
  else Success(())
}).flatMap(unit => {
  successAndTrue(Try(rename(source, destination)))
})
```

This operation returns a `Try[Unit]` value that represents whether or not the entire rename
operation succeeded or failed. If it failed at any point during the rename process, all
subsequent computation will be stopped and the relevant error will be returned in the
`Failure`, without an ugly `try catch` clause. Best of all, any calling methods can
examine the specific failure that occurred and choose to handle it in any way they want.

# A smooth recovery

Using the `Try`, we've combined our _result failure_ and _exception failure_ modes into a
single mode of failure, the `Failure[Exception]`. We could have achieved the same result
by making sure to throw an `Exception` on each false call, like:

```
if (!doSomethingInADLS())
  throw new Exception(...)
else
  // continue with the next
```

And while the distinction may seem trivial, it is powerful for a developer to only have
to deal with possible failures in a single location, instead of throughout an entire code
block.

The other important benefit of this `Try` method is the ability to customize error
handling. For example, the following snippet outlines two different methods of error
handling - the first is similar to what we did in the original example, and the second
simply logs everything and continues execution.

```
val tryRename = // the value from Try call

// recover normally
tryRename.recover(unit => {
  PartialFunction(e => {
    e match {
      case e: IOException => recoverFromIO()
      case e: ADLException => recoverFromADL()
    }

    cleanupFromFailure()
  })
})

// recover with logging
tryRename.recover(unit => {
  PartialFunction(e => {
    val log = Logger.getLogger(...)
    log.error(e.getMessage())
  })
})
```
