# Spark ETL _(beta)_ #

[![Build](https://github.com/acamillo/spark-etl/workflows/build/badge.svg?branch=master)](https://github.com/acamillo/spark-etl/actions?query=branch%3Aseries%2F3.x+workflow%3Abuild) 

* [Overview](#overview)
* [Usage](#usage)
  * [Library dependency (sbt)](#library-dependency-sbt)
  * [Sub-projects](#sub-projects)

## Overview

**Spark ETL** is a simple and *opinionated* set of patterns to define type-safe Extract-Transform-Load (**ETL**) 
pipelines. This Domain Specific Language (DSL) provides a minimal set of constructors and operators to create and compose
linear pipelines which involve a single `Extract` source and `Load` sink. The `pipeline`'s end result could be an effect, 
such as the `Load` operation or an action (eg `count`). 
 
 
A pipeline can be as easy as a linear extraction - transformation - loading linear process
```
Extract source A ~> Transform A to B ~> Load B (sink 1)
```

or the combination of multiple Extract sources joined together and flowing the data through to multiple Load sinks

```

Extract source A ~>                               ~> Load D (sink 1)
                   \                             /
Extract source B    ~> Transform (A, B, C) to D ~>   Load D (sink 2)
                   /                             \
Extract source C ~>                               ~> ~> Count D

                                                   
Extract source A ~>                               
                   \                             
                    ~> Transform (A, B) ~> 
                   /                       \                 
Extract source B ~>                         ~> to F ~>  Load F (sink 3)
                                           /                             
                       Extract source C ~>                               
                                                   
``` 

The library builds  on an immutable and functional architecture where side-effects are executed at the end-of-the-world 
when the pipeline is run. Given the very basic requirements of the library I opted to use [Monix](https://github.com/monix)
for effect management. As you will see, reading the code, all I to Monix is lazy evaluation of the effects,



This library has no pretence of completeness, indeed it will leave the adopters the freedom to
add, modify and run new transformation using a terminology from the very specific business domain logic.
 
## Examples 
```bash
sbt "examples/runMain com.github.acamillo.sparketl.Main"
```

## Usage

### Building Blocks ###

An ETL pipeline consists of the following building blocks:

#### `DataReader[A]` ####
This represents a data source that when evaluated returns a Spark Dataset of [A]

#### `DataWriter[A]` ####
This represents a data sink that when executed takes a Dataset of [A] and store it to a Spark destination.

#### `Extract[A]` ####
A producer of a single element of data whose type is a Spark Dataset of `[A]`. This is the start of the ETL pipeline, 
you can connect this to `Transform`ers or to a `Load` to create an `Pipeline[A]` that can be run. Given a hypothetical 
 data set of `Product` and data source `reader` we can easily create an `Extract[Product]` using the constructor `fromReader(reader)`.
 ```scala 
val productR = mkReader[Product] fromJson (List("/tmp/source/json/products/"))

val extract = (fromReader(productR) orElse empty) >>> ensure(_.id != 200L)
```

#### `Transform[A, B]` ####
A transformation of a Dataset from elements of type `A` to type `B` you can attach these after an `Extract[A]` or before a `Load`

#### `Load` ####
The end of the pipeline which takes data produced by an `Extract[A]` and store them onto a data sink.

#### `Pipeline[A]` ####
This represents the general outcome of a fully created ETL pipeline which can be executed using `run()` to produce an`A`.


**Note:** At the end of the day, these building blocks are a reification of values and functions. You can build an 
ETL pipeline out of functions and values, but it helps to have a Domain Specific Language to increase readability.


### Library dependency (sbt)

For the stable release (compatible with Monix 3.2.1 and Spark 2.4.6):
 
```scala
libraryDependencies += "com.github.acamillo" %% "spark-etl" % ""
```
