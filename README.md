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
 
 
A pipeline can be as easy as a linear extraction - transformation - loading process
```
Extract source A ~> Transform A to B ~> Load B (sink 1)
```

or the combination of multiple Extract sources joined together and flowing the data through to multiple Load sinks

```
Extract source A ~>                               ~> Load D (sink 1)
                   \                             /
Extract source B    ~> Transform (A, B, C) to D ~>   Load D (sink 2)
                   /                             
Extract source C ~>                              
```
and
```                                                
Extract source A ~>                               
                   \                             
                    ~> Transform (A, B) to C ~>                           ~> Load F (sink 2)
                   /                           \                        /
Extract source B ~>                             ~> Transform(C,D) to F ~>    Load F (sink 3)
                                               /                        \     
                            Extract source D ~>                          ~>  Count F
                                                   
``` 

The library builds on an immutable and functional architecture where side-effects are executed at the end-of-the-world 
when the pipelines are run. Given the very basic requirements of the library (basically effect suspensions) I opted to 
use [Monix](https://github.com/monix) for effect management. 
> A Polymorphic implementation based on Cats library proved to be troublesome to live with Spark serialization algorithm,


This library has no pretence of completeness, indeed it will leave the adopters the freedom to
add, modify and run new transformation using a terminology from the very specific business domain logic.
 
## Examples 
```bash
sbt "examples/runMain com.github.acamillo.sparketl.Main"
```

## Usage

### Building Blocks ###

An ETL pipeline consists of the following blocks:

#### `Extract[A]` ####
A producer of an element of data whose type is a Spark `Dataset[A]`. This is the start of the ETL pipeline. 
You can compose an `extract` with `Transform`s or to a `Load` to create a `Pipeline[A]` that can be executed.
The data source of an `extract`or is represented by a [DataReader](modules/core/src/main/scala/spark/etl/DataReader.scala)

Given a hypothetical data set of `Product` and data source `readerOfProducts` we can easily create an `Extract[Product]` using 
the constructor `fromReader(readerOfProducts)`.
 ```scala mdoc
val products = Reader.make[Product] fromJson (List("/tmp/source/json/products/"))

val extract = (fromReader(products) orElse empty) ++ ensure(_.id != 200L)
```

#### `Transform[A, B]` ####
A transformation of a Dataset from elements of type `A` to type `B`. `Transform`s can be composed together to
build a set of common recipes. You can connect as many as transformation as desired to an `Extract[A]` using the `++` operator. 

// this pipeline will extract data from the entityReader data source. If it fails it falls back on returning an
// empty data set. It then adds a couple of transformations, ie it verify that the `id` is gt than 200
// and then drops all the late arrivals.

```scala mdoc
  val toBalance = Transform[(User, Account), FriendlyBalance](
    _.map(tuple => FriendlyBalance(tuple._1.name, tuple._2.balance))
  )
```
#### `Load` ####
The end of the ETL pipeline which takes data produced by an `Extract[A]` and store them onto a data sink. 
To effectively store data a Loader needs a data sink associated defined by a [DataWriter](modules/core/src/main/scala/spark/etl/DataWriter.scala). 
Note: A `Load` is a `Pipeline` that yields a Unit value.

```scala mdoc
  val balancePQ = Writer.make[FriendlyBalance] ++ repartition(10) toParquet ("/tmp/output/db/entity")
  val balance = users.joinWith(accounts)(_.col("account_id") === _.col("account_id")) ++ toBalance

  val loader: Pipeline[Unit] = balance to balancePQ
  loader.run()
```
#### `Pipeline[A]` ####
This represents the general outcome of a fully created ETL pipeline which can be executed using `run()` to produce an`A`.

#### `DataReader[A]` ####
This represents a data source that when evaluated returns a Spark `Dataset[A]`. 

#### `DataWriter[A]` #### 
This represents a data sink that when executed takes a Dataset of [A] and store it to a Spark destination.


**Note:** At the end of the day, these building blocks are a reification of values and functions. You can build an 
ETL pipeline out of functions and values, but it helps to have a Domain Specific Language to increase readability.

## Examples ##
See [here](etl-docs/target/mdoc/readme.md) for examples on how to get started

### Library dependency (sbt)

For the stable release (compatible with Monix 3.2.1 and Spark 2.4.6):
 
```scala
libraryDependencies += "com.github.acamillo" %% "spark-etl" % ""
```
