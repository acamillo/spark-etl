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
 
 
A pipeline can as easy as a linear extraction - transformation - loading linear process
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

#### `Extract[A]` ####
A producer of a single element of data whose type is `A`. This is the start of the ETL pipeline, you can connect this
to `Transform`ers or to a `Load[A, AStatus]` to create an `ETLPipeline[AStatus]` that can be run.

#### `Transform[A, B]` ####
A transformer of a an element `A` to `B` you can attach these after an `Extract[A]` or before a `Load[B]`

#### `Load[B, BStatus]` ####
The end of the pipeline which takes data `B` flowing through the pipeline and consumes it and produces a status 
`BStatus` which indicates whether consumption happens successfully

#### `ETLPipeline[ConsumeStatus]` ####
This represents the fully created ETL pipeline which can be executed using `unsafeRunSync()` to produce a 
`ConsumeStatus` which indicates whether the pipeline has finished successfully.

**Note:** At the end of the day, these building blocks are a reification of values and functions. You can build an 
ETL pipeline out of functions and values but it helps to have a Domain Specific Language to increase readability.


- Use **[monix-jvm-app-template.g8](https://github.com/monix/monix-jvm-app-template.g8)**
for quickly getting started with a Monix-driven app
- See **[monix-sample](https://github.com/monix/monix-sample)** for
a project exemplifying Monix used both on the server and on the client.

### Library dependency (sbt)

For the stable release (compatible with Cats, and Cats-Effect 2.x):
 
```scala
libraryDependencies += "io.monix" %% "monix" % "3.2.2"
```
