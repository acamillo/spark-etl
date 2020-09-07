# Spark ETL _(beta)_ #

[![Build](https://github.com/acamillo/spark-etl/workflows/build/badge.svg?branch=master)](https://github.com/acamillo/spark-etl/actions?query=branch%3Aseries%2F3.x+workflow%3Abuild) 

- [Overview](#overview)
- [Usage](#usage)
  - [Library dependency (sbt)](#library-dependency-sbt)
  - [Sub-projects](#sub-projects)

  ## Overview

**Spark ETL** is a simple and *opinionated* way to help you structure type-safe Extract-Transform-Load (**ETL**) 
pipelines. This Domain Specific Language (DSL) is flexible enough to create linear pipelines which involve a single 
`Extract` source and `Load` sink 

```
Extract source A ~> Transform A to B ~> Load B (sink 1)
```

all the way to joining multiple Extract sources together and flowing the data through to multiple Load sinks

```

Extract source A ~>                               ~> Load D (sink 1)
                   \                             /
Extract source B    ~> Transform (A, B, C) to D ~>   Load D (sink 2)
                   /                             \
Extract source C ~>                               ~> Load D (sink 3)

``` 

It is built on an immutable and functional architecture where side-effects are executed at the end-of-the-world when the 
pipeline is run. 

This is intended to be used in conjunction with Spark (especially for doing ETL) in order to minimize boilerplate and 
have the ability to see an almost whiteboard-like representation of your pipeline.

## Usage

- Use **[monix-jvm-app-template.g8](https://github.com/monix/monix-jvm-app-template.g8)**
for quickly getting started with a Monix-driven app
- See **[monix-sample](https://github.com/monix/monix-sample)** for
a project exemplifying Monix used both on the server and on the client.

### Library dependency (sbt)

For the stable release (compatible with Cats, and Cats-Effect 2.x):
 
```scala
libraryDependencies += "io.monix" %% "monix" % "3.2.2"
```
