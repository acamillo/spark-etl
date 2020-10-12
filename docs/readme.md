# My Project

To install my project
```scala
libraryDependencies += "com" % "lib" % "@VERSION@"
```

```scala mdoc
val x = 1
List(x, x)
```

 ```scala mdoc
val productR = mkReader[Product] fromJson (List("/tmp/source/json/products/"))

val extract = (fromReader(productR) orElse empty) >>> ensure(_.id != 200L)
```
