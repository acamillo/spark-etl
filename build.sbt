lazy val Version = new {
  val scala = "2.12.12"

  val spark = "2.4.6"
  val monix = "3.2.2"
}

lazy val appDependencies = Seq(
  "org.apache.spark" %% "spark-core" % Version.spark withSources (), // % Provided,
  "org.apache.spark" %% "spark-sql"  % Version.spark withSources (), // % Provided,
  "io.monix"         %% "monix-eval" % Version.monix withSources ()
) map (_.withSources)

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/acamillo/spark-etl")),
  licenses := List("MIT" -> url("https://github.com/acamillo/spark-etl/blob/master/LICENSE")),
  scmInfo := Some(
    ScmInfo(
      url(s"https://github.com/acamillo/spark-etl"),
      "scm:git:git@github.com:acamillo/spark-etl"
    )
  ),
  developers := List(
    Developer(
      "acamillo",
      "Alessandro Camillo",
      "acamillo@users.noreply.github.com",
      url("https://github.com/acamillo")
    )
  )
)

lazy val noPublishSettings = Seq(
  skip in publish := true,
  publishArtifact := false
)

lazy val commonSettings = Seq(
  organization := "com.github.acamillo",
  scalaVersion := Version.scala
)

lazy val core = project
  .in(file("modules/core"))
  .settings(commonSettings)
  .settings(publishSettings)
  .settings(
    name := "spark-etl",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % Version.spark % Provided,
      "org.apache.spark" %% "spark-sql"  % Version.spark % Provided,
      "io.monix"         %% "monix-eval" % Version.monix
    )
  )

lazy val examples = project
  .in(file("modules/examples"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    name := "spark-etl-examples",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % Version.spark % Provided,
      "org.apache.spark" %% "spark-sql"  % Version.spark % Provided,
      "io.monix"         %% "monix-eval" % Version.monix
    )
  )
  .enablePlugins(MdocPlugin)

lazy val root = project
  .in(file("."))
  //  .aggregate(core, examples)
  .aggregate(core, examples)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(
    name := "spark-etl",
    addCommandAlias("checkFormat", ";scalafmtCheckAll;scalafmtSbtCheck"),
    addCommandAlias("format", ";scalafmtAll;scalafmtSbt"),
    addCommandAlias("build", ";checkFormat;clean;test")
  )

// the documentation source folder is `docs`
// the docs output folder is: etl-docs/target/mdoc/readme.md
// see here for info: https://scalameta.org/mdoc/docs/installation.html#reference
lazy val docs = project // new documentation project
  .in(file("etl-docs")) // important: it must not be docs/
  .settings(
    mdocVariables := Map(
      "VERSION" -> version.value
    )
  )
  .dependsOn(root)
  .enablePlugins(MdocPlugin)

//lazy val root = project
//  .in(file("."))
//  .settings(
//    name                := "spark-etl",
//    libraryDependencies ++= appDependencies,
//    mainClass           in (Compile, run) := Some("com.acamillo.spark.Main")
//  )

//lazy val microsite = project
//  .in(file("examples"))
//  .enablePlugins(MdocPlugin)
//  .settings(commonSettings ++ noPublishSettings)
////  .settings(siteSettings)
//  .dependsOn(core)
