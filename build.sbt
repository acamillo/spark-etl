lazy val Version = new {
  val scala = "2.12.12"

  val spark = "2.4.6"
  val monix = "3.2.1"
}

lazy val appDependencies = Seq(
  "org.apache.spark" %% "spark-core" % Version.spark withSources (), // % Provided,
  "org.apache.spark" %% "spark-sql"  % Version.spark withSources (), // % Provided,
  "io.monix"         %% "monix-eval" % Version.monix withSources ()
) map (_.withSources)

//organization in ThisBuild := "com.github.acamillo"
scalaVersion in ThisBuild := Version.scala
version      := "0.1-SNAPSHOT"

lazy val commonSettings = Seq(
  organization := "com.github.acamillo",
  scalaVersion := Version.scala
)

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

lazy val root = project
  .in(file("."))
  .settings(
    name                := "spark-etl",
    libraryDependencies ++= appDependencies,
    mainClass           in (Compile, run) := Some("com.acamillo.spark.Main")
  )
