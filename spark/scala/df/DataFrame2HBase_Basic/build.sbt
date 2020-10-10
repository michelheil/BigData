name := "sparkHBaseConnector"

version := "0.2"

scalaVersion := "2.11.7"

// resolvers += Resolver.url("Hortonworks", url("https://repo.hortonworks.com/content/repositories/releases/"))
resolvers += "Hortonworks" at "https://repo.hortonworks.com/content/repositories/releases/"

// https://mvnrepository.com/artifact/com.hortonworks.shc/shc-core
libraryDependencies += "com.hortonworks.shc" % "shc-core" % "1.1.0.3.1.5.6-1"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1"