## How to create a java class out of a json file representing an avro schema
- create maven project
- create folder "avro" in src/main/
- create an AVRO Schema (JSON-file) and store it under /src/main/avro/. File ending has to be .avsc
- download latest version of org.apache.avro
- set up maven pom-.xml as documented in http://avro.apache.org/docs/current/gettingstartedjava.html
- compile maven project

=> Java class will be created in main/java/
