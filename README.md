NiFi-ArangoDB-Bundle
===================
ArangoDB processor for Apache NiFi


This processor connects to ArangoDB for putting/getting documents, graph & key-value data.

Built for NiFi 1.1.0 & ArangoDB 3.1.x (using ArangoDB driver version 4.1.0)

Currently only putting documents is working, todo:

 - Get Document
 - Put/Get graph
 - Put/Get key-value
 - tests

----------


Building this
-------------

 1. git clone https://github.com/pinkdevelops/nifi-arangodb-bundle.git
 2. cd nifi-arangodb-bundle
 3. mvn clean install -DskipTests
 4. cp nifi-arangodb-nar/target/nifi-arangodb-nar-1.0-SNAPSHOT.nar $NIFI_HOME/lib