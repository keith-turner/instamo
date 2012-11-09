Instamo
=======

Introduction
-----------

Instamo makes it easy to write some code and run it against a local, transient
[Accumulo](http://accumulo.apache.org) 1.4 instance in minutes.  No setup or
installation is required.  This is possible if Java, Git, and Maven are already
installed by following the steps below.

```
git clone git://github.com/keith-turner/instamo.git
cd instamo
vim src/main/java/instamo/AccumuloApp.java
mvn package
mvn exec:exec
```

Map Reduce
----------

Its possible to run local map reduce jobs against the MiniAccumuloCluster instance.   There is an example of this in the source.  The following command will run the map reduce example.

```
mvn exec:exec -DmapReduce
```

Purpose
-------

This project was created while experimenting with creating an implementation
for [ACCUMULO-14](https://issues.apache.org/jira/browse/ACCUMULO-14).  Instamo
contains a new implementation of MiniAccumuloCluster that spawns Zookeeper and
Accumulo processes storing all data in a single local directory.  The purpose
of Instamo is to make it extremely simple to use MiniAccumuloCluster.
MiniAccumuloCluster in Instamo works with Accumulo 1.4.  I plan to work twoards
having MiniAccumuloCluster be part of Accumulo 1.5.

Comparison to Mock Accumulo
--------------------------

MiniAccumuloCluster project provides a capability that is superior to Mock
Accumulo.  Mock Accumulo is a reimplemntation of Accumlo in process.  In many
cases Mock Accumulo may behave slightly differently than Accumulo, which is not
good for testing.   The main advantage of Mock Accumulo is that its much much
faster.  However, maybe MiniAccumuloCluster can be sped up.  Speeding it up
will probably require changes to Accumulo itself.  It seems to take around
three seconds for the master to start assigning tablets.  This speedup will not
be possible in Accumulo 1.4, but maybe in 1.5.

