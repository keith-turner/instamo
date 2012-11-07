Instamo
=======

Introduction
-----------

Instamo makes it easy to write some code and run it against a local, transient
[Accumulo](http://accumulo.apache.org) cluster in minutes.  No setup or
installation is required.  This is possible if Java, Git, and Maven are already
installed by following the simple steps below.

```
git clone git://github.com/keith-turner/instamo.git
cd instamo
vim src/main/java/instamo/AccumuloApp.java
mvn package
mvn exec:exec
```

Purpose
-------

This project was created while experimenting with creating an implementation
for [ACCUMULO-14](https://issues.apache.org/jira/browse/ACCUMULO-14).  Instamo
contains a new implementation of MiniAccumuloCluster that spawns Zookeeper and
Accumulo processes storing all data in a single local directory.  This purpose
of Instamo is to make it extremely simple to use MiniAccumuloCluster.
MiniAccumuloCluster in Instamo works with Accumulo 1.4.  I plan to work twoards
having MiniAccumuloCluster be part of Accumulo 1.5.


