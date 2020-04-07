# Stored Driver for YCSB

TODO: docs.

## Quickstart

### 1. Start Couchbase Server

Start the server.

### 2. Set up YCSB

You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -DskipTests package
```

NOTE: compile only the stored binding:
```
mvn compile -pl stored -am
# Dev loop:
find stored/ | entr -c mvn compile -pl stored -am
# Manual testing:
java site.ycsb.CommandLine -db site.ycsb.db.StoredClient -p stored.url=http://127.0.0.1:8080
```

### 3. Run the Workload

Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load stored -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run stored -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property (as seen in the next section) like this:

```
bin/ycsb run stored -s -P workloads/workloada -p couchbase.useJson=false
```

## Configuration Options

You can set the following properties (with the default settings applied):

 - stored.url=http://127.0.0.1:8080 => The URL for one server.
