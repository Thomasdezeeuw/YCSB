# Stored Driver for YCSB

YCSB for Stored.

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
bin/ycsb.sh load stored -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb.sh run stored -s -P workloads/workloada -p stored.mapping_key=/blob/7cc3e1de7953362c40352b7f6f283a040884f5af65cbd07519428ccfdc7e94e39255967fe94b3b23b90fe11f6d84d586d2bd76668287a33d8ab1bfee68fc11b0
```

## Configuration Options

You can set the following properties (with the default settings applied):

 - stored.url=http://127.0.0.1:8080 => The URL for one server.
 - stored.mapping\_key => this is printed when loading a workload.
