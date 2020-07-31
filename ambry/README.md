# Ambry Driver for YCSB

YCSB for Ambry.

## Quickstart

### 1. Start Ambry Server and Frontend

Start the server and frontend, see https://github.com/linkedin/ambry.


### 2. Set up YCSB

You need to clone the repository and compile everything.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -DskipTests package
```

NOTE: compile only the binding:
```
mvn compile -pl ambry -am
```

### 3. Run the Workload

Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb.sh load ambry -s -P workloads/workloadc
```

When loading the workload data it will print the required `ambry.mapping_id`.
Using that  you can run the workload:

```
bin/ycsb.sh run ambry -s -P workloads/workloadc -p ambry.mapping_id=/AAYQAf__AAAAAQAAAAAAAAAA7ZVLSJg8S5e3ym-zkcmUaQ
```

## Configuration Options

You can set the following properties (with the default settings applied):

 - ambry.url=http://127.0.0.1:1174 => The URL for the frontend.
 - ambry.mapping\_id => this is printed when `load`ing a workload.
