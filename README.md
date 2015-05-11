# Spear
Spear is a [`SparkListener`](https://github.com/apache/spark/blob/v1.3.1/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala) that maintains info about Spark jobs, stages, tasks, executors, and RDDs in MongoDB.

## Usage

Build the `Spear` JARs with Maven:
```
$ mvn package -DskipTests
```

Add the shaded `Spear` JAR (`target/spear-with-dependencies-1.0-SNAPSHOT.jar`) to the classpath of your Spark jobs, or to the `--jars` argument to `spark-shell`:

```
$ $SPARK_HOME/bin/spark-shell --jars /path/to/spear/target/spear-with-dependencies-1.0-SNAPSHOT.jar
```

Instantiate a `Spear` with your `SparkContext` and (optional) Mongo server hostname/port:

```
val spear = new Spear(sc, mongoHost, mongoPort)
```
xor
```
val spear = new Spear(sc)  // defaults to localhost:27017
```

`Spear` will register itself with `SparkContext` to receive updates about your Spark jobs' progress, and write them to your Mongo instance.

## DB Collections

`Spear` creates a database named after your Spark application (as returned by `sc.applicationId`); each application's metrics are siloed in this way. To inspect from a mongo console, connect to your DB accordingly from the shell:
```
$ mongo <mongo host>:<mongo port>/<spark app ID>
```
or from within a running mongo client:
```
> use <spark app ID>
```

`Spear` writes several collections to this database:

### `jobs`
Spark jobs, e.g.:
```
> db.jobs.findOne()
{
  "_id" : ObjectId("554fe3bfa3105440b75f23ee"),
  "id" : 0,
	"time" : NumberLong("1431299007856"),
  "stageIDs" : [
    0
  ],
  "finishTime" : NumberLong("1431299046452"),
  "succeeded" : true
}
```

### `stages`
```
> db.stages.findOne()
{
	"_id" : ObjectId("554fe3bf8812d9581ebcc854"),
	"attempt" : 0,
	"id" : 0,
	"name" : "count at ReadDepthHistogram.scala:66",
	"numTasks" : 1033,
	"rdds" : [
		1,
		0
	],
	"details" : "org.apache.spark.rdd.RDD.count(RDD.scala:1006)\norg.hammerlab.pageant.ReadDepthHistogram$Histogram2.<init>(ReadDepthHistogram.scala:66)\norg.hammerlab.pageant.ReadDepthHistogram$.run2(ReadDepthHistogram.scala:198)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:24)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:32)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:34)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:36)\n$line19.$read$$iwC$$iwC$$iwC$$iwC.<init>(<console>:38)\n$line19.$read$$iwC$$iwC$$iwC.<init>(<console>:40)\n$line19.$read$$iwC$$iwC.<init>(<console>:42)\n$line19.$read$$iwC.<init>(<console>:44)\n$line19.$read.<init>(<console>:46)\n$line19.$read$.<init>(<console>:50)\n$line19.$read$.<clinit>(<console>)\n$line19.$eval$.<init>(<console>:7)\n$line19.$eval$.<clinit>(<console>)\n$line19.$eval.$print(<console>)\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)",
	"tasksStarted" : 1033,
	"tasksSucceeded" : 1033,
	"submissionTime" : NumberLong("1431299008115"),
	"completionTime" : NumberLong("1431299046444")
}
```

### `tasks`
```
> db.tasks.findOne()
{
	"_id" : ObjectId("554fe3c0a3105440b75f23ef"),
	"id" : NumberLong(0),
	"index" : 11,
	"attempt" : 0,
	"stageId" : 0,
	"stageAttemptId" : 0,
	"launchTime" : NumberLong("1431299008123"),
	"execId" : "279",
	"taskLocality" : 1,
	"taskType" : "ResultTask",
	"taskEndReason" : {
		"success" : true
	},
	"metrics" : {
		"hostname" : "demeter-csmau08-13.demeter.hpc.mssm.edu",
		"executorDeserializeTime" : NumberLong(22734),
		"executorRunTime" : NumberLong(7452),
		"resultSize" : NumberLong(1783),
		"jvmGCTime" : NumberLong(60),
		"resultSerializationTime" : NumberLong(2),
		"memoryBytesSpilled" : NumberLong(0),
		"diskBytesSpilled" : NumberLong(0),
		"inputMetrics" : {
			"bytesRead" : NumberLong(0),
			"recordsRead" : NumberLong(1179438)
		}
	}
}
```
### `metrics`
This is somewhat duplicative with the `tasks` table; the [`SparkListenerExecutorMetricsUpdate`](https://github.com/apache/spark/blob/v1.3.1/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala#L101-L110) contains task index but not attempt number, but the `tasks` table keys off of `{stage ID, stage attempt ID, task ID, task attempt ID}`, so currently the `metrics` table just assembles an array of all `TaskMetrics` updates for each task (across all attempts for that task).
```
> db.metrics.findOne()
{
	"_id" : ObjectId("554fe3c98812d9581ebcc857"),
	"id" : NumberLong(652),
	"stageAttemptId" : 0,
	"stageId" : 0,
	"metrics" : [
		{
			"hostname" : "demeter-csmau08-2.demeter.hpc.mssm.edu",
			"executorDeserializeTime" : NumberLong(0),
			"executorRunTime" : NumberLong(0),
			"resultSize" : NumberLong(0),
			"jvmGCTime" : NumberLong(0),
			"resultSerializationTime" : NumberLong(0),
			"memoryBytesSpilled" : NumberLong(0),
			"diskBytesSpilled" : NumberLong(0),
			"inputMetrics" : {
				"bytesRead" : NumberLong(0),
				"recordsRead" : NumberLong(0)
			}
		},
		{
			"hostname" : "demeter-csmau08-2.demeter.hpc.mssm.edu",
			"executorDeserializeTime" : NumberLong(6375),
			"executorRunTime" : NumberLong(8495),
			"resultSize" : NumberLong(1782),
			"jvmGCTime" : NumberLong(57),
			"resultSerializationTime" : NumberLong(2),
			"memoryBytesSpilled" : NumberLong(0),
			"diskBytesSpilled" : NumberLong(0),
			"inputMetrics" : {
				"bytesRead" : NumberLong(0),
				"recordsRead" : NumberLong(1200505)
			}
		}
	]
}
```

### `executors`

```
> db.executors.findOne()
{
	"_id" : ObjectId("554fe3aaa3105440b75f227a"),
	"id" : "276",
	"host" : "demeter-csmaz11-10.demeter.hpc.mssm.edu:48378"
}
```

### `rdds`
```
> db.rdds.findOne()
{
	"_id" : ObjectId("554fe3bf8812d9581ebcc855"),
	"id" : 1,
	"name" : "reads1",
	"numPartitions" : 1033,
	"storageLevel" : 0
}
```

## Notes/Gotchas
* On instantiation, `Spear` queries the BlockManager for info about all registered executors, since it has typically missed all [`SparkListenerExecutorAdded`](https://github.com/apache/spark/blob/v1.3.1/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala#L93-L95) events by the time the `SparkContext` is initialized and passed to the `Spear`.
* There is currently an uncanny mix of case classes that are serialized to Mongo records and non-type-checked string fields that are updated on those records; porting to statically-checked record types using e.g. [`rogue`](https://github.com/foursquare/rogue) would be nice.
