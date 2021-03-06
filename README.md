# Spear
Spear is a [`SparkListener`](https://github.com/apache/spark/blob/v1.3.1/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala) that maintains info about Spark jobs, stages, tasks, executors, and RDDs in MongoDB.

**This code is being heavily refactored / replaced**; it is too slow / resource-intensive to run in the Spark driver as written, and other approaches are being explored.

## Usage
* Add the shaded `Spear` JAR (`target/spear-with-dependencies-1.0-SNAPSHOT.jar`) to the classpath of your Spark driver. 
* Register `org.hammerlab.spear.Spear` in the `spark.extraListeners` conf param, e.g. for a `spark-shell`:

```
$ $SPARK_HOME/bin/spark-shell \
    --driver-class-path /path/to/spear/target/spear-with-dependencies-1.0-SNAPSHOT.jar \
    --conf spark-extraListeners=org.hammerlab.spear.Spear
```

### Configuring Mongo URL

By default, `Spear` will look for a MongoDB instance at `localhost:27017/<app ID>`, where the database name `<app ID>` is taken from the Spark application ID.

To configure `Spear` to write events to a MongoDB instance at a different URL, pass `spear.host`, `spear.port`, and `spear.db` conf params to Spark:

```
$ $SPARK_HOME/bin/spark-shell \
    --driver-class-path /path/to/spear/target/spear-with-dependencies-1.0-SNAPSHOT.jar \
    --conf spark.extraListeners=org.hammerlab.spear.Spear \
    --conf spear.host=<mongo host> \
    --conf spear.port=<mongo port> \
    --conf spear.db=<mongo db name>
```

Your `SparkContext` will send updates about your Spark jobs' progress which `Spear` will then write to your Mongo instance.

## DB Collections

`Spear` creates and maintains several tables in Mongo: `applications`, `jobs`, `stages`, `tasks`, `executors`, `rdds`, and more. 

To inspect these tables and their contents from a mongo console, connect to your DB accordingly, e.g. from the shell:
```
$ mongo <mongo host>:<mongo port>/<db name; possibly Spark app ID>
```
or from within a running mongo client:
```
> use <db name>
```

Below are some descriptions or examples of each collections that `Spear` works with:

### `jobs`
Spark jobs, e.g.:
```
> db.jobs.findOne()
{
	"_id" : ObjectId("5555f9f2d860627bc896ba99"),
	"appId" : "local-1431734416602",
   	"id" : 0,
	"stageCounts" : {
		"num" : 2,
		"started" : 2,
		"succeeded" : 2,
		"failed" : 0,
		"running" : 0
	},
	"taskCounts" : {
		"num" : 20,
		"started" : 20,
		"succeeded" : 20,
		"failed" : 0,
		"running" : 0
	},
	"properties" : {

	},
	"stageIDs" : [
		0,
		1
	],
	"time" : {
		"start" : NumberLong("1431697906380"),
		"end" : NumberLong("1431697913068")
	},
	"succeeded" : true
}
```

### `stages`
```
> db.stages.findOne()
{
	"_id" : ObjectId("5555f9f2d860627bc896ba9a"),
	"appId" : "local-1431734416602",
	"attempt" : 0,
	"id" : 0,
	"jobId" : 0,
	"details" : "org.apache.spark.rdd.RDD.map(RDD.scala:287)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:23)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:28)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:30)\n$line19.$read$$iwC$$iwC$$iwC$$iwC$$iwC.<init>(<console>:32)\n$line19.$read$$iwC$$iwC$$iwC$$iwC.<init>(<console>:34)\n$line19.$read$$iwC$$iwC$$iwC.<init>(<console>:36)\n$line19.$read$$iwC$$iwC.<init>(<console>:38)\n$line19.$read$$iwC.<init>(<console>:40)\n$line19.$read.<init>(<console>:42)\n$line19.$read$.<init>(<console>:46)\n$line19.$read$.<clinit>(<console>)\n$line19.$eval$.<init>(<console>:7)\n$line19.$eval$.<clinit>(<console>)\n$line19.$eval.$print(<console>)\nsun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\nsun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)\nsun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\njava.lang.reflect.Method.invoke(Method.java:606)\norg.apache.spark.repl.SparkIMain$ReadEvalPrint.call(SparkIMain.scala:1065)",
	"rddIDs" : [
		1,
		0
	],
	"taskCounts" : {
		"num" : 10,
		"started" : 10,
		"succeeded" : 10,
		"failed" : 0,
		"running" : 0
	},
	"name" : "map at <console>:23",
	"properties" : {

	},
	"time" : {
		"start" : NumberLong("1431697906550"),
		"end" : NumberLong("1431697910477")
	},
	"metrics" : {
		"executorDeserializeTime" : NumberLong(2281),
		"executorRunTime" : NumberLong(11813),
		"resultSize" : NumberLong(8900),
		"jvmGCTime" : NumberLong(488),
		"resultSerializationTime" : NumberLong(11),
		"memoryBytesSpilled" : NumberLong(0),
		"diskBytesSpilled" : NumberLong(0),
		"inputMetrics" : {
			"bytesRead" : NumberLong(0),
			"recordsRead" : NumberLong(0)
		},
		"outputMetrics" : {
			"bytesWritten" : NumberLong(0),
			"recordsWritten" : NumberLong(0)
		},
		"shuffleReadMetrics" : {
			"remoteBlocksFetched" : 0,
			"localBlocksFetched" : 0,
			"fetchWaitTime" : NumberLong(0),
			"remoteBytesRead" : NumberLong(0),
			"localBytesRead" : NumberLong(0),
			"recordsRead" : NumberLong(0)
		},
		"shuffleWriteMetrics" : {
			"shuffleBytesWritten" : NumberLong(813425),
			"shuffleWriteTime" : NumberLong(4542000),
			"shuffleRecordsWritten" : NumberLong(100000)
		}
	}
}
```

### `tasks`
```
> db.tasks.findOne()
{
	"_id" : ObjectId("5555f9f3d860627bc896baa1"),
	"appId" : "local-1431734416602",
	"id" : NumberLong(0),
	"speculative" : false,
	"taskLocality" : 0,
	"execId" : "<driver>",
	"time" : {
		"start" : NumberLong("1431697906561"),
		"end" : NumberLong("1431697908695")
	},
	"stageAttemptId" : 0,
	"stageId" : 0,
	"attempt" : 0,
	"index" : 0,
	"metrics" : [
		{
			"hostname" : "localhost",
			"executorDeserializeTime" : NumberLong(0),
			"executorRunTime" : NumberLong(0),
			"resultSize" : NumberLong(0),
			"jvmGCTime" : NumberLong(0),
			"resultSerializationTime" : NumberLong(0),
			"memoryBytesSpilled" : NumberLong(0),
			"diskBytesSpilled" : NumberLong(0),
			"shuffleWriteMetrics" : {
				"shuffleBytesWritten" : NumberLong(0),
				"shuffleWriteTime" : NumberLong(0),
				"shuffleRecordsWritten" : NumberLong(0)
			}
		},
		{
			"hostname" : "localhost",
			"executorDeserializeTime" : NumberLong(523),
			"executorRunTime" : NumberLong(1467),
			"resultSize" : NumberLong(890),
			"jvmGCTime" : NumberLong(40),
			"resultSerializationTime" : NumberLong(1),
			"memoryBytesSpilled" : NumberLong(0),
			"diskBytesSpilled" : NumberLong(0),
			"shuffleWriteMetrics" : {
				"shuffleBytesWritten" : NumberLong(80835),
				"shuffleWriteTime" : NumberLong(360000),
				"shuffleRecordsWritten" : NumberLong(10000)
			}
		}
	],
	"taskEndReason" : {
		"tpe" : 1
	},
	"taskType" : "ShuffleMapTask"
}
```

### `executors`

```
{
	"_id" : ObjectId("5555f9a9d4c69b527c8b38b7"),
	"appId" : "local-1431734416602",
	"id" : "<driver>",
	"host" : "localhost",
	"port" : 50973,
	"metrics" : {
		"executorDeserializeTime" : NumberLong(5802),
		"executorRunTime" : NumberLong(388196),
		"resultSize" : NumberLong(633760),
		"jvmGCTime" : NumberLong(33063),
		"resultSerializationTime" : NumberLong(119),
		"memoryBytesSpilled" : NumberLong(0),
		"diskBytesSpilled" : NumberLong(0),
		"inputMetrics" : {
			"bytesRead" : NumberLong(0),
			"recordsRead" : NumberLong(0)
		},
		"outputMetrics" : {
			"bytesWritten" : NumberLong(0),
			"recordsWritten" : NumberLong(0)
		},
		"shuffleReadMetrics" : {
			"remoteBlocksFetched" : 0,
			"localBlocksFetched" : 20400,
			"fetchWaitTime" : NumberLong(172),
			"remoteBytesRead" : NumberLong(0),
			"localBytesRead" : NumberLong(121076289),
			"recordsRead" : NumberLong(14400000)
		},
		"shuffleWriteMetrics" : {
			"shuffleBytesWritten" : NumberLong(121076289),
			"shuffleWriteTime" : NumberLong(1784705000),
			"shuffleRecordsWritten" : NumberLong(14400000)
		}
	}
}
```

### `rdds`
```
> db.rdds.findOne()
{
	"_id" : ObjectId("5555f9f2d860627bc896ba9e"),
	"appId" : "local-1431734416602",
	"id" : 1,
	"tachyonSize" : NumberLong(0),
	"diskSize" : NumberLong(0),
	"memSize" : NumberLong(0),
	"numCachedPartitions" : 0,
	"storageLevel" : {
		"useDisk" : false,
		"useMemory" : false,
		"useOffHeap" : false,
		"deserialized" : false,
		"replication" : 1
	},
	"numPartitions" : 10,
	"name" : "1"
}
```

## Building

Build the `Spear` JARs with SBT:
```
$ sbt assembly
```

## Notes/Gotchas
* On instantiation, `Spear` queries the BlockManager for info about all registered executors, since it has typically missed all [`SparkListenerExecutorAdded`](https://github.com/apache/spark/blob/v1.3.1/core/src/main/scala/org/apache/spark/scheduler/SparkListener.scala#L93-L95) events by the time the `SparkContext` is initialized and passed to the `Spear`.
* Likewise, `Spear` has typically missed the `SparkListenerApplicationStart` event, and currently doesn't try to understand Spark applications.
