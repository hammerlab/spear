namespace java org.hammerlab.spear

typedef string ApplicationID
typedef i32 ID
typedef ID JobID
typedef ID StageID
typedef ID StageAttemptID
typedef ID TaskIndex
typedef i64 TaskID
typedef string ExecutorID
typedef i32 RDDID
typedef i64 Time

struct Application {
  1: optional ApplicationID id
  2: optional Duration time
  3: optional string name
  4: optional string user
} (
  primary_key="id",
  mongo_collection="applications",
  mongo_identifier="spark",
)

struct Job {
  1: optional ApplicationID appId
  2: optional JobID id
  3: optional Duration time
  4: optional list<i32> stageIDs

  // NOTE(ryan): the "failed" case includes a private[spark] JobResult that
  // wraps an Exception; it would be nice to get at that info.
  5: optional bool succeeded
  6: optional map<string, string> properties
  7: optional Counts taskCounts
  8: optional Counts taskAttemptCounts
  9: optional Counts stageCounts
  10: optional Counts stageAttemptCounts
  11: optional bool started
  12: optional bool ended
} (
  primary_key="appId",
  mongo_collection="jobs",
  mongo_identifier="spark",
  index="appId: asc, id: asc"
)

struct Stage {
  1: optional ApplicationID appId
  2: optional StageID id
  3: optional i32 attempt
  4: optional string name
  5: optional list<i32> rddIDs
  6: optional string details
  7: optional Counts taskCounts
  8: optional Counts taskAttemptCounts
  9: optional Duration time
  10: optional string failureReason
  11: optional map<i64, AccumulableInfo> accumulables
  12: optional TaskMetrics metrics
  13: optional map<string, string> properties
  14: optional JobID jobId
  15: optional bool started  // sanity check: did we receive a StageSubmitted event for this stage?
  16: optional bool ended    // sanity check: did we receive a StageCompleted event for this stage? == !!failureReason
  17: optional bool skipped
  18: optional bool succeeded
  19: optional TaskMetrics validatedMetrics  // Only accept updates about tasks that we have received "start" events for.
} (
  primary_key="appId",
  mongo_collection="stages",
  mongo_identifier="spark",
  index="appId: asc, id: asc, attempt: asc",
  index="appId: asc, jobId: asc"
)

struct StageJobJoin {
  1: optional ApplicationID appId
  2: optional StageID stageId
  3: optional JobID jobId
} (
  primary_key="appId",
  mongo_collection="stage_job_joins",
  mongo_identifier="spark",
  foreign_key="jobId",
  index="appId: asc, stageId: asc"
)

struct Task {
  1: optional ApplicationID appId
  2: optional TaskID id
  3: optional i32 index
  4: optional i32 attempt
  5: optional StageID stageId
  6: optional i32 stageAttemptId
  7: optional Duration time
  8: optional ExecutorID execId
  9: optional TaskLocality taskLocality
  10: optional bool speculative
  11: optional bool gettingResult
  12: optional string taskType
  13: optional TaskEndReason taskEndReason
  14: optional list<TaskMetrics> metrics
  15: optional bool started  // sanity check: did we receive a TaskStart event for this task?
  16: optional bool ended    // sanity check: did we receive a TaskEnd event for this task? == !!taskEndReason
} (
  primary_key="appId",
  mongo_collection="tasks",
  mongo_identifier="spark",
  index="appId: asc, id: asc",
  index="appId: asc, stageId: asc, stageAttemptId: asc, index: asc"
)

struct Executor {
  1: optional ApplicationID appId
  2: optional ExecutorID id
  3: optional string host
  4: optional i32 port
  5: optional i32 totalCores
  6: optional i64 addedAt
  7: optional i64 removedAt
  8: optional string removedReason
  9: optional map<string, string> logUrlMap
  10: optional TaskMetrics metrics
  11: optional TaskMetrics validatedMetrics  // Only accept updates about tasks that we have received "start" events for.
} (
  primary_key="appId",
  mongo_collection="executors",
  mongo_identifier="spark",
  index="appId: asc, id: asc",
  index="appId: asc, host: asc"
)

struct RDD {
  1: optional ApplicationID appId
  2: optional RDDID id
  3: optional string name
  4: optional i32 numPartitions
  5: optional StorageLevel storageLevel
  6: optional i32 numCachedPartitions
  7: optional i64 memSize
  8: optional i64 diskSize
  9: optional i64 tachyonSize
} (
  primary_key="appId",
  mongo_collection="rdds",
  mongo_identifier="spark"
  index="appId: asc, id: asc",
)

struct Duration {
  1: optional Time start
  2: optional Time end
}

struct Counts {
  1: optional i32 num
  2: optional i32 started
  3: optional i32 succeeded
  4: optional i32 failed
  5: optional i32 running
  6: optional i32 skipped
}

struct AccumulableInfo {
  1: optional i64 id
  2: optional string name
  3: optional string update
  4: optional string value
}

struct BlockManagerId {
  1: optional ExecutorID executorID
  2: optional string host
  3: optional i32 port
}

enum TaskEndReasonType {
  SUCCESS = 1,
  RESUBMITTED = 2,
  TASK_RESULT_LOST = 3,
  TASK_KILLED = 4,
  FETCH_FAILED = 5,
  EXCEPTION_FAILURE = 6,
  TASK_COMMIT_DENIED = 7,
  EXECUTOR_LOST_FAILURE = 8,
  UNKNOWN_REASON = 9
}

struct FetchFailed {
  1: optional BlockManagerId bmAddress
  2: optional i32 shuffleId
  3: optional i32 mapId
  4: optional i32 reduceId
  5: optional string message
}

struct StackTraceElem {
  1: optional string declaringClass
  2: optional string methodName
  3: optional string fileName
  4: optional i32 lineNumber
}

struct ExceptionFailure {
  1: optional string className
  2: optional string description
  3: optional list<StackTraceElem> stackTrace
  4: optional string fullStackTrace
  5: optional TaskMetrics metrics
}

struct TaskCommitDenied {
  1: optional i32 jobID
  2: optional i32 partitionID
  3: optional i32 attemptID
}

struct ExecutorLostFailure {
  1: optional ExecutorID execId
}

struct TaskEndReason {
  1: optional TaskEndReasonType tpe

  // Populate at most one of these
  2: optional FetchFailed fetchFailed
  3: optional ExceptionFailure exceptionFailure
  4: optional TaskCommitDenied taskCommitDenied
  5: optional ExecutorLostFailure executorLostFailure
}

enum DataReadMethod {
  Memory, Disk, Hadoop, Network
}

enum DataWriteMethod {
  Hadoop
}

struct InputMetrics {
  1: optional DataReadMethod readMethod
  2: optional i64 bytesRead
  3: optional i64 recordsRead
}

struct OutputMetrics {
  1: optional DataWriteMethod writeMethod
  2: optional i64 bytesWritten
  3: optional i64 recordsWritten
}

struct ShuffleReadMetrics {
  1: optional i32 remoteBlocksFetched
  2: optional i32 localBlocksFetched
  3: optional i64 fetchWaitTime
  4: optional i64 remoteBytesRead
  5: optional i64 localBytesRead
  6: optional i64 recordsRead
}

struct ShuffleWriteMetrics {
  1: optional i64 shuffleBytesWritten
  2: optional i64 shuffleWriteTime
  3: optional i64 shuffleRecordsWritten
}

struct BlockStatus {
  1: optional StorageLevel storageLevel
  2: optional i64 memSize
  3: optional i64 diskSize
  4: optional i64 tachyonSize
}

struct UpdatedBlock {
  1: optional string blockId
  2: optional BlockStatus blockStatus
}

struct TaskMetrics {
  1: optional string hostname
  2: optional i64 executorDeserializeTime
  3: optional i64 executorRunTime
  4: optional i64 resultSize
  5: optional i64 jvmGCTime
  6: optional i64 resultSerializationTime
  7: optional i64 memoryBytesSpilled
  8: optional i64 diskBytesSpilled
  9: optional InputMetrics inputMetrics
  10: optional OutputMetrics outputMetrics
  11: optional ShuffleReadMetrics shuffleReadMetrics
  12: optional ShuffleWriteMetrics shuffleWriteMetrics
  13: optional list<UpdatedBlock> updatedBlocks
}

enum TaskLocality {
  PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
}

struct StorageLevel {
  1: optional bool useDisk
  2: optional bool useMemory
  3: optional bool useOffHeap
  4: optional bool deserialized
  5: optional i32 replication
}
