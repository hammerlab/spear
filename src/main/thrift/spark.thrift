namespace java org.hammerlab.spear

typedef i32 JobID
typedef i32 StageID
typedef i32 StageAttemptID
typedef i64 TaskID
typedef string ExecutorID
typedef i32 RDDID
typedef i64 Time

struct Duration {
  1: optional Time start
  2: optional Time end
}

struct TaskCounts {
  1: optional i32 numTasks
  2: optional i32 tasksStarted
  3: optional i32 tasksSucceeded
  4: optional i32 tasksFailed
  5: optional i32 tasksRunning
}

struct Job {
  1: optional JobID id
  2: optional Duration time
  3: optional list<i32> stageIDs
  4: optional bool succeeded
  5: optional map<string, string> properties
} (
  primary_key="id",
  mongo_collection="jobs",
  mongo_identifier="spark"
)

struct Stage {
  1: optional StageID id
  2: optional i32 attempt
  3: optional string name
  4: optional list<i32> rddIDs
  5: optional string details
  6: optional TaskCounts counts
  7: optional Duration time
  8: optional string failureReason
  9: optional map<i64, AccumulableInfo> accumulables
  10: optional TaskMetrics metrics
  11: optional map<string, string> properties
  12: optional JobID jobId
} (
  primary_key="id",
  mongo_collection="stages",
  mongo_identifier="spark",
  index="id: asc, attempt: asc",
  foreign_key="jobId" // TODO(ryan): what does this get us?
)

struct StageJobJoin {
  1: optional StageID stageId
  2: optional JobID jobId
} (
  primary_key="stageId",
  mongo_collection="stage_job_join",
  mongo_identifier="spark",
  foreign_key="jobId"
)

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

struct Task {
  1: optional TaskID id
  2: optional i32 index
  3: optional i32 attempt
  4: optional StageID stageId
  5: optional i32 stageAttemptId
  6: optional Duration time
  7: optional ExecutorID execId
  8: optional TaskLocality taskLocality
  9: optional bool speculative
  10: optional bool gettingResult
  11: optional string taskType
  12: optional TaskEndReason taskEndReason
  13: optional list<TaskMetrics> metrics
} (
  primary_key="id",
  mongo_collection="tasks",
  mongo_identifier="spark",
  index="stageId: asc, stageAttemptId: asc, index: asc"
)

enum TaskLocality {
  PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
}

struct Executor {
  1: optional ExecutorID id
  2: optional string host
  3: optional i32 port
  4: optional i32 totalCores
  5: optional i64 addedAt
  6: optional i64 removedAt
  7: optional string removedReason
  8: optional map<string, string> logUrlMap
  9: optional TaskMetrics metrics
} (
  primary_key="id",
  mongo_collection="executors",
  mongo_identifier="spark",
  index="host: asc"
)

struct StorageLevel {
  1: optional bool useDisk
  2: optional bool useMemory
  3: optional bool useOffHeap
  4: optional bool deserialized
  5: optional i32 replication
}

struct RDD {
  1: optional RDDID id
  2: optional string name
  3: optional i32 numPartitions
  4: optional StorageLevel storageLevel
  5: optional i32 numCachedPartitions
  6: optional i64 memSize
  7: optional i64 diskSize
  8: optional i64 tachyonSize
} (
  primary_key="id",
  mongo_collection="rdds",
  mongo_identifier="spark"
)
