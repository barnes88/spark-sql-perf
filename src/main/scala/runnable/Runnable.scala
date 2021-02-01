package runnable

import org.apache.spark.SparkConf

abstract class Runnable() {
  // Benchmark Parameters
  val cores: Int = Runtime.getRuntime.availableProcessors.toInt // CPU Cores to use
  val scaleFactor: String = "1" // Size of dataset to generate in GB
  val format: String = "parquet" // file format
  val iterations: Int = 1 // how many iterations of queries to run
  val timeout: Int = 24*60*60 // timeout in seconds

  // Spark Configuration object
  var conf: SparkConf = _

  // Default Arguments for execute must be overridden by child classes
  def execute(firstQuery: Int = 0, lastQuery: Int = 0, isGpu: Boolean = false): Long 
  
  def initSparkConf(isGpu: Boolean, appName: String, logsDir: String,
    rootDir: String, cores: String) {
    conf = new SparkConf()
    // Default Spark Configs for all Tpc Benchmarks
        .setAppName(appName)
        .setMaster(s"local[$cores]")
        .set("spark.driver.memory", "16g")
        .set("spark.executor.memory", "16g")
        .set("spark.eventLog.enabled", "true")
        .set("spark.sql.parquet.compression.codec", "snappy")
        .set("spark.sql.files.maxRecordsPerFile", "20000000")
        .set("parquet.memory.pool.ratio", "0.5")
        .set("spark.eventLog.enabled", "true")
        .set("spark.eventLog.dir", s"$logsDir")
        .set("spark.local.dir", s"$rootDir/SPARK_LOCAL")
    
    // Spark Rapids GPU specific Configs
    if (isGpu) {
        conf
        .set("spark.sql.rapids.sql.enabled", "true")
        .set("spark.rapids.sql.incompatibleOps.enabled", "true")
        .set("spark.executor.instances", "1") // changed to 1 executor
        .set("spark.executor.cores", "1")
        .set("spark.rapids.sql.concurrentGpuTasks", "1")
        .set("spark.rapids.memory.pinnedPool.size", "2G")
        .set("spark.locality.wait", "0s")
        .set("spark.sql.files.maxPartitionBytes", "512m")
        //.set("spark.sql.shuffle.partitions", "10")
        .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .set("spark.rapids.memory.gpu.allocFraction", "0.5")
        .set("spark.rapids.sql.format.parquet.multiThreadedRead.maxNumFilesParallel", "2147483647") //default
        .set("spark.rapids.sql.format.parquet.multiThreadedRead.numThreads", "20") //default
        .set("spark.rapids.sql.format.parquet.reader.type", "MULTITHREADED") // default, alternatives: "COALESCING" or "PERFILE"
    }
  }

  def printSparkConf(): String = conf.toDebugString
}
