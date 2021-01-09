package runnable

import java.nio.file.Path
import java.nio.file.Paths
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS

class TpcdsRun extends Runnable {
  override def execute(firstQuery: Int = 1, lastQuery: Int = 104): Long = {
    /* Run Parameters */
    val cores: Int = Runtime.getRuntime.availableProcessors.toInt //number of CPU-cores
    println("\nNUMBER OF CORES SET TO " + cores)
    val rootDir = Paths.get("TPCDS").toAbsolutePath().toString()
    val logsDir = new File(Paths.get("SPARK_LOGS").toAbsolutePath.toString)
    if (!logsDir.exists())
      logsDir.mkdirs()

    val resultLocation = s"$rootDir/results"
    val databaseName = "tpcds"
    val scaleFactor = "1" // Size of dataset to generate in GB
    val format = "parquet"
    val iterations = 1 // how many iterations of queries to run
    val timeout = 24*60*60 // timeout in seconds

    /* Setup Spark Context and Config */
    val conf = new SparkConf()
      .setAppName(s"TpcdsRun_q${firstQuery}-${lastQuery}_sf$scaleFactor")
      .setMaster(s"local[$cores]")
      .set("spark.driver.memory", "16g")
      .set("spark.executor.memory", "16g")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", s"$logsDir")
      .set("spark.local.dir", s"$rootDir/SPARK_LOCAL")

    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sqlContext = spark.sqlContext

    /* Setup Dataset for tpcds */
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = Paths.get(".", "tpcds-kit/tools").toAbsolutePath().toString(),
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false,
      useStringForDate = false
    )

    tables.genData(
      location = rootDir,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables 
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files. 
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = 100 // how many dsdgen partitions to run - number of input tasks.
    )

    spark.sql(s"create database $databaseName")
    tables.createExternalTables(rootDir, "parquet", databaseName, overwrite = true, discoverPartitions = true)

    tables.analyzeTables(databaseName, analyzeColumns = true) 

    /* Run benchmarking queries */
    val tpcds = new TPCDS(sqlContext = sqlContext)
    val queries = tpcds.tpcds2_4Queries
    val queriesSubset = queries.slice(firstQuery-1, lastQuery)
    spark.sql(s"use $databaseName")

    val queryStartTime = System.currentTimeMillis()
    val experiment = tpcds.runExperiment(
      queriesSubset, 
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)

    experiment.waitForFinish(timeout)
    val queryEndTime = System.currentTimeMillis()
    val queryTimeSeconds = (queryEndTime - queryStartTime) / 1000
    println("\n\n USED A SUBSET OF QUERIES: ")
    queriesSubset.foreach(print)
    return queryTimeSeconds
    //experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
      //.withColumn("Name", substring(col("name"), 2, 100))
      //.withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
      //.select('Name, 'Runtime)
  }
}
