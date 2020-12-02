import java.nio.file.Path
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS

object TpcdsRun {
  def main(args: Array[String]) {
    /* Run Parameters */
    val cores: Int = Runtime.getRuntime.availableProcessors.toInt //number of CPU-cores
    println("\nNUMBER OF CORES SET TO " + cores)
    val rootDir = Paths.get("TPCDS").toAbsolutePath().toString()
    val resultLocation = s"$rootDir/results"
    val databaseName = "tpcds"
    val scaleFactor = "1" // Size of dataset to generate in GB
    val format = "parquet"
    val iterations = 1 // how many iterations of queries to run
    val timeout = 24*60*60 // timeout in seconds

    /* Setup Spark Context and Config */
    val conf = new SparkConf()
      .setAppName(s"TpcdsRun_sf$scaleFactor")
      .setMaster(s"local[$cores]")
      .set("spark.driver.memory", "16g")
      .set("spark.executor.memory", "16g")
      .set("spark.eventLog.enabled", "true")
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
    spark.sql(s"use $databaseName")

    val experiment = tpcds.runExperiment(
      queries, 
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)

    experiment.waitForFinish(timeout)

    //experiment.getCurrentResults // or: spark.read.json(resultLocation).filter("timestamp = 1429132621024")
      //.withColumn("Name", substring(col("name"), 2, 100))
      //.withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
      //.select('Name, 'Runtime)
  }
}
