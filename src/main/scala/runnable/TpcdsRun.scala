package runnable

import java.nio.file.Path
import java.nio.file.Paths
import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS

import jcuda.runtime.JCuda

class TpcdsRun extends Runnable {
  override def execute(firstQuery: Int = 1, lastQuery: Int = 104, isGpu: Boolean = false): Long = {
    /* Run Parameters */
    println("\nNUMBER OF CORES SET TO " + cores)
    val rootDir = Paths.get("TPCDS").toAbsolutePath().toString()
    val logsDir = new File(Paths.get("SPARK_LOGS").toAbsolutePath.toString)
    if (!logsDir.exists())
      logsDir.mkdirs()
    val resultLocation = s"$rootDir/gpuResults" 
    val databaseName = "tpcds"
    
    /* Setup Spark Context and Config */
    val appName = 
      if(isGpu)
        s"Gpu_Tpcds_q${firstQuery}-${lastQuery}_sf${scaleFactor}"
       else
        s"Tpcds_q${firstQuery}-${lastQuery}_sf${scaleFactor}"
        
    val conf = super.createSparkConf(
      isGpu = isGpu,
      appName = appName.toString,
      logsDir = logsDir.toString,
      rootDir = rootDir.toString,
      cores = cores.toString)

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
    println("USING SUBSET OF QUERIES: ")
    queriesSubset.foreach(print)
    spark.sql(s"use $databaseName")

    println("\n*\n*\nDATABASE INITIALIZED, STARTING QUERIES\n*\n*\n")
    val queryStartTime = System.currentTimeMillis()
    JCuda.cudaProfilerStart()
    val experiment = tpcds.runExperiment(
      queriesSubset, 
      iterations = iterations,
      resultLocation = resultLocation,
      forkThread = true)

    experiment.waitForFinish(timeout)
    JCuda.cudaProfilerStop()

    val queryEndTime = System.currentTimeMillis()
    val queryTimeSeconds = (queryEndTime - queryStartTime) / 1000
    println("\n\n USED A SUBSET OF QUERIES: ")
    queriesSubset.foreach(print)
    return queryTimeSeconds
  }
}
