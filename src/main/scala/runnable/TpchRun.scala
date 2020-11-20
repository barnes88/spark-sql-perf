import java.nio.file.Path
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.commons.io.IOUtils
import com.databricks.spark.sql.perf.tpch._
import com.databricks.spark.sql.perf.Query
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults

object TpchRun {
  def main(args: Array[String]) {
    /* Run Parameters */
    val threadsPerExecutor = 20
    val resultLocation = Paths.get("TPCH_results").toAbsolutePath().toString()
    val scaleFactor = 1 // Size of dataset to generate in GB
    val format = "parquet"
    val iterations = 1 // how many iterations of queries to run
    val timeout = 36*60*60 // timeout in seconds
    def databaseName(scaleFactor: Int, format: String) = s"tpch_sf${scaleFactor}_${format}"
    def randomizeQueries = false
    val workers = 10
    val runtype = "TPCH run"
    val configuration = "default"

    /* Setup Spark Context and Config */
    val conf = new SparkConf()
      .setAppName("TpchRun")
      .setMaster(s"local[$threadsPerExecutor]")
      .set("spark.driver.memory", "16g")
      .set("spark.executor.memory", "16g")
      .set("spark.eventLog.enabled", "true")
      .set("spark.sql.broadcastTimeout", "7200")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val sqlContext = spark.sqlContext

    /* Setup Dataset for tpch */
    val tpch = new TPCH(sqlContext = spark.sqlContext)

    val queries = (1 to 22).map { q =>
      val queryContent: String = IOUtils.toString(
        getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
      new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
      executionMode = CollectResults)
    }

    /* Run benchmarking queries */
    spark.sql(s"create database ${databaseName(scaleFactor, format)}")
    spark.sql(s"USE ${databaseName(scaleFactor, format)}")
    val experiment = tpch.runExperiment(
      queries,
      iterations = iterations,
      resultLocation = resultLocation,
      tags = Map(
        "runtype" -> runtype,
        "date" -> java.time.LocalDate.now.toString,
        "database" -> databaseName(scaleFactor, format),
        "scale_factor" -> scaleFactor.toString,
        "spark_version" -> spark.version,
        "system" -> "Spark",
        "workers" -> workers.toString,
        "configuration" -> configuration
      )
    )
    experiment.waitForFinish(timeout)
    // val summary = experiment.getCurrentResults
    // .withColumn("Name", substring(col("name"), 2, 100))
    // .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
    // .select('Name, 'Runtime)
    // summary.show(9999, false)
  }
}
