package runnable

object RunTpcBenchmarks {
  def main (args: Array[String]) {
    if (args.length < 1) {
      println("Must specify TPC benchmark name as an argument")
      println("Valid options include one of: [Tpch, Gpu_Tpch, Tpcds, GpuTpcds]")
      return
    }

    val benchmarkName: String = args(0)
    var subQuery: Boolean = false
    var firstQuery: Int = 0
    var lastQuery: Int = 0
    var isGpu: Boolean = false
    if ( args.length == 3) {
      println("Running a subset of Queries: " + args(1) + " to " + args(2))
      subQuery = true
      firstQuery = args(1).toInt
      lastQuery = args(2).toInt
    }

    val startTime = System.currentTimeMillis() 
    val benchmark: Runnable  = benchmarkName match {
      case "Tpch" =>
        println("Running Tpch Benchmark")
        new TpchRun
      case "Gpu_Tpch" =>
        println("Running Gpu_Tpch Benchmark")
        isGpu = true
        new TpchRun
      case "Tpcds" =>
        println("Running Tpcds Benchmark")
        new TpcdsRun
      case "Gpu_Tpcds" =>
        println("Running Gpu_Tpcds Benchmark")
        isGpu = true
        new TpcdsRun
      case _ =>
        println("Error, benchmark name not recognized")
        println("Valid options include one of: [Tpch, Gpu_Tpch, Tpcds, GpuTpcds]")
        null
    }

    val queryTimeSeconds = 
      if (subQuery)
        benchmark.execute(firstQuery, lastQuery, isGpu)
      else
        benchmark.execute(isGpu = isGpu)

    val endTime = System.currentTimeMillis()
    val wallTimeSeconds = (endTime - startTime)/1000

    println("Benchmark " + benchmarkName + " complete!")
    println("Wallclock time elapsed: " + wallTimeSeconds/60 + " minutes " + wallTimeSeconds %60 + " seconds")
    println("Time Spent executing Bencmark queries: " + queryTimeSeconds/60 + " minutes " + queryTimeSeconds%60 + " seconds" )
    if (subQuery)
      println("Queries run: " + args(1) + " to " + args(2))
    else
      println("All Queries run")
  }
}
