package runnable

object RunTpcBenchmarks {
  def main (args: Array[String]) {
    if (args.length != 1) {
      println("Must specify TPC benchmark name as an argument")
      println("Valid options include one of: [Tpch, Gpu_Tpch, Tpcds, GpuTpcds]")
      return
    }

    val benchmark : String = args(0)

    val startTime = System.currentTimeMillis() 
    benchmark match {
      case "Tpch" =>
        println("Running Tpch Benchmark")
        TpchRun.execute()
      case "Gpu_Tpch" =>
        println("Running Gpu_Tpch Benchmark")
        Gpu_TpchRun.execute()
      case "Tpcds" =>
        println("Running Tpcds Benchmark")
        TpcdsRun.execute()
      case "Gpu_Tpcds" =>
        println("Running Gpu_Tpcds Benchmark")
        Gpu_TpcdsRun.execute()
      case _ =>
        println("Error, benchmark name not recognized")
        println("Valid options include one of: [Tpch, Gpu_Tpch, Tpcds, GpuTpcds]")
    }

    val endTime = System.currentTimeMillis()

    println("Benchmark " + benchmark + "complete!")
    println("Wallclock time elapsed: " + (endTime - startTime)/60000 + " minutes" )
  }
}
