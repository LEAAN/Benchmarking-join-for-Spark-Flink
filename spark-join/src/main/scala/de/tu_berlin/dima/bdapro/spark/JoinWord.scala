package de.tu_berlin.dima.bdapro.spark

import org.apache.spark.{SparkConf, SparkContext}

object JoinWord {

  def main(args: Array[String]) {
    if (args.length != 3) {
      Console.err.println("Usage: <jar> inputPath outputPath")
      System.exit(-1)
    }

    val inputPath1 = args(0)
    val inputPath2 = args(1)
    val outputPath = args(2)

    val conf = new SparkConf()
//    Seems that these lines shall only be used on local machine
      .setAppName("Simple Application").setMaster("local")
      .set("spark.default.parallelism", "28")
//      .set("spark.driver.cores", "4")
//              .set("spark.shuffle.manager", "tungsten-sort")
    val sc = new SparkContext(conf)

    val emp = sc.textFile(inputPath1).flatMap(_.toLowerCase.split("\n"))
      .map(t => (t, 2))
    val dept = sc.textFile(inputPath2).flatMap(_.toLowerCase.split("\n"))
      .map(t => (t, 1))

    val joindata = emp.join((dept))
      val happening = joindata.toDebugString
    println("**********************************")
    println(happening)
//    print(joindata)

    joindata.foreach(println)
//      joindata.saveAsTextFile(outputPath)
//    sc.stop()
  }
}



//    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("CountingSheep")
//      .set("spark.executor.memory", "1g")
//    val sc = new SparkContext(conf)


//    sc.setLogLevel("WARN")
//  sc.setLogLevel("WARN")
//      sc.setLogLevel("TRACE")
//    val emp = sc.parallelize(Seq(("jordan",10), ("ricky",20), ("matt",30), ("mince",35), ("rhonda",30)))
//    val dept = sc.parallelize(Seq(("hadoop",10), ("spark",20), ("hive",30), ("sqoop",40)))
//    val shifted_fields_emp = emp.map(t => (t._2, t._1))
//    val shifted_fields_dept = dept.map(t => (t._2,t._1))
//    shifted_fields_emp.join(shifted_fields_dept).saveAsTextFile(outputPath)