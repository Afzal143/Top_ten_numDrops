/**
  * Created by afzal on 4/3/17.
  */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CallDetailsProblem {
  def main(args:Array[String]){

    //First we’ll read the data from the csv file
    val sc = new SparkContext(new   SparkConf().setAppName("CallDetailsProblem ").setMaster("local[2]"))
    val logFile = "/home/afzal/CDR.csv"
    val text = sc.textFile(logFile)

    //As we’re dealing with a CSV file with no headers it’s a good idea to define a case class that defines the schema
    //define the schema using a case class

    case class Call(visitor_locn: String, call_duration:
    Integer, phone_no: String, error_code: String)

    // create a RDD of Calls
    val calls = text.map(_.split(",")).map(p =>
      Call(p(0),p(1).toInt,p(2),p(3)))

    println(calls.count());
    calls.foreach {
      x =>  println(x)
    }

    var result = calls.map(x => (x.visitor_locn,1)).reduceByKey(_+_).collect.sortBy(_._2);

    // println(result.reverse.mkString("\n"));
    //Number of 10 most customers having errors:
    var result2 = calls.map(x => (x.error_code,1)).reduceByKey(_+_).collect.sortBy(_._2);
    println(result2.reverse.mkString("\n"));

  }


}

