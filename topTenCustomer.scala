
//Find out top 10 phone_number facing frequently call drops in Roaming

import org.apache.spark.sql.SQLContext
import spark.implicits._


val df = spark.read.csv("/Users/afzal/Desktop/Big_Data_1/CDR.csv").toDF("visitor_locn", "call_dura", "phone_num", "error_code")
df.show()

df.createOrReplaceTempView("callDrop")
val sqlDF = spark.sql("select phone_num, call_dura from callDrop where call_dura = 1")
sqlDF.show()
val callNumDropTopTen = sqlDF.groupBy("phone_num").count().orderBy(desc("count")).show(10)



