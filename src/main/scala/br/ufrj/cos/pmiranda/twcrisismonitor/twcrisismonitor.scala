package br.ufrj.cos.pmiranda.twcrisismonitor


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import scala.collection.JavaConversions._
import org.apache.spark.sql._
import com.databricks.spark.corenlp.functions._





object twcrisismonitor {
  def main(args: Array[String]) {
  
    //Get parameters
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val dictfilenm = args(4) //filepath of a dictionary with company's categories terms
    val outdir = args(5) //output directory
    val historicinterval = args(6).toInt //in minutes
    val recenticinterval= args(7).toInt //in minutes
    val stubport = args(8).toInt //set 0 to disable stub
    val limit_stdev_cof = args(9).toDouble //in minutes
    val clusername = args(10)
    val clpassword = args(11)
    val clhost = args(12)
    val threshold = args(13).toDouble

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Read dictionary with company's categories terms
    val dictCategory = new Category(dictfilenm)
    val filters = dictCategory.getTermsArray

    //Conf scc
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("twcrisismonitor Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(15))
    val spark = SparkSession
    .builder
    .config("cloudant.host",clhost)
    .config("cloudant.username", clusername)
    .config("cloudant.password",clpassword)
    //.config("createDBOnSave","true") // to create a db on save
    .config("jsonstore.rdd.partitions", "20") // using 20 partitions
    .getOrCreate()
    //val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    import spark.implicits._

    

    //Twitter Configurations
    val twstream = TwitterUtils.createStream(ssc, None, filters)
    val filtredtwstream = twstream.filter(status => status.getLang == dictCategory.getLanguage)
    val twcollector = filtredtwstream.map(status => (dictCategory.getName, dictCategory.getEmail,status.getText))
    //unifiedStream aggregates all input streams
    val unifiedStream =  if(stubport == 0){
      twcollector
    }else {
      //Add a stub stream listening to socket on stubport
      val stubstream = ssc.socketTextStream("localhost", stubport)
      val stubcollector = stubstream.map(text => (dictCategory.getName, dictCategory.getEmail, text))
      twcollector.union(stubcollector)
    }
    //unifiedStream.print()
    
    unifiedStream.foreachRDD(rdd =>
       if (! rdd.isEmpty) {
        val df = rdd.toDF("category","email","text")
        val sentimentdf = df.select('category, 'email, 'text, sentiment('text).as('sentimentscr))
        sentimentdf.createOrReplaceTempView("sentimentdf")
        val sqlDF = spark.sql("SELECT category, SUM(CASE WHEN sentimentscr = 1 THEN 1 ELSE 0 END) as negative, SUM(CASE WHEN sentimentscr > 1 THEN 1 ELSE 0 END) as notnegative, current_timestamp() as time, email FROM sentimentdf GROUP BY category, email")
        sqlDF.show(20, false)
        //if negative ratio > threshold send alarm
        if (1.0 * sqlDF.head.getLong(1)/(sqlDF.head.getLong(1)+sqlDF.head.getLong(2)+0.9)>threshold){
          sqlDF.write.format("com.cloudant.spark").save("twitter_alarm")
          println("ALARM!")
        }
        sqlDF.write.format("com.cloudant.spark").save("twitter_sentiments")
        
       }
    )
    
    ssc.start()
    ssc.awaitTermination()
  }
}
