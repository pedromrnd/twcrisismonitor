package br.ufrj.cos.pmiranda.twcrisismonitor


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import scala.collection.JavaConversions._

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

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Read dictionary with company's categories terms
    val dictCompany = new Company(dictfilenm)
    val filters = dictCompany.getTermsArray

    //Conf scc
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("snMonitor Application")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(15))
    

    //Twitter Configurations
    val twstream = TwitterUtils.createStream(ssc, None, filters)
    val twcollector = twstream.map(status => (dictCompany.getName,status.getText))

    //unifiedStream aggregates all input streams
    val unifiedStream =  if(stubport == 0){
      twcollector
    }else {
      //Add a stub stream listening to socket on stubport
      val stubstream = ssc.socketTextStream("localhost", stubport)
      val stubcollector = stubstream.map(text => (dictCompany.getName, text))
      twcollector.union(stubcollector)
    }
    unifiedStream.print()

    //Split text in words
    val words = unifiedStream.flatMapValues(_.toLowerCase.split("[^a-zA-Z0-9@#-]"))

    //Emit categories related to a word
    val categories = words.flatMap({case (company, word) => (dictCompany.wordCategory(word)).map(cat => ((company, cat),1.0))})

    //Gets tuples with a 0.0 for each category
    val catTuples = dictCompany.categoryTuples()

    //Converts from Java to Scala format
    var list = List[Tuple2[Tuple2[String, String], Double]]()
    for (tup <- catTuples) {
      var scalaTup = ((tup._1._1, tup._1._2), tup._2.doubleValue)
      list = scalaTup :: list
    }
    //Transforms the list of tuples into an RDD
    val zerosRDD = sc.parallelize(list)

    //Creates a union between categories and zerosRDD
    val categoriesWithZeros = categories.transform(rdd => rdd.union(zerosRDD))

    //Sum categories count
    val categoryCounts = categoriesWithZeros.reduceByKey(_+_)

    //Write categories count log to a file
    val monitorWriter = new MonitorWriter(outdir)
    categoryCounts.foreachRDD((rdd,time) =>  monitorWriter.appendCategoryCount(rdd.collect(),time))

    //Compute stats for a short time window and long time window
    val categoryCountsHistoricWindow = categoryCounts.window(Minutes(historicinterval), Seconds(15)).groupByKey
    val categoryCountsRecentWindow = categoryCounts.window(Minutes(recenticinterval), Seconds(15)).groupByKey
    val historicStat = categoryCountsHistoricWindow.mapValues( value => org.apache.spark.util.StatCounter(value))
    val recentStat = categoryCountsRecentWindow.mapValues( value => org.apache.spark.util.StatCounter(value))
    val stats = historicStat.join(recentStat)
    stats.print()

    //Get only critical categories stats
    val critical_stats = stats.filter(({case ((company,category),(historicStat,recentStat)) =>
                                          recentStat.mean > (historicStat.mean + (limit_stdev_cof * historicStat.stdev))
                                      }))

    //Activate alarm for critical categories stats
    critical_stats.foreachRDD((rdd,time) =>  monitorWriter.activateAlarm(rdd.collect(),time))


    ssc.start()
    ssc.awaitTermination()
  }
}
