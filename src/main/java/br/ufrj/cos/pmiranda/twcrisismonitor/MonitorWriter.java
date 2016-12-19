package br.ufrj.cos.pmiranda.twcrisismonitor;
import org.apache.spark.streaming.Time;
import org.apache.spark.util.StatCounter;
import scala.*;
import scala.Double;

import java.io.*;
import java.lang.Float;

public class MonitorWriter {
    private File outdir;
    private PrintWriter count_file;
    private PrintWriter alarm_file;

    MonitorWriter(String outdir) throws IOException {
        this.outdir = new File(outdir);
        count_file = new PrintWriter(new BufferedWriter(new FileWriter(new File(this.outdir,"categories_count.txt"), true)));
        alarm_file = new PrintWriter(new BufferedWriter(new FileWriter(new File(this.outdir,"alarms.txt"), true)));
    }
    public void appendCategoryCount(Tuple2<Tuple2<String, String>, scala.Double>[] rdds,Time time){
        for( Tuple2<Tuple2<String, String>, scala.Double> rdd:  rdds) {
            double count =scala.Double.unbox(rdd._2());
            count_file.format("%s %d %d\n", rdd._1()._2(), (int) count, time.milliseconds() / 1000L);
            //count_file.format("%s %d \n", rdd._1()._2(), time.milliseconds() / 1000L);
            count_file.flush();
        }
    }
    public void activateAlarm(Tuple2<Tuple2<String, String>,Tuple2<StatCounter, StatCounter>>[] rdds,Time time){
        for( Tuple2<Tuple2<String, String>,Tuple2<StatCounter, StatCounter>> rdd:  rdds) {
            String company = rdd._1()._1();
            String category = rdd._1()._2();
            System.out.format("Alarm: %d %s %s\n",time.milliseconds() / 1000L, company, category);
            alarm_file.format("%d %s %s\n",time.milliseconds() / 1000L, company, category);
            alarm_file.flush();
        }
    }
}
