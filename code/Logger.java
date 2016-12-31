package storm.starter.lr_bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.File;
import java.io.IOException;


public class Logger extends BaseBasicBolt {

  final long nanoSecMin = 60000000000L;

  double  sum     = 0;
  long    count   = 0;
  boolean written = false;
  boolean flag    = false;

  long    startTime;
  long    intervalFrom;
  long    intervalTo;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {

    // set boundaries of measurement interval
    if (!flag) {
      startTime    = System.nanoTime();
      intervalFrom = startTime + nanoSecMin;
      intervalTo   = startTime + nanoSecMin * 6;
      flag         = true;
    }

    long    emitTime = input.getLongByField("EmitTime");
    int     time     = input.getIntegerByField("Time");
    String  location = input.getStringByField("Location");
    boolean accident = input.getBooleanByField("Accident");

    long    timeNow  = System.nanoTime();
    long    latency  = timeNow - emitTime;

    boolean inInterval =
      timeNow >= intervalFrom && timeNow < intervalTo;

    if (inInterval) {
      count++;
      sum += (double) latency / 1000000;  // latency in ms
    }


    if (timeNow >= intervalTo && !written) {

      try {
        BufferedWriter out =
          new BufferedWriter(new FileWriter("results.txt", true));
        double latency_ms = (double) (sum / count);
        out.write("500," + count + "," + latency_ms + "\n");
        out.close();
      }
      catch (IOException e) {}
      finally { written = true;}

    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

}