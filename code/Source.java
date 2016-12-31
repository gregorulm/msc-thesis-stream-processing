package storm.starter.lr_spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.util.ArrayList;
import java.util.Map;


public class Source extends BaseRichSpout {

  SpoutOutputCollector collector;

  final int LIMIT  = 500;

  String lr_file   = "cardatapoints_1.out0";
  int    tupleID   = 0;
  int    bufferPos = 0;
  int    iteration = 0;

  String[] buffer;

  long thisSec;
  long nextSec;
  int emittedInCurrentSec;

  @Override
  public void open(Map conf,
                   TopologyContext context,
                   SpoutOutputCollector collector) {

    thisSec             = System.currentTimeMillis();
    nextSec             = thisSec + 1000;
    emittedInCurrentSec = 0;
    this.collector      = collector;

    try {
      
      BufferedWriter out = new BufferedWriter
      (new FileWriter("start.txt", true));
      out.write(System.currentTimeMillis() + "\n");
      out.write("OK.\n");
      out.close();

      FileReader        fileReader = new FileReader(lr_file);
      BufferedReader    reader     = new BufferedReader(fileReader);
      int               count      = 0;
      String            entry      = null;
      ArrayList<String> tmp        = new ArrayList<String>();

      while ((entry = reader.readLine()) != null){
        tmp.add(entry);
        count++;
      }

      buffer = new String[count];

      for (int i = 0; i < tmp.size(); i++) {
        buffer[i] = tmp.get(i);
      }
    }

    catch (FileNotFoundException e) {
      System.out.println("File not found.");
      String cwd = System.getProperty("user.dir");
      System.out.println("cwd: " + cwd);

    } catch(Exception e){
      throw new RuntimeException("Error reading tuple.", e);

    } finally {}

  }


  @Override
  public void nextTuple() {

      long val = System.currentTimeMillis();

      if (val >= nextSec) {
        emittedInCurrentSec = 0;
        long tmp = nextSec;
        thisSec  = tmp;
        nextSec  = thisSec + 1000;
      }

      if (val < nextSec  && emittedInCurrentSec > LIMIT) {
          return;
      }


      if (bufferPos >= buffer.length) {
        // process buffer anew
        iteration++;
        bufferPos = 0;
      }

      String   entry     = buffer[bufferPos];
      String[] allValues = entry.split(",");
      int[]    vals      = new int[allValues.length];

      for(int i = 0; i < vals.length; i++) {
        vals[i] = Integer.parseInt(allValues[i]);
      }

      int type = vals[0];
      int time = vals[1];
      int vid  = vals[2];
      //int spd  = vals[3];
      int xway = vals[4];
      int lane = vals[5];
      int dir  = vals[6];
      int seg  = vals[7];
      int pos  = vals[8];

      // add one hour (3600 sec) for each iteration
      time += iteration * 3600;

      long emitTime = System.nanoTime();

      // only consider position reports
      if (type == 0) {
        collector.emit(
          "RawData",
          new Values(
            emitTime,
            time, vid, xway, lane, dir, seg, pos), tupleID++);   
      }

      emittedInCurrentSec++;
      bufferPos++;

}


  @Override
  public void ack(Object id) {
  }


  @Override
  public void fail(Object id) {
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declareStream(
      "RawData",
        new Fields(
          "EmitTime", "Time",
          "VID", "XWay", "Lane", "Dir", "Seg", "Pos"));
  }

}