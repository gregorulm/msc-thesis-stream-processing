package storm.starter.lr_bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;


public class Accidents extends BaseBasicBolt {

  // key: unique identifier for location, val: car status
  Map<String, HashSet<Integer>> status =
    new HashMap<String, HashSet<Integer>>();

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {

    int     xway     = input.getIntegerByField("XWay");
    int     lane     = input.getIntegerByField("Lane");
    int     dir      = input.getIntegerByField("Dir");
    int     seg      = input.getIntegerByField("Seg");
    String  location = xway + "." + lane + "." + dir + "." + seg;


    if (input.getSourceStreamId().equals("StoppedCars")) {

      int     time     = input.getIntegerByField("Time");
      int     vid      = input.getIntegerByField("VID");
      boolean stopped  = input.getBooleanByField("Stopped");


      // update car status
      HashSet<Integer> cars = status.get(location);

      if (stopped) {
        if (cars == null) {
          cars = new HashSet<Integer>();
        }
        cars.add(vid);
      }

      // remove car if it was counted as stopped
      if (!stopped) {
        if (cars != null) {
          cars.remove(vid);
        }
      }

      status.put(location, cars);

    }


    if (input.getSourceStreamId().equals("Requests")) {

      HashSet<Integer> cars = status.get(location);

      long    emitTime = input.getLongByField("EmitTime");
      int     time     = input.getIntegerByField("Time");
      boolean accident = true;

      if (cars == null || cars.size() < 2) {
         accident = false;
      }

      collector.emit(
         "Responses",
         new Values(emitTime, time, location, accident));
    }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declareStream(
      "Responses",
      new Fields("EmitTime", "Time", "Location", "Accident"));
  }

}