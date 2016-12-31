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


public class Position extends BaseBasicBolt {

  // all stopped cars
  // key: VID, value: time of last position report
  Map<Integer, Integer> stoppedCars = new HashMap<Integer, Integer>();

  // key: location, val: car status
  Map<String, Map<Integer, Pair<Integer, Integer>>> allPos =
    new HashMap<String, Map<Integer, Pair<Integer, Integer>>>();


  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {

    if (input.getSourceStreamId().equals("Positions")) {

      int    time     = input.getIntegerByField("Time");
      int    vid      = input.getIntegerByField("VID");
      int    xway     = input.getIntegerByField("XWay");
      int    lane     = input.getIntegerByField("Lane");
      int    dir      = input.getIntegerByField("Dir");
      int    seg      = input.getIntegerByField("Seg");
      int    pos      = input.getIntegerByField("Pos");
      String location = xway + "." + lane + "." + dir + "." + seg + "." + pos;

      Map<Integer, Pair<Integer, Integer>> carsAtPos = allPos.get(location);

      // is there an entry for the current position?
      if (allPos.get(location) == null) {
        carsAtPos = new HashMap<Integer, Pair<Integer, Integer>>();
        allPos.put(location, carsAtPos);
      }

      // has the current vid been encountered at the current location?
      if (allPos.get(location).get(vid) == null) {
        Pair<Integer, Integer> initStats = new Pair<Integer, Integer>(time, 1);
        carsAtPos = allPos.get(location);
        carsAtPos.put(vid, initStats);
        allPos.put(location, carsAtPos);
      }

      // car has been seen (take into account "send at least once semantics")
      Pair<Integer, Integer> carStatus = allPos.get(location).get(vid);
      int vidTime  = carStatus.getKey();
      int vidCount = carStatus.getValue();

      if (vidTime != time) {  // values are identical if tuple was resent
        carsAtPos = allPos.get(location);
        Pair<Integer, Integer> stats;

        if ((time - vidTime) <= 30) { // reset
          vidCount += 1;
          stats = new Pair<Integer, Integer>(time, vidCount);
        } else {
          stats = new Pair<Integer, Integer>(time, 1);
        }
        carsAtPos.put(vid, stats);
        allPos.put(location, carsAtPos);
      }

      // now we can use the count of the car status to determine whether
      // a car has been stopped, i.e. count >= 4
      carStatus = allPos.get(location).get(vid);

      if (carStatus.getValue() >= 4) {
        // if vid already in stoppedCars, then only update value:
        // otherwise, insert, and emit tuples
        if (stoppedCars.get(vid) == null) {
          boolean stopped = true;
          // emit values for current seg and last three segments;
          // car stopped
          for (int i = 0; i < 4; i++) {
            collector.emit(
              "StoppedCars",
              new Values(time, xway, lane, dir, seg - i, vid, stopped));
          }
        }
        stoppedCars.put(vid, time);
      }

      // determine whether all cars in stoppedCars are still stopped
      // if not, send new tuple to next bolt
      int remove = -1;

      for (int key : stoppedCars.keySet()) {  // key is vid
        int last_time = stoppedCars.get(key);
        if (time - last_time > 90) {  //
        boolean stopped = false;
        remove = key;

        // emit values for current seg and last three segments;
        // car no longer stopped
        for (int i = 0; i < 4; i++) {
          collector.emit(
            "StoppedCars",
            new Values(time, xway, lane, dir, seg - i, key, stopped));
        }

        break;
        }
      }
      /*
        we break out of the for loop to avoid concurrent modification errors;
        this is not problematic due to the very small number of stopped cars,
        the fact that cars stop and restart at different times, and the large
        number of position reports (this check is performed for every position
        report!)
      */
      if (remove != -1) {
        stoppedCars.remove(remove);
      }

    }
  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declareStream(
      "StoppedCars",
      new Fields("Time", "XWay", "Lane", "Dir", "Seg", "VID", "Stopped"));

  }

}
