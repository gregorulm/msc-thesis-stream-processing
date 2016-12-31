package storm.starter.lr_bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class Split extends BaseBasicBolt {

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {

    int  time     = input.getIntegerByField("Time");
    int  vid      = input.getIntegerByField("VID");
    int  xway     = input.getIntegerByField("XWay");
    int  lane     = input.getIntegerByField("Lane");
    int  dir      = input.getIntegerByField("Dir");
    int  seg      = input.getIntegerByField("Seg");
    int  pos      = input.getIntegerByField("Pos");
    long emitTime = input.getLongByField("EmitTime");
    

    collector.emit(
      "Positions",
      new Values(time, vid, xway, lane, dir, seg, pos));

    collector.emit(
      "Requests",
      new Values(emitTime,
        time, xway, lane, dir, seg));

  }


  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declareStream(
      "Positions",
      new Fields("Time", "VID", "XWay", "Lane", "Dir", "Seg", "Pos"));

    declarer.declareStream(
      "Requests",
      new Fields("EmitTime",
        "Time", "XWay", "Lane", "Dir", "Seg"));

  }

}