package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import storm.starter.lr_spout.Source;
import storm.starter.lr_bolts.Split;
import storm.starter.lr_bolts.Position;
import storm.starter.lr_bolts.Accidents;
import storm.starter.lr_bolts.Logger;


public class LinearRoadMain {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout(
      "spout",
      new Source(), 1)
        .setMaxSpoutPending(4);
        // avoids congestion and also ensures that there are
        // enough tuples in flight

    builder.setBolt(
      "split",
      new Split(), 2)
        .shuffleGrouping(
          "spout",
          "RawData");

    builder.setBolt(
      "position",
      new Position(), 3)
        .fieldsGrouping(
          "split",
          "Positions",
          new Fields("XWay", "Lane"));

    builder.setBolt(
      "accidents",
      new Accidents(), 1)
        .shuffleGrouping(
          "position",
          "StoppedCars")
        .shuffleGrouping(
          "split",
          "Requests");

    builder.setBolt(
      "logger",
      new Logger(), 1)
        .shuffleGrouping(
          "accidents",
          "Responses");

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(2);
      StormSubmitter.submitTopologyWithProgressBar(
        args[0], conf, builder.createTopology());

    } else { // run locally for testing
      conf.setMaxTaskParallelism(1);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("linear-road", conf, builder.createTopology());

      // run topology for 7 minutes
      Thread.sleep(420000);

      cluster.shutdown();
    }

    System.out.println("Done.\n");
  }

}
