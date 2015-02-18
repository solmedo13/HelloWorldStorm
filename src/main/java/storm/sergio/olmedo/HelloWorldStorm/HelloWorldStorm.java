package storm.sergio.olmedo.HelloWorldStorm;

import storm.sergio.olmedo.HelloWorldStorm.bolts.AnalizerDataBolt;
import storm.sergio.olmedo.HelloWorldStorm.spouts.DataStreamingConsumerSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class HelloWorldStorm {
	public static void main(String[] args) throws InterruptedException {
		Config config = new Config();
		config.put("inputFile", "E:\\GitHub\\HelloWorldStorm\\src\\resources\\HelloWorld" );
		config.setDebug(true);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("readDataSpout",new DataStreamingConsumerSpout());
		builder.setBolt("analyzerDataBolt", new AnalizerDataBolt()).shuffleGrouping("readDataSpout");

	

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("HelloStorm", config, builder.createTopology());
		
		//Thread.sleep(10000);
		
		
		//cluster.shutdown();
	}
}
