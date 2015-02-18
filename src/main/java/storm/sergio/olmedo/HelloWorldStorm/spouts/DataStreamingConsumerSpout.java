package storm.sergio.olmedo.HelloWorldStorm.spouts;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DataStreamingConsumerSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private TopologyContext context;
	private Map conf;

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.conf=conf;
		abrirFichero();
		

	}
	private void abrirFichero(){
		try {
			this.context = context;
			this.fileReader = new FileReader(conf.get("inputFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error readinf file"
					+ conf.get("inputFile"));

		}
	}

	public void nextTuple() {

		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				this.collector.emit(new Values(str), str);

			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading type", e);
		} 

	}


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public void close() {
		try {
			fileReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean isDistributed() {
		return false;
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
