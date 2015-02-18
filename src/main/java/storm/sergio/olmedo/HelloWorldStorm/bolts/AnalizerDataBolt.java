package storm.sergio.olmedo.HelloWorldStorm.bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class AnalizerDataBolt implements IRichBolt {
	private OutputCollector collector;
	private String [] words;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}
	public void execute(Tuple input){
		String sentence = input.getString(0);
		this.words = sentence.split(" ");
		for(String word: words){
			word=word.trim();
			if(!word.isEmpty()){
				word = word.toLowerCase();
				System.out.println(word + " ");
				collector.emit(new Values(word));
				
			}
			
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println("Numero de palabras = " + words.length);
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			
			
		}
		collector.ack(input);
		
		
	}
	
	public void cleanup() {
	
	}
	


	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	
	
}
