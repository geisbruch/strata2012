package twitter.streaming;

import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TwitterDataExtractor extends BaseBasicBolt{
	private static final long serialVersionUID = -3025639777071957758L;
	
	static JSONParser jsonParser = new JSONParser();
	
	@Override
	public void cleanup() {
	}
 
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = (String)input.getValueByField("tweet");
		try {
			JSONObject jsonObject = (JSONObject) jsonParser.parse(json);
			if(jsonObject.containsKey("entities")){
				JSONObject entities = (JSONObject) jsonObject.get("entities");
				if(entities.containsKey("hashtags")){
					for(Object obj : (JSONArray)entities.get("hashtags")){
						JSONObject hashObj = (JSONObject) obj;
						collector.emit(new Values(input.getString(0),hashObj.get("text").
								toString().toLowerCase()));
					}
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("trackList","hashtag"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

}
