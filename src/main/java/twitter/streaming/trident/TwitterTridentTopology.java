package twitter.streaming.trident;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import twitter.streaming.ApiStreamingSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;


public class TwitterTridentTopology {
	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		
		TridentState state = topology
				.newStream("tweets", new ApiStreamingSpout())
					.each(new Fields("tweet"),new HashTagSplitter(), new Fields("hashtag"))
				.groupBy(new Fields("hashtag"))
				.persistentAggregate(new RedisState.Factory(), 
									 new Count(), 
									 new Fields("pepe"))
				.parallelismHint(3);
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.put("redisHost", "localhost");
		conf.put("redisPort", 6379);
		conf.put("track", "jump,men,usa,argentina");
		conf.put("user", "geisbruch");
		conf.put("password", "peludo");

		cluster.submitTopology("trident-twitter-topology", conf, topology.build());

	}
}