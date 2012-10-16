package twitter.streaming.trident;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.Split;
import twitter.streaming.ApiStreamingSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;


public class TwitterTridentTopology {
	public static void main(String[] args) throws InterruptedException {
		TridentTopology topology = new TridentTopology();
		LocalDRPC drpc = new LocalDRPC();
		
		TridentState hashtags = topology
				.newStream("tweets", new ApiStreamingSpout())
					.each(new Fields("tweet"),new HashTagSplitter(), new Fields("hashtag"))
				.groupBy(new Fields("hashtag"))
				.persistentAggregate(new RedisState.Factory(), 
									 new Count(), 
									 new Fields("count"))
				.parallelismHint(3);
		
		topology.newDRPCStream("counts",drpc)
			.each(new Fields("args"), new Split(), new Fields("hashtag"))
			.stateQuery(hashtags, new Fields("hashtag"), new MapGet(), new Fields("count"))
			.each(new Fields("count"), new FilterNull())
			.groupBy(new Fields("hashtag"))
            .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.put("redisHost", "localhost");
		conf.put("redisPort", 6379);
		conf.put("track", "jump,men,usa,argentina");
		conf.put("user", "geisbruch");
		conf.put("password", "peludo");

		
		cluster.submitTopology("trident-twitter-topology", conf, topology.build());

		Thread.sleep(60000);
		System.out.println("Tweets Count query");
		System.out.println(drpc.execute("counts", "storm strataconf hadoop"));
		

	}
}