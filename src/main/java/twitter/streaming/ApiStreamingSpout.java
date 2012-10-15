package twitter.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ApiStreamingSpout extends BaseRichSpout implements Runnable{

	static String STREAMING_API_URL="https://stream.twitter.com/1/statuses/filter.json?track=";
	private String track;
	private String user;
	private String password;
	private DefaultHttpClient client;
	private SpoutOutputCollector collector;
	private UsernamePasswordCredentials credentials;
	private BasicCredentialsProvider credentialProvider;

	static Logger LOG = Logger.getLogger(ApiStreamingSpout.class);
	
	LinkedBlockingQueue<String> tweets = new LinkedBlockingQueue<String>();
	@Override
	public void nextTuple() {
		if(tweets.size()>0){
			Collection<Object> tweetsToEmit = new ArrayList<Object>();
			tweets.drainTo(tweetsToEmit);
			for(Object tweet : tweetsToEmit){
				collector.emit(new Values(track,tweet));
			}
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		int spoutsSize = context.getComponentTasks(context.getThisComponentId()).size();
		int myIdx = context.getThisTaskIndex();
		String[] tracks = ((String) conf.get("track")).split(",");
		StringBuffer tracksBuffer = new StringBuffer();
		for(int i=0; i< tracks.length;i++){
			if( i % spoutsSize == myIdx){
				tracksBuffer.append(",");
				tracksBuffer.append(tracks[i]);
			}
		}
		
		if(tracksBuffer.length() == 0)
			throw new RuntimeException("No track found for spout" +
					" [spoutsSize:"+spoutsSize+", tracks:"+tracks.length+"] the amount" +
					" of tracks must be more then the spout paralellism");
		
		this.track =tracksBuffer.substring(1).toString();
		
		user = (String) conf.get("user");
		password = (String) conf.get("password");
		
		credentials = new UsernamePasswordCredentials(user, password);
		credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, credentials);
		this.collector = collector;
		new Thread(this).start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("criteria","tweet"));
	}

	@Override
	public void run() {
		/*
		 * Create the client call
		 */
		while(true){
			try{
				client = new DefaultHttpClient();
				client.setCredentialsProvider(credentialProvider);
				HttpGet get = new HttpGet(STREAMING_API_URL+track);		
				HttpResponse response;
				try {
					//Execute
					response = client.execute(get);
					StatusLine status = response.getStatusLine();
					if(status.getStatusCode() == 200){
						InputStream inputStream = response.getEntity().getContent();
						BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
						String in;
						//Read line by line
						while((in = reader.readLine())!=null){
							//Parse and emit
							tweets.add(in);
						}
					}
				} catch (IOException e) {
					LOG.error("Error in communication with twitter api ["+get.getURI().toString()+"]");
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
					}
				}
			}catch(Throwable e){
				LOG.error(e);
			}
			LOG.error("An error ocurs, all runnable thread will be restarted");
			try{
				Thread.sleep(1000);
			}catch(Exception e){
			}
		}
	}
}
