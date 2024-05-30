package tukano.impl.rest.servers;

import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;

import tukano.api.Short;
import tukano.api.java.Result;
import tukano.api.java.Shorts;
import utils.kafka.lib.KafkaPublisher;
import utils.kafka.lib.KafkaSubscriber;
import utils.kafka.lib.RecordProcessor;
import utils.kafka.sync.SyncPoint;

public class RepShorts implements Shorts, RecordProcessor {

	private static final String SHORTS_TOPIC = "shortsTopic";

	private static final String POST = "post";

	private static final String SUB = "sub";

	private static final String UNSUB = "unsub";

	private static final String REMOVE_MSG = "removeMsg";

	private static Logger Log = Logger.getLogger(RestShortsServer.class.getName());

	private final KafkaPublisher publisher;
	private final KafkaSubscriber subscriber;

	private SyncPoint<Result<Long>> sync;

	final String KAFKA_BROKERS = "kafka:9092";
	
	
	 private Gson json;

	public RepShorts() {
		
		json = new Gson();
		this.sync = SyncPoint.getInstance();
		publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of("kafkadirectory"), "earliest");
		subscriber.start(false, (r) -> onReceive(r));
	}

	
	
	
	@Override
	public void onReceive(ConsumerRecord<String, String> r) {

		var key = r.key();
		
		//Adaptar para as nossas funcionalidades
		switch (key) {
		case POST -> receivePostMsg(r.value(), r.offset());
		case SUB -> receiveSubscribe(r.value(), r.offset());
		case UNSUB -> receiveUnsubscribe(r.value(), r.offset());
		case REMOVE_MSG -> receiveRemoveMessage(r.value(), r.offset());
		}
		sync.setResult(r.offset(), Result.ok());
	}

	@Override
	public Result<Short> createShort(String userId, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<Short> getShort(String shortId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		// TODO Auto-generated method stub
		return null;
	}

	private void receivePostMsg(String value, long offset) {
		// TODO Auto-generated method stub
	}

	private void receiveSubscribe(String value, long offset) {
		// TODO Auto-generated method stub

	}

	private void receiveUnsubscribe(String value, long offset) {
		// TODO Auto-generated method stub

	}

	private void receiveRemoveMessage(String value, long offset) {
		// TODO Auto-generated method stub
	}
}
