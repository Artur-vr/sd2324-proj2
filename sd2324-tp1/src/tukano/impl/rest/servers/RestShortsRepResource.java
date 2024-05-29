package tukano.impl.rest.servers;

import java.util.List;

import org.apache.jute.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.inject.Singleton;
import jakarta.ws.rs.ext.Provider;
import tukano.api.Short;
import tukano.impl.api.java.ExtendedShorts;
import tukano.impl.api.rest.RestExtendedShorts;
import tukano.impl.java.servers.JavaShorts;
import utils.IP;
import utils.kafka.lib.KafkaPublisher;
import utils.kafka.lib.KafkaSubscriber;
import utils.kafka.lib.RecordProcessor;
import utils.kafka.sync.SyncPoint;

@Singleton
@Provider
public class RestShortsRepResource extends RestResource implements RestExtendedShorts, RecordProcessor {

	final ExtendedShorts impl;

	static final String FROM_BEGINNING = "earliest";
	static final String TOPIC = "single_partition_topic";
	static final String KAFKA_BROKERS = "kafka:9092";

	final String replicaId;
	final KafkaPublisher sender;
	final KafkaSubscriber receiver;
	final SyncPoint<String> sync;
	
	public RestShortsRepResource() {
		this.replicaId = IP.hostName();
		this.sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		this.receiver.start(false, this);
		this.sync = new SyncPoint<>();
		
		this.impl = new JavaShorts();
	}
	
	
	@Override
	public Short createShort(String userId, String password) {
		return super.resultOrThrow( impl.createShort(userId, password));
	}

	@Override
	public void deleteShort(String shortId, String password) {
		super.resultOrThrow( impl.deleteShort(shortId, password));
	}

	@Override
	public Short getShort(String shortId) {
		return super.resultOrThrow( impl.getShort(shortId));
	}
	@Override
	public List<String> getShorts(String userId) {
		return super.resultOrThrow( impl.getShorts(userId));
	}

	@Override
	public void follow(String userId1, String userId2, boolean isFollowing, String password) {
		super.resultOrThrow( impl.follow(userId1, userId2, isFollowing, password));
	}

	@Override
	public List<String> followers(String userId, String password) {
		return super.resultOrThrow( impl.followers(userId, password));
	}

	@Override
	public void like(String shortId, String userId, boolean isLiked, String password) {
		super.resultOrThrow( impl.like(shortId, userId, isLiked, password));
	}

	@Override
	public List<String> likes(String shortId, String password) {
		return super.resultOrThrow( impl.likes(shortId, password));
	}

	@Override
	public List<String> getFeed(String userId, String password) {
		return super.resultOrThrow( impl.getFeed(userId, password));
	}

	@Override
	public void deleteAllShorts(String userId, String password, String token) {
		super.resultOrThrow( impl.deleteAllShorts(userId, password, token));
	}


	@Override
	public void onReceive(ConsumerRecord<String, String> r) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'onReceive'");
	}	
}
