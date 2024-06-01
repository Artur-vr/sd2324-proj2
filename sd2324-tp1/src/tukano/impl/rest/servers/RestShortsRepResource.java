package tukano.impl.rest.servers;

import java.util.List;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import jakarta.inject.Singleton;
import jakarta.ws.rs.ext.Provider;
import tukano.api.Short;
import tukano.api.java.Result;
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

	private static Logger Log = Logger.getLogger(RestShortsRepResource.class.getName());


	final String replicaId;
	final KafkaPublisher publisher;
	final KafkaSubscriber subscriber;
	@SuppressWarnings("rawtypes")
	final SyncPoint<Result> sync;
	private Long version;

	
	public RestShortsRepResource() {
		replicaId = IP.hostAddress();
		publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
		subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
		subscriber.start(false, this);
		sync = new SyncPoint<>();
		version = 0L;
		
		impl = new JavaShorts();
	}

	@Override
	public void onReceive(ConsumerRecord<String, String> r) {
		Result<?> result = null;

		try {
            String method = r.key();
            String[] param = r.value().split(",");
            switch (method) {
                case "createShort":
                    result = impl.createShort(param[0], param[1]); break;
                case "deleteShort":
                    result = impl.deleteShort(param[0], param[1]); break;
                case "getShort":
                    result = impl.getShort(param[0]); break;
                case "getShorts":
                    result = impl.getShorts(param[0]); break;
				case "follow":
					result = impl.follow(param[0], param[1], Boolean.parseBoolean(param[2]), param[3]); break;
                case "followers":
                    result = impl.followers(param[0], param[1]); break;
				case "like":
					result = impl.like(param[0], param[1], Boolean.parseBoolean(param[2]), param[3]); break;
				case "likes":
					result = impl.likes(param[0], param[1]); break;
				case "getFeed":
					result = impl.getFeed(param[0], param[1]); break;
				case "deleteAllShorts":
					result = impl.deleteAllShorts(param[0], param[1], param[2]); break;
                default:
                    Log.severe("WRONG COMMAND\n");
            }
            sync.setResult(r.offset(),result);

        } catch (Exception e){
            Log.severe("Exception in OnREceive function\n");
        }

	}	
	
	
	@Override
	public Short createShort(String userId, String password) {
		version = publisher.publish(TOPIC, "createShort", userId + "," + password);
		Result<Short> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}

	@Override
	public void deleteShort(String shortId, String password) {
		version = publisher.publish(TOPIC, "deleteShort", shortId + "," + password);
		Result<Void> result = sync.waitForResult(version);
		super.resultOrThrow( result );
	}

	@Override
	public Short getShort(String shortId) {
		version = publisher.publish(TOPIC, "getShort", shortId);
		Result<Short> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}
	@Override
	public List<String> getShorts(String userId) {
		version = publisher.publish(TOPIC, "getShorts", userId);
		Result<List<String>> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}

	@Override
	public void follow(String userId1, String userId2, boolean isFollowing, String password) {
		version = publisher.publish(TOPIC, "follow", userId1 + "," + userId2 + "," + isFollowing + "," + password);
		Result<Void> result = sync.waitForResult(version);
		super.resultOrThrow( result );
	}

	@Override
	public List<String> followers(String userId, String password) {
		version = publisher.publish(TOPIC, "followers", userId + "," + password);
		Result<List<String>> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}

	@Override
	public void like(String shortId, String userId, boolean isLiked, String password) {
		version = publisher.publish(TOPIC, "like", shortId + "," + userId + "," + isLiked + "," + password);
		Result<Void> result = sync.waitForResult(version);
		super.resultOrThrow( result );
	}

	@Override
	public List<String> likes(String shortId, String password) {
		version = publisher.publish(TOPIC, "likes", shortId + "," + password);
		Result<List<String>> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}

	@Override
	public List<String> getFeed(String userId, String password) {
		version = publisher.publish(TOPIC, "getFeed", userId + "," + password);
		Result<List<String>> result = sync.waitForResult(version);
		return super.resultOrThrow( result );
	}

	@Override
	public void deleteAllShorts(String userId, String password, String token) {
		version = publisher.publish(TOPIC, "deleteAllShorts", userId + "," + password + "," + token);
		Result<Void> result = sync.waitForResult(version);
		super.resultOrThrow( result );
	}



}
