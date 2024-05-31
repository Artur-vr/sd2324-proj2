package tukano.impl.rest.servers;

import java.util.List;

import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;

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
	
	private static Logger Log = Logger.getLogger(RestShortsRepResource.class.getName());
	
	static final String FROM_BEGINNING = "earliest";
	static final String TOPIC = "single_partition_topic";
	static final String KAFKA_BROKERS = "kafka:9092";
	
	final String replicaId;
	final KafkaPublisher publisher;
	final KafkaSubscriber subscriber;
	
    protected Gson gson = new Gson();
    
    private SyncPoint<Result> sync;
    
    private Long version;
    
	public RestShortsRepResource(long v) {
		
		this.impl = new JavaShorts();
		this.version = v;
		this.replicaId = IP.hostName();
		
		this.publisher = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        this.subscriber = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC), FROM_BEGINNING);
        
        this.sync = new SyncPoint<Result>();
        this.subscriber.start(false,  this);
        
		
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
		
		Result<Short> result = impl.createShort(userId, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "createShort", encodedData);
			}
		return super.resultOrThrow( impl.createShort(userId, password));
	}

	@Override
	public void deleteShort(String shortId, String password) {
		Result<Void> result = impl.deleteShort(shortId, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "deleteShort", encodedData);
			}
		super.resultOrThrow( impl.deleteShort(shortId, password));
	}

	@Override
	public Short getShort(String shortId) {
		Result<Short> result = impl.getShort(shortId);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "getShort", encodedData);
			}
		return super.resultOrThrow( impl.getShort(shortId));
	}
	@Override
	public List<String> getShorts(String userId) {
		Result<List<String>> result = impl.getShorts(userId);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "getShorts", encodedData);
			}
		return super.resultOrThrow( impl.getShorts(userId));
	}

	@Override
	public void follow(String userId1, String userId2, boolean isFollowing, String password) {
		Result<Void> result = impl.follow(userId1, userId2, isFollowing, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "follow", encodedData);
			}
		super.resultOrThrow( impl.follow(userId1, userId2, isFollowing, password));
	}

	@Override
	public List<String> followers(String userId, String password) {
		Result<List<String>> result = impl.followers(userId, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "followers", encodedData);
			}
		return super.resultOrThrow( impl.followers(userId, password));
	}

	@Override
	public void like(String shortId, String userId, boolean isLiked, String password) {
		Result<Void> result = impl.like(shortId, userId, isLiked, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "like", encodedData);
			}
		super.resultOrThrow( impl.like(shortId, userId, isLiked, password));
	}

	@Override
	public List<String> likes(String shortId, String password) {
		Result<List<String>> result = impl.likes(shortId, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "likes", encodedData);
			}
		return super.resultOrThrow( impl.likes(shortId, password));
	}

	@Override
	public List<String> getFeed(String userId, String password) {
		Result<List<String>> result = impl.getFeed(userId, password);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "getFeed", encodedData);
			}
		return super.resultOrThrow( impl.getFeed(userId, password));
	}

	@Override
	public void deleteAllShorts(String userId, String password, String token) {
		Result<Void> result = impl.deleteAllShorts(userId, password, token);
		if(result.isOK()) {
			String encodedData = gson.toJson(result.value());
			publish(version, "deleteAllShorts", encodedData);
			}
		super.resultOrThrow( impl.deleteAllShorts(userId, password, token));
	}
	
	
	private <T> Result<T> publish(Long version, String op, String value){
		if(this.version == null) {
			this.version = 0L;
		}
		
		long offset = publisher.publish(TOPIC, op, value);
		this.version = offset;
		if(version != null) {
			sync.waitForVersion(version, 2000);
		}
		
		Result<T> result = sync.waitForResult(offset);
		return  result;
	}


}
