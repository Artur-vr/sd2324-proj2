package tukano.impl.rest.servers;

import static java.lang.String.format;
import static tukano.api.java.Result.error;
import static tukano.api.java.Result.errorOrResult;
import static tukano.api.java.Result.errorOrValue;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.java.Result.ErrorCode.FORBIDDEN;
import static tukano.api.java.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.java.Result.ErrorCode.TIMEOUT;
import static tukano.impl.java.clients.Clients.BlobsClients;
import static tukano.impl.java.clients.Clients.UsersClients;
import static utils.DB.getOne;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;

import tukano.api.Short;
import tukano.api.User;
import tukano.api.java.Blobs;
import tukano.api.java.Result;
import tukano.api.java.Shorts;
import tukano.impl.api.java.ExtendedBlobs;
import tukano.impl.api.java.ExtendedShorts;
import utils.DB;
import utils.kafka.lib.KafkaPublisher;
import utils.kafka.lib.KafkaSubscriber;
import utils.kafka.lib.RecordProcessor;
import utils.kafka.sync.SyncPoint;

public class RepShorts implements ExtendedShorts, RecordProcessor {

	private static final String SHORTS_TOPIC = "shortsTopic";

	private static final String POST = "post";

	private static final String SUB = "sub";

	private static final String UNSUB = "unsub";

	private static final String REMOVE_MSG = "removeMsg";
	

	private static final String BLOB_COUNT = "*";
	
	private static final long USER_CACHE_EXPIRATION = 3000;
	private static final long SHORTS_CACHE_EXPIRATION = 3000;
	private static final long BLOBS_USAGE_CACHE_EXPIRATION = 10000;


	private static Logger Log = Logger.getLogger(RestShortsServer.class.getName());
	
	private static final Gson gson = new Gson();
	
	AtomicLong counter = new AtomicLong(totalShortsInDatabase());

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
	
	private boolean starting = true;

	static record Credentials(String userId, String pwd) {
		static Credentials from(String userId, String pwd) {
			return new Credentials(userId, pwd);
		}
	}
	
	protected final LoadingCache<Credentials, Result<User>> usersCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Result<User> load(Credentials u) throws Exception {
					var res = UsersClients.get().getUser(u.userId(), u.pwd());
					if (res.error() == TIMEOUT)
						return error(BAD_REQUEST);
					return res;
				}
			});

	protected final LoadingCache<String, Result<Short>> shortsCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(SHORTS_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Result<Short> load(String shortId) throws Exception {

					var query = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
					var likes = DB.sql(query, Long.class);
					return errorOrValue(getOne(shortId, Short.class), shrt -> shrt.copyWith(likes.get(0)));
				}
			});

	protected final LoadingCache<String, Map<String, Long>> blobCountCache = CacheBuilder.newBuilder()
			.expireAfterWrite(Duration.ofMillis(BLOBS_USAGE_CACHE_EXPIRATION)).removalListener((e) -> {
			}).build(new CacheLoader<>() {
				@Override
				public Map<String, Long> load(String __) throws Exception {
					if(starting){
						startCacheUpdates();
						starting = false;
					}
					
					final var QUERY = "SELECT REGEXP_SUBSTRING(s.blobUrl, '^(\\w+:\\/\\/)?([^\\/]+)\\/([^\\/]+)') AS baseURI, count('*') AS usage From Short s GROUP BY baseURI";
					var hits = DB.sql(QUERY, BlobServerCount.class);

					var candidates = hits.stream()
							.collect(Collectors.toMap(BlobServerCount::baseURI, BlobServerCount::count));

					for (var uri : BlobsClients.all())
						candidates.putIfAbsent(uri.toString(), 0L);

					return candidates;

				}
			});
	
	@Override
	public void onReceive(ConsumerRecord<String, String> r) {

		var key = r.key();
		
		//Adaptar para as nossas funcionalidades
		switch (key) {
		
		}
		sync.setResult(r.offset(), Result.ok());
	}

	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		return errorOrResult(okUser(userId, password), user -> {
			var shortId = format("%s-%d", userId, counter.incrementAndGet());
			////// new code for blob server replication
			var blobUrl = "";
			var uri1 = getLeastLoadedBlobServerURI();
			var uri2 = getLeastLoadedBlobServerURI();
			if (uri1.equals(uri2)) {
				blobUrl = format("%s/%s/%s", uri1, Blobs.NAME, shortId);
			} else {
				blobUrl = format("%s/%s/%s", uri1, Blobs.NAME, shortId);
				blobUrl += "|" + format("%s/%s/%s", uri2, Blobs.NAME, shortId);
			}
			////////
			// var blobUrl = format("%s/%s/%s", getLeastLoadedBlobServerURI(), Blobs.NAME, shortId);
			////////
			var shrt = new Short(shortId, userId, blobUrl);


	        long offset = publisher.publish(SHORTS_TOPIC, POST, gson.toJson(shrt));
	        if (offset < 0) {
	            return error(INTERNAL_ERROR);
	        }
	            sync.waitForResult(offset);
	            
			return DB.insertOne(shrt);
		});
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

	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Unimplemented method 'deleteAllShorts'");
	}
	

	
	protected Result<User> okUser(String userId, String pwd) {
		try {
			return usersCache.get(new Credentials(userId, pwd));
		} catch (Exception x) {
			x.printStackTrace();
			return Result.error(INTERNAL_ERROR);
		}
	}

	private Result<Void> okUser(String userId) {
		var res = okUser(userId, "");
		if (res.error() == FORBIDDEN)
			return ok();
		else
			return error(res.error());
	}

	protected Result<Short> shortFromCache(String shortId) {
		try {
			return shortsCache.get(shortId);
		} catch (ExecutionException e) {
			e.printStackTrace();
			return error(INTERNAL_ERROR);
		}
	}

	
	private String getLeastLoadedBlobServerURI() {
		try {
			var servers = blobCountCache.get(BLOB_COUNT);

			var leastLoadedServer = servers.entrySet()
					.stream()
					.sorted((e1, e2) -> Long.compare(e1.getValue(), e2.getValue()))
					.findFirst();

			if (leastLoadedServer.isPresent()) {
				var uri = leastLoadedServer.get().getKey();
				servers.compute(uri, (k, v) -> v + 1L);
				return uri;
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
		return "?";
	}
	

	static record BlobServerCount(String baseURI, Long count) {
	};

	private long totalShortsInDatabase() {
		var hits = DB.sql("SELECT count('*') FROM Short", Long.class);
		return 1L + (hits.isEmpty() ? 0L : hits.get(0));
	}

	private void removeFromBlobUrl(String blobURL) {
		DB.transaction(hibernate -> {
			// Query to find all Short entities containing the blobURL
			var query = format("SELECT * FROM Short s WHERE s.blobUrl LIKE '%%%s%%'", blobURL);
			List<Short> shorts = hibernate.createNativeQuery(query, Short.class).list();
			for (Short shrt : shorts) {
				// Remove the blobURL from the blobUrl attribute
				String updatedBlobUrl = Stream.of(shrt.getBlobUrl().split("\\|"))
						.filter(url -> !url.contains(blobURL))
						.collect(Collectors.joining("|"));
				
				//get avaliable blobs
				var uris = BlobsClients.all();
				Set<String> uris2 = new java.util.HashSet<String>();
				for (ExtendedBlobs u : uris) {
					var ur = u.toString();
					if(!ur.equals(blobURL)&&!updatedBlobUrl.contains(ur))
						uris2.add(u.toString());
				}

				//generate random number to choose the blob
				var random = new Random();
				var uri2 = uris2.toArray(new String[uris2.size()])[random.nextInt(uris2.size())];

				// Extract the URI using regular expressions
				String regex = String.format("(.+?)/%s/(.+)", Blobs.NAME);
				Pattern pattern = Pattern.compile(regex);
				Matcher matcher = pattern.matcher(updatedBlobUrl);
				String uri1 = "";
				if (matcher.matches()){
					uri1 = matcher.group(1);
				}
					
				
				//download copy of short
				var downl = BlobsClients.get(URI.create(uri1)).download(shrt.getShortId());
				byte[] bytes = null;
				if (downl.isOK())
					bytes =downl.value();
				else
					Log.info(downl.error().toString());

				//upload the short to the new blob server
				var res = BlobsClients.get(URI.create(uri2)).upload(shrt.getShortId(), bytes);
				if (!res.isOK())
					Log.info(res.error().toString());

				//update blobUrl
				updatedBlobUrl += "|" + uri2 + "/"+Blobs.NAME+"/"+shrt.getShortId();			

				// Set the updated blobUrl
				shrt.setBlobUrl(updatedBlobUrl);

				// Update the entity in the database
				DB.updateOne(shrt);

				// Invalidate the cache for this shortId
				shortsCache.invalidate(shrt.getShortId());
			}
		});
	}

	//Create a ScheduledExecutorService
	ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

	public void startCacheUpdates(){
        // Schedule a task to refresh the cache every 5 seconds
        executorService.scheduleAtFixedRate(() -> {
			
            try {
                var servers = blobCountCache.get(BLOB_COUNT);
				
				Set<String> uris = new HashSet<>();
				for (ExtendedBlobs blob : BlobsClients.all()) {
					uris.add(blob.toString());
				}	

				for (var server : servers.keySet()) {
					if (!uris.contains(server)) {
						Log.info(() -> format("Removing blob server %s from the list of candidates\n\n", server));
						removeFromBlobUrl(server);
						blobCountCache.invalidate(server);
					}
				}
            } catch (ExecutionException e) {
                e.printStackTrace(); 
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

}
