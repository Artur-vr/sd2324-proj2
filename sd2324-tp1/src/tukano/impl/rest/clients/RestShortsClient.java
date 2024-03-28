package tukano.impl.rest.clients;

import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;

import java.util.List;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import tukano.api.Short;
import tukano.api.java.Result;
import tukano.api.rest.RestShorts;
import tukano.impl.api.java.ExtendedShorts;

public class RestShortsClient extends RestClient implements ExtendedShorts{

	public RestShortsClient(String serverURI) {
		super(serverURI, RestShorts.PATH);
	}

	public Result<Short> _createShort(String userId, String password) {
		Response r = target
				.path(userId)
				.queryParam(RestShorts.PWD, password )
				.request()
				.accept(MediaType.APPLICATION_JSON)
				.post( Entity.json(null));

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok( r.readEntity( Short.class ));
	}

	public Result<Void> _deleteShort(String shortId, String password) {
		Response r = target
				.path(shortId)
				.queryParam(RestShorts.PWD, password )
				.request()
				.delete();

		var status = r.getStatus();
		if( status != Status.NO_CONTENT.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok();
	}

	public Result<Short> _getShort(String shortId) {
		Response r = target
				.path(shortId)
				.request()
				.get();

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok( r.readEntity( Short.class ));
	}

	public Result<List<String>> _getShorts(String userId) {
		Response r = target
				.path(userId)
				.path(RestShorts.SHORTS)
				.request()
				.accept( MediaType.APPLICATION_JSON)
				.get();

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok(r.readEntity( new GenericType<List<String>>() {}));
	}

	public Result<Void> _follow(String userId1, String userId2, boolean isFollowing, String password) {
		Response r = target
				.path(userId1)
				.path(userId2)
				.path(RestShorts.FOLLOWERS)
				.queryParam(RestShorts.PWD, password )
				.request()
				.post( Entity.entity(isFollowing, MediaType.APPLICATION_JSON));

		var status = r.getStatus();
		if( status != Status.NO_CONTENT.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok();
	}

	public Result<List<String>> _followers(String userId, String password) {
		Response r = target
				.path(userId)
				.path(RestShorts.FOLLOWERS)
				.queryParam(RestShorts.PWD, password )
				.request()
				.accept( MediaType.APPLICATION_JSON)
				.get();

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok(r.readEntity( new GenericType<List<String>>() {}));
	}

	public Result<Void> _like(String shortId, String userId, boolean isLiked, String password) {
		Response r = target
				.path(shortId)
				.path(userId)
				.path(RestShorts.LIKES)
				.queryParam(RestShorts.PWD, password )
				.request()
				.post( Entity.entity(isLiked, MediaType.APPLICATION_JSON));

		var status = r.getStatus();
		if( status != Status.NO_CONTENT.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok();
	}

	public Result<List<String>> _likes(String shortId, String password) {
		Response r = target
				.path(shortId)
				.path(RestShorts.LIKES)
				.queryParam(RestShorts.PWD, password )
				.request()
				.accept( MediaType.APPLICATION_JSON)
				.get();

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok(r.readEntity( new GenericType<List<String>>() {}));
	}

	public Result<List<String>> _getFeed(String userId, String password) {
		Response r = target
				.path(userId)
				.path(RestShorts.FEED)
				.queryParam(RestShorts.PWD, password )
				.request()
				.accept( MediaType.APPLICATION_JSON)
				.get();

		var status = r.getStatus();
		if( status != Status.OK.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok(r.readEntity( new GenericType<List<String>>() {}));
	}

	public Result<Void> _deleteAllShorts(String userId, String password) {
		Response r = target
				.path(userId)
				.path(RestShorts.SHORTS)
				.queryParam(RestShorts.PWD, password )
				.request()
				.delete();

		var status = r.getStatus();
		if( status != Status.NO_CONTENT.getStatusCode() )
			return error( getErrorCodeFrom(status));
		else
			return ok();
	}
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		return super.reTry( () -> _createShort(userId, password));
	}

	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		return super.reTry( () -> _deleteShort(shortId, password));
	}

	@Override
	public Result<Short> getShort(String shortId) {
		return super.reTry( () -> _getShort(shortId));
	}

	@Override
	public Result<List<String>> getShorts(String userId) {
		return super.reTry( () -> _getShorts(userId));
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		return super.reTry( () -> _follow(userId1, userId2, isFollowing, password));
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		return super.reTry( () -> _followers(userId, password));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		return super.reTry( () -> _like(shortId, userId, isLiked, password));
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		return super.reTry( () -> _likes(shortId, password));
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		return super.reTry( () -> _getFeed(userId, password));
	}

	@Override
	public Result<Void> deleteAllShorts(String userId, String password) {
		return super.reTry( () -> _deleteAllShorts(userId, password));
	}
}
