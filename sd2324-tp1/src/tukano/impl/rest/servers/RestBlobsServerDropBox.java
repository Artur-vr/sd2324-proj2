package tukano.impl.rest.servers;

import java.util.logging.Logger;

import org.glassfish.jersey.server.ResourceConfig;

import tukano.api.java.Blobs;
import tukano.impl.rest.servers.utils.CustomLoggingFilter;
import tukano.impl.rest.servers.utils.GenericExceptionMapper;
import utils.Args;
import utils.DropBoxUtils;


public class RestBlobsServerDropBox extends AbstractRestServer {
	public static final int PORT = 5555;
	
	private static Logger Log = Logger.getLogger(RestBlobsServerDropBox.class.getName());

	RestBlobsServerDropBox(int port) {
		super( Log, Blobs.NAME, port);
	}
	
	
	@Override
	void registerResources(ResourceConfig config) {
		config.register( RestBlobsResourceDropBox.class ); 
		config.register(new GenericExceptionMapper());
		config.register(new CustomLoggingFilter());
	}
	
	public static void main(String[] args) {
		Args.use(args);
		if(Boolean.parseBoolean(args[0]) == true){
			Log.info("Cleaning previous state");
			DropBoxUtils.getInstance().cleanState();
		}
		
		new RestBlobsServerDropBox(Args.valueOf("-port", PORT)).start();
		
	}
}