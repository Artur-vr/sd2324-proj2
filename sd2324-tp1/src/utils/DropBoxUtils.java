package utils;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import tukano.api.java.Result;
import tukano.api.java.Result.ErrorCode;

import org.hsqldb.persist.Log;
import org.pac4j.scribe.builder.api.DropboxApi20;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;
import java.util.logging.Level;

import static tukano.api.java.Result.error;
import static tukano.api.java.Result.ok;
import static tukano.api.java.Result.ErrorCode;

public class DropBoxUtils {

    private static DropBoxUtils instance;

    private static final String apiKey = "1nmf8dhqqw8r4l3";
    private static final String apiSecret = "2w484hdoyq1ihkj";
    private static final String accessTokenStr = "";

    private static final String UPLOAD_V2_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String LIST_FOLDER_CONTINUE_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String DOWNLOAD_FILE_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String DELETE_FILE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";

    private static final int HTTP_SUCCESS = 200;
    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

    private final Gson json;
    private final OAuth20Service service;
    private final OAuth2AccessToken accessToken;
    private static Logger Log = Logger.getLogger(DropBoxUtils.class.getName());

    // private static void enableDebugLogging() {
    //     Log.setLevel(Level.ALL);
    //     ConsoleHandler handler = new ConsoleHandler();
    //     handler.setLevel(Level.ALL);
    //     Log.addHandler(handler);
    // }


    private DropBoxUtils() {
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    public static DropBoxUtils getInstance() {
        if ( instance == null )
            instance = new DropBoxUtils();
        //enableDebugLogging(); // Enable debug logging when instance is created
        return instance;
    }

    // Define the UploadFileArgs class
    public static class UploadFileArgs {
        private final String path;
        public UploadFileArgs(String path) {
            this.path = path;
        }
    }

    // Define the CreateFolderV2Args class
    public static class CreateFolderV2Args {
        private final boolean autorename;
        private final String path;

        public CreateFolderV2Args(String path, boolean autorename) {
            this.autorename = autorename;
            this.path = path;
        }
    }

    public static class ListFolderArgs {
        private final String path;
        public ListFolderArgs(String path) {
            this.path = path;
        }
    }

    // Define the ListFolderReturn class
    public static class ListFolderReturn {
        private List<Entry> entries;
        private boolean has_more;
        private String cursor;

        public List<Entry> getEntries() {
            return entries;
        }

        public boolean has_more() {
            return has_more;
        }

        public String getCursor() {
            return cursor;
        }

        public static class Entry {
            private String name;
            private String path;
            private int size;

            @Override
            public String toString() {
                return "Entry{" +
                        "name='" + name + '\'' +
                        ", path='" + path + '\'' +
                        ", size=" + size +
                        '}';
            }
        }
    }

    // Define the ListFolderContinueArgs class
    public static class ListFolderContinueArgs {
        private final String cursor;

        public ListFolderContinueArgs(String cursor) {
            this.cursor = cursor;
        }
    }

    public Result<Void> uploadFromPath(String filePath, String destinationPath) {
        Log.info(String.format("DropBoxUtils: Uploading file from path: %s to: %s", filePath, destinationPath));
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            return uploadBytes(fileBytes, destinationPath);
        } catch (IOException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("STATUSS: %s. Exception occurred while trying to upload file from path: %s\n message: %s", statusCode, filePath, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    public Result<Void> uploadBytes(byte[] fileBytes, String destinationPath) {
        Log.info(String.format("DropBoxUtils: Uploading bytes to: %s", destinationPath));
        try {
            OAuthRequest uploadFile = new OAuthRequest(Verb.POST, UPLOAD_V2_URL);
            uploadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
            uploadFile.addHeader("Dropbox-API-Arg", json.toJson(new UploadFileArgs(destinationPath)));
            uploadFile.setPayload(fileBytes);
    
            service.signRequest(accessToken, uploadFile);
            Response response = service.execute(uploadFile);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to upload file: Status: %d, \nReason: %s\n", response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
            return ok();
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to upload bytes to: %s STATUSS: %s.\n%s", destinationPath, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    
    public Result<Void> createDirectory(String path) {
        Log.info(String.format("DropBoxUtils: Creating directory: %s", path));
        try {
            OAuthRequest createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
            createFolder.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            createFolder.setPayload(json.toJson(new CreateFolderV2Args(path, false)));
    
            service.signRequest(accessToken, createFolder);
            Response response = service.execute(createFolder);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to create directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
            return ok();
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to create directory: %s STATUSS: %s.\n%s", path, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    
    public Result<List<String>> listDirectory(String path) {
        Log.info(String.format("DropBoxUtils: Listing directory: %s", path));
        try {
            List<String> directoryContents = new ArrayList<>();
            OAuthRequest listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_URL);
            listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            listDirectory.setPayload(json.toJson(new ListFolderArgs(path)));
    
            service.signRequest(accessToken, listDirectory);
            Response response = service.execute(listDirectory);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
    
            var responseJson = json.fromJson(response.getBody(), ListFolderReturn.class);
            responseJson.getEntries().forEach(e -> directoryContents.add(e.toString()));
    
            while (responseJson.has_more()) {
                listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_URL);
                listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
                listDirectory.setPayload(json.toJson(new ListFolderContinueArgs(responseJson.getCursor())));
                service.signRequest(accessToken, listDirectory);
                response = service.execute(listDirectory);
    
                if (response.getCode() != HTTP_SUCCESS) {
                    Log.severe(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));
                    return error(getErrorCodeFrom(response.getCode()));
                }
    
                responseJson = json.fromJson(response.getBody(), ListFolderReturn.class);
                responseJson.getEntries().forEach(e -> directoryContents.add(e.toString()));
            }
            return ok(directoryContents);
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to list directory: %s STATUSS: %s.\n%s", path, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    
    public Result<Void> downloadToPath(String localFilePath, String destinationPath) {
        Log.info(String.format("DropBoxUtils: Downloading to path: %s from: %s", localFilePath, destinationPath));
        try {
            OAuthRequest downloadFile = new OAuthRequest(Verb.POST, DOWNLOAD_FILE_URL);
            downloadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
            downloadFile.addHeader("Dropbox-API-Arg", "{\"path\": \"" + destinationPath + "\"}");
    
            service.signRequest(accessToken, downloadFile);
            Response response = service.execute(downloadFile);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to download file: %s, Status: %d, \nReason: %s\n", destinationPath, response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
    
            try (FileOutputStream fileOutputStream = new FileOutputStream(localFilePath)) {
                response.getStream().transferTo(fileOutputStream);
            }
            return ok();
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to download to path: %s STATUSS: %s.\n%s", destinationPath, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    
    public Result<byte[]> downloadBytes(String destinationPath) {
        Log.info(String.format("DropBoxUtils: Downloading bytes from: %s", destinationPath));
        try {
            OAuthRequest downloadFile = new OAuthRequest(Verb.POST, DOWNLOAD_FILE_URL);
            downloadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
            downloadFile.addHeader("Dropbox-API-Arg", "{\"path\": \"" + destinationPath + "\"}");
    
            service.signRequest(accessToken, downloadFile);
            Response response = service.execute(downloadFile);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to download file: %s, Status: %d, \nReason: %s\n", destinationPath, response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
    
            return ok(response.getBody().getBytes());
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to download bytes from: %s STATUSS: %s.\n%s", destinationPath, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }

    public Result<Void> delete(String path) {
        Log.info(String.format("DropBoxUtils: Deleting file: %s", path));
        try {
            OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE_FILE_URL);
            deleteFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            deleteFile.setPayload("{\"path\":\"" + path + "\"}");
    
            service.signRequest(accessToken, deleteFile);
            Response response = service.execute(deleteFile);
    
            if (response.getCode() != HTTP_SUCCESS) {
                Log.severe(String.format("Failed to delete file: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));
                return error(getErrorCodeFrom(response.getCode()));
            }
            return ok();
        } catch (IOException | ExecutionException | InterruptedException e) {
            int statusCode = extractStatusCodeFromException(e);
            Log.severe(String.format("Exception occurred while trying to delete file: %s STATUSS: %s.\n%s", path, statusCode, e.getMessage()));
            return error(getErrorCodeFrom(statusCode));
        }
    }
    
  
    public void cleanState(){
        // Clean the state of the dropbox account
        Log.info("DropboxUtils: Cleaning state of the dropbox account");
        
        Result<Void> result = this.delete("/tmp");

        if (result.isOK() || result.error() == ErrorCode.NOT_FOUND || result.error() == ErrorCode.CONFLICT){
            System.out.println("State cleaned successfully");
        } else {
            Log.severe("Failed to clean the state of the dropbox account. 1");
        }
    
    }

    public static ErrorCode getErrorCodeFrom(int status) {
		return switch (status) {
		case 200, 204 -> ErrorCode.OK;
		case 409 -> ErrorCode.CONFLICT;
		case 403 -> ErrorCode.FORBIDDEN;
		case 404 -> ErrorCode.NOT_FOUND;
		case 400 -> ErrorCode.BAD_REQUEST;
		case 500 -> ErrorCode.INTERNAL_ERROR;
		case 501 -> ErrorCode.NOT_IMPLEMENTED;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}

    private int extractStatusCodeFromException(Exception e) {
        // Default status code for unhandled exceptions
        int statusCode = 500;
        
        // Check if the exception contains the status code
        String message = e.getMessage();
        if (message != null) {
            // Extract status code from message, if present
            String[] parts = message.split("Status: ");
            if (parts.length > 1) {
                try {
                    statusCode = Integer.parseInt(parts[1].split(",")[0].trim());
                } catch (NumberFormatException ex) {
                    Log.severe("Failed to parse status code from exception message");
                }
            }
        }
        
        return statusCode;
    }

}