package utils;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;

import org.hsqldb.persist.Log;
import org.pac4j.scribe.builder.api.DropboxApi20;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    private DropBoxUtils() {
        json = new Gson();
        accessToken = new OAuth2AccessToken(accessTokenStr);
        service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    }

    public static DropBoxUtils getInstance() {
        if ( instance == null )
            instance = new DropBoxUtils();
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

        public CreateFolderV2Args(String path, boolean autorename) {
            this.autorename = autorename;
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

    public void uploadFromPath(String filePath, String destinationPath) throws IOException, ExecutionException, InterruptedException {

        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));

        OAuthRequest uploadFile = new OAuthRequest(Verb.POST, UPLOAD_V2_URL);
        uploadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
        uploadFile.addHeader("Dropbox-API-Arg", json.toJson(new UploadFileArgs(destinationPath)));
        uploadFile.setPayload(fileBytes);

        service.signRequest(accessToken, uploadFile);

        Response response = service.execute(uploadFile);

        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to upload file: %s, Status: %d, \nReason: %s\n", filePath, response.getCode(), response.getBody()));

    }

    public void uploadBytes(byte[] fileBytes, String destinationPath) throws IOException, ExecutionException, InterruptedException {

        OAuthRequest uploadFile = new OAuthRequest(Verb.POST, UPLOAD_V2_URL);
        uploadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
        uploadFile.addHeader("Dropbox-API-Arg", json.toJson(new UploadFileArgs(destinationPath)));
        uploadFile.setPayload(fileBytes);

        service.signRequest(accessToken, uploadFile);

        Response response = service.execute(uploadFile);

        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to upload file: Status: %d, \nReason: %s\n", response.getCode(), response.getBody()));

    }

    public void createDirectory(String path) throws IOException, ExecutionException, InterruptedException {

        OAuthRequest createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
        createFolder.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        createFolder.setPayload(json.toJson(new CreateFolderV2Args(path, false)));

        service.signRequest(accessToken, createFolder);

        Response response = service.execute(createFolder);

        if ( response.getCode() != HTTP_SUCCESS )
            throw new IOException(String.format("Failed to create directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));

    }

    public void listDirectory(String path) throws IOException, ExecutionException, InterruptedException {

        List<String> directoryContents = new ArrayList<>();

        OAuthRequest listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_URL);
        listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        listDirectory.setPayload(json.toJson(new ListFolderArgs(path)));

        service.signRequest(accessToken, listDirectory);

        Response response = service.execute(listDirectory);

        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));

        var responseJson = json.fromJson(response.getBody(), ListFolderReturn.class);
        responseJson.getEntries().forEach( e -> directoryContents.add( e.toString() ) );

        while( responseJson.has_more() ) {
            listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_URL);
            listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);

            listDirectory.setPayload(json.toJson(new ListFolderContinueArgs(responseJson.getCursor())));
            service.signRequest(accessToken, listDirectory);

            response = service.execute(listDirectory);

            if ( response.getCode() != HTTP_SUCCESS )
                throw new RuntimeException(String.format("Failed to list directory: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));

            responseJson = json.fromJson(response.getBody(), ListFolderReturn.class);
            responseJson.getEntries().forEach( e -> directoryContents.add( e.toString() ) );
        }

    }

    public void delete(String path) throws IOException, ExecutionException, InterruptedException {

        OAuthRequest deleteFile = new OAuthRequest(Verb.POST, DELETE_FILE_URL);
        deleteFile.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        deleteFile.setPayload("{\"path\":\"" + path + "\"}");

        service.signRequest(accessToken, deleteFile);

        Response response = service.execute(deleteFile);
        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to delete file: %s, Status: %d, \nReason: %s\n", path, response.getCode(), response.getBody()));

    }

    public void downloadToPath(String localFilePath, String destinationPath) throws IOException, ExecutionException, InterruptedException {

        OAuthRequest downloadFile = new OAuthRequest(Verb.POST, DOWNLOAD_FILE_URL);
        downloadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
        downloadFile.addHeader("Dropbox-API-Arg", "{\"path\": \"" + destinationPath + "\"}");

        service.signRequest(accessToken, downloadFile);

        Response response = service.execute(downloadFile);
        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to download file: %s, Status: %d, \nReason: %s\n", destinationPath, response.getCode(), response.getBody()));

        FileOutputStream fileOutputStream = new FileOutputStream(localFilePath);
        response.getStream().transferTo(fileOutputStream);

    }

    public byte[] downloadBytes(String destinationPath) throws IOException, ExecutionException, InterruptedException {

        OAuthRequest downloadFile = new OAuthRequest(Verb.POST, DOWNLOAD_FILE_URL);
        downloadFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
        downloadFile.addHeader("Dropbox-API-Arg", "{\"path\": \"" + destinationPath + "\"}");

        service.signRequest(accessToken, downloadFile);

        Response response = service.execute(downloadFile);
        if ( response.getCode() != HTTP_SUCCESS )
            throw new RuntimeException(String.format("Failed to download file: %s, Status: %d, \nReason: %s\n", destinationPath, response.getCode(), response.getBody()));

        return response.getBody().getBytes();
        
    }


    public void cleanState(){
        // Clean the state of the dropbox account
        try {
            this.delete("/temp");
        } catch (IOException | ExecutionException | InterruptedException e) {
            System.out.println("Failed to clean the state of the dropbox account");
            e.printStackTrace();
        }
    }

}