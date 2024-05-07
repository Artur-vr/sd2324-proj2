package utils;

import com.google.gson.Gson;
import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;

public class DropBoxUtil {

    private static final String apiKey = "1nmf8dhqqw8r4l3";
    private static final String apiSecret = "2w484hdoyq1ihkj";
    private static final String accessTokenStr = "";

    private static final String LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String LIST_FOLDER_CONTINUE_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String DOWNLOAD_FILE_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String UPLOAD_V2_URL = "	https://content.dropboxapi.com/2/files/upload";
    private static final String DELETE_FILE_URL = "https://api.dropboxapi.com/2/files/delete_v2";
    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";

    private static final int HTTP_SUCCESS = 200;

    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

    static private final Gson json = new Gson();
    static private final OAuth20Service service = new ServiceBuilder(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
    static private OAuth2AccessToken accessToken = null;

    static public String listFolder(String path) {
        return null;
    }

    static public String listFolderContinue(String cursor) {
        return null;
    }

    static public String downloadFile(String path) {
        return null;
    }

    static public String uploadFile(String path, byte[] data) {
        return null;
    }

    static public String deleteFile(String path) {
        return null;
    }

    static public String createFolder(String path) {
        return null;
    }

    static public void setAccessToken(boolean forget) {
        ...
    }

}
