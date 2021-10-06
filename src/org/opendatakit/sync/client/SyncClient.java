package org.opendatakit.sync.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.zip.DataFormatException;

import org.apache.commons.fileupload.MultipartStream;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.message.BasicNameValuePair;
import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONException;
import org.apache.wink.json4j.JSONObject;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvReader;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvWriter;
import org.opendatakit.aggregate.odktables.rest.SavepointTypeManipulator;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.Row;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.RowList;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.PrivilegesInfo;
import org.apache.http.entity.mime.*;
import org.apache.http.entity.mime.content.ByteArrayBody;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Class used to communicate with the ODK and Mezuri servers via the REST API.
 * This class uploads data to the server and downloads data from the server.
 * 
 * @author clarlars@gmail.com
 * 
 */
public class SyncClient {
  public static final String TAG = "WinkClient";
  
  public static final String JSON_STR = "json";
  
  public static final String ASSETS_DIR = "assets";
  
  public static final String TABLES_DIR = "tables";
  
  public static final String FILES_STR = "files";
  
  public static final String UTF8_STR = "UTF-8";
  
  public static final String FILENAME_STR = "filename";
  
  public static final String INSTANCES_DIR = "instances";

  public static final String DATA_ETAG_QUERY_PARAM = "data_etag=";

  public static final String FETCH_LIMIT_QUERY_PARAM = "fetchLimit=";

  public static final String CURSOR_QUERY_PARAM = "cursor=";

  public static final String START_TIME_QUERY_PARAM = "startTime=";

  public static final String END_TIME_QUERY_PARAM = "endTime=";

  public static final String MANIFEST_STR = "manifest";
  
  public static final String ROWS_STR = "rows";

  public static final String ROW_ETAG_URI_FRAGMENT = "?row_etag=";

  public static final String AS_ATTACHMENT_URI_FRAGMENT = "?as_attachment=true";

  public static final String SEPARATOR_STR = "/";

  // Maybe better to have a function to map this?
  public static final String ELEM_KEY_JSON = "elementKey";

  public static final String ELEM_NAME_JSON = "elementName";

  public static final String ELEM_TYPE_JSON = "elementType";

  public static final String LIST_CHILD_ELEM_KEYS_JSON = "listChildElementKeys";

  public static final String ELEM_KEY_TABLE_DEF = "_element_key";

  public static final String ELEM_NAME_TABLE_DEF = "_element_name";

  public static final String ELEM_TYPE_TABLE_DEF = "_element_type";

  public static final String LIST_CHILD_ELEM_KEYS_TABLE_DEF = "_list_child_element_keys";

  // Row definitions - mapping here?
  public static final String ID_JSON = "id";

  public static final String FORM_ID_JSON = "formId";

  public static final String LOCALE_JSON = "locale";

  public static final String SAVEPOINT_TYPE_JSON = "savepointType";

  public static final String SAVEPOINT_TIMESTAMP_JSON = "savepointTimestamp";

  public static final String SAVEPOINT_CREATOR_JSON = "savepointCreator";

  public static final String CREATE_USER_JSON = "createUser";

  public static final String LAST_UPDATE_USER = "lastUpdateUser";

  public static final String ROW_ETAG_JSON = "rowETag";

  public static final String DATA_ETAG_AT_MODIFICATION_JSON = "dataETagAtModification";

  public static final String DELETED_JSON = "deleted";

  public static final String ROWS_STR_JSON = "rows";

  public static final String FILTER_SCOPE_JSON = "filterScope";
  
  public static final String DEFAULT_ACCESS_JSON = "defaultAccess";
  
  public static final String ROW_OWNER_JSON = "rowOwner";
  
  public static final String GROUP_MODIFY_JSON = "groupModify";
  
  public static final String GROUP_PRIVILEGED_JSON = "groupPrivileged";
  
  public static final String GROUP_READ_ONLY_JSON = "groupReadOnly";

  public static final String TABLE_ID_JSON = "tableId";

  public static final String TABLES_JSON = "tables";

  public static final String SCHEMA_ETAG_JSON = "schemaETag";

  public static final String DATA_ETAG_JSON = "dataETag";

  public static final String WEB_SAFE_RESUME_CURSOR_JSON = "webSafeResumeCursor";

  public static final String HAS_MORE_RESULTS_JSON = "hasMoreResults";

  public static final String ID_ROW_DEF = "_id";

  public static final String FORM_ID_ROW_DEF = "_form_id";

  public static final String LOCALE_ROW_DEF = "_locale";

  public static final String SAVEPOINT_TYPE_ROW_DEF = "_savepoint_type";

  public static final String SAVEPOINT_TIMESTAMP_ROW_DEF = "_savepoint_timestamp";

  public static final String SAVEPOINT_CREATOR_ROW_DEF = "_savepoint_creator";

  public static final String CREATE_USER_ROW_DEF = "_create_user";

  public static final String LAST_UPDATE_USER_ROW_DEF = "_last_update_user";

  public static final String ROW_ETAG_ROW_DEF = "_row_etag";

  public static final String DEFAULT_ACCESS_ROW_DEF = "_default_access";

  public static final String ROW_OWNER_ROW_DEF = "_row_owner";
  
  public static final String GROUP_READ_ONLY_ROW_DEF = "_group_read_only";

  public static final String GROUP_MODIFY_ROW_DEF = "_group_modify";
  
  public static final String GROUP_PRIVILEGED_ROW_DEF = "_group_privileged";

  public static final String DATA_ETAG_AT_MODIFICATION_ROW_DEF = "_data_etag_at_modification";

  public static final String DELETED_ROW_DEF = "_deleted";

  public static final String ORDERED_COLUMNS_DEF = "orderedColumns";

  public static final String DFEAULT_FETCH_LIMIT = "1000";

  public static final String BOUNDARY = "boundary";

  public static final String MULTIPART_FILE_HEADER = "filename=\"";

  public static final int MAX_BATCH_SIZE = 10485760;

  public static final String TYPE_STR = "type";
  
  public static final String USERS_STR = "usersInfo";
  
  public static final String PRIVILEGES_INFO_STR = "privilegesInfo";
  
  public static final String LIST_STR = "list";
  
  public static final String SSL_STR = "ssl";
  
  public static final String RESET_USERS_AND_PERMISSIONS = "reset-users-and-permissions";
  
  protected static final String [] metadataColumns1 = {ID_ROW_DEF, FORM_ID_ROW_DEF, LOCALE_ROW_DEF, 
    SAVEPOINT_TYPE_ROW_DEF, SAVEPOINT_TIMESTAMP_ROW_DEF, SAVEPOINT_CREATOR_ROW_DEF};
  protected static final String[] metadataColumns2 = {DEFAULT_ACCESS_ROW_DEF, GROUP_MODIFY_ROW_DEF, 
    GROUP_PRIVILEGED_ROW_DEF, GROUP_READ_ONLY_ROW_DEF, ROW_ETAG_ROW_DEF, ROW_OWNER_ROW_DEF};

  protected static final int DEFAULT_BOUNDARY_BUFSIZE = 4096;

  private CloseableHttpClient httpClient = null;

  private HttpClientContext localContext = null;

  private CredentialsProvider credsProvider = null;

  static Map<String, String> mimeMapping;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("jpeg", "image/jpeg");
    m.put("jpg", "image/jpeg");
    m.put("png", "image/png");
    m.put("gif", "image/gif");
    m.put("pbm", "image/x-portable-bitmap");
    m.put("ico", "image/x-icon");
    m.put("bmp", "image/bmp");
    m.put("tiff", "image/tiff");

    m.put("mp2", "audio/mpeg");
    m.put("mp3", "audio/mpeg");
    m.put("wav", "audio/x-wav");

    m.put("asf", "video/x-ms-asf");
    m.put("avi", "video/x-msvideo");
    m.put("mov", "video/quicktime");
    m.put("mpa", "video/mpeg");
    m.put("mpeg", "video/mpeg");
    m.put("mpg", "video/mpeg");
    m.put("mp4", "video/mp4");
    m.put("qt", "video/quicktime");

    m.put("css", "text/css");
    m.put("htm", "text/html");
    m.put("html", "text/html");
    m.put("csv", "text/csv");
    m.put("txt", "text/plain");
    m.put("log", "text/plain");
    m.put("rtf", "application/rtf");
    m.put("pdf", "application/pdf");
    m.put("zip", "application/zip");
    m.put("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    m.put("docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    m.put("pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation");
    m.put("xml", "application/xml"); // does not assume UTF-8
    m.put("js", "application/x-javascript");
    m.put("json", "application/json"); // assumes UTF-8
    mimeMapping = m;
  }
  
  private String determineContentType(String fileName) {
    int ext = fileName.lastIndexOf('.');
    if (ext == -1) {
      return "application/octet-stream";
    }
    String type = fileName.substring(ext + 1);
    String mimeType = mimeMapping.get(type);
    if (mimeType == null) {
      return "application/octet-stream";
    }
    return mimeType;
  }
  
  /**
   * Init client with default parameters
   * 
   */
  public void init() {
    httpClient = HttpClientBuilder
        .create()
        .setRedirectStrategy(new LaxRedirectStrategy())
        .build();
  
  }

  /**
   * Init client with parameters for digest auth
   * 
   * @param host
   *          the host name to authenticate against
   * @param userName
   *          the user name to use for authentication
   * @param password
   *          the password to use for authentication
   */
  public void init(String host, String userName, String password) {
    int CONNECTION_TIMEOUT = 60000;

    // Context
    // context holds authentication state machine, so it cannot be
    // shared across independent activities.
    localContext = HttpClientContext.create();
    
    credsProvider = new BasicCredentialsProvider();

    AuthScope aDigest = new AuthScope(host, -1, null, AuthSchemes.DIGEST);
    AuthScope a = new AuthScope(host, -1, null, AuthSchemes.BASIC);
    Credentials c = new UsernamePasswordCredentials(userName, password);
    credsProvider.setCredentials(aDigest, c);
    credsProvider.setCredentials(a, c);

    localContext.setCredentialsProvider(credsProvider);

    SocketConfig socketConfig = SocketConfig.copy(SocketConfig.DEFAULT)
        .setSoTimeout(2 * CONNECTION_TIMEOUT).build();

    // if possible, bias toward digest auth (may not be in 4.0 beta 2)
    List<String> targetPreferredAuthSchemes = new ArrayList<String>();
    targetPreferredAuthSchemes.add(AuthSchemes.DIGEST);
    targetPreferredAuthSchemes.add(AuthSchemes.BASIC);

    RequestConfig requestConfig = RequestConfig
        .copy(RequestConfig.DEFAULT)
        .setConnectTimeout(CONNECTION_TIMEOUT)
        // support authenticating
        .setAuthenticationEnabled(true)
        // support redirecting to handle http: => https: transition
        .setRedirectsEnabled(true)
        // max redirects is set to 4
        .setMaxRedirects(4).setCircularRedirectsAllowed(true)
        .setTargetPreferredAuthSchemes(targetPreferredAuthSchemes)
     	.setProxyPreferredAuthSchemes(targetPreferredAuthSchemes)
        .build();

    httpClient = HttpClientBuilder
        .create()
        .setDefaultSocketConfig(socketConfig)
        .setDefaultRequestConfig(requestConfig)
        .setRedirectStrategy(new LaxRedirectStrategy())
        .build();
  }

  public void initAnonymous(String host) {
	    int CONNECTION_TIMEOUT = 60000;

	    // Context
	    // context holds authentication state machine, so it cannot be
	    // shared across independent activities.
	    localContext = HttpClientContext.create();

	    SocketConfig socketConfig = SocketConfig.copy(SocketConfig.DEFAULT)
	        .setSoTimeout(2 * CONNECTION_TIMEOUT).build();

	    RequestConfig requestConfig = RequestConfig
	        .copy(RequestConfig.DEFAULT)
	        .setConnectTimeout(CONNECTION_TIMEOUT)
	        // support authenticating
	        .setAuthenticationEnabled(false)
	        // support redirecting to handle http: => https: transition
	        .setRedirectsEnabled(true)
	        // max redirects is set to 4
	        .setMaxRedirects(4).setCircularRedirectsAllowed(true)
	        .build();

	    httpClient = HttpClientBuilder.create().setDefaultSocketConfig(socketConfig)
	        .setDefaultRequestConfig(requestConfig).build();
	    

  }
  
  	public boolean isAuthenticationEnabled() {
  	    if (localContext == null) {
  	      throw new IllegalStateException("The initialization function must be called");
  	    }

  		return localContext.getRequestConfig().isAuthenticationEnabled();
	  }
  
  /**
   * Init client parameters for authentication
   * 
   * @param host
   *          the host to authenticate against
   * @param userName
   *          the user name to use for auth
   * @param password
   *          the password to use for auth
   * @param socketConfig
   *          the socket config parameters to use
   * @param reqConfig
   *          the request config parameters to use
   * @param basicProvider
   *          the credentials provider to use
   */
//  public void init(String host, String userName, String password, SocketConfig socketConfig,
//      RequestConfig reqConfig, BasicCookieStore basicStore, BasicCredentialsProvider basicProvider) {
//
//    localContext = new BasicHttpContext();
//
//    if (basicStore != null) {
//      cookieStore = basicStore;
//      localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
//    }
//
//    if (basicProvider != null) {
//      credsProvider = basicProvider;
//    }
//
//    if (credsProvider != null && userName != null && password != null && host != null) {
//      AuthScope a = new AuthScope(host, -1, null, AuthSchemes.DIGEST);
//      Credentials c = new UsernamePasswordCredentials(userName, password);
//      credsProvider.setCredentials(a, c);
//    }
//
//    if (credsProvider != null) {
//      localContext.setAttribute(HttpClientContext.CREDS_PROVIDER, credsProvider);
//    }
//
//    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
//
//    if (socketConfig != null) {
//      clientBuilder.setDefaultSocketConfig(socketConfig);
//    }
//
//    if (reqConfig != null) {
//      clientBuilder.setDefaultRequestConfig(reqConfig);
//    }
//
//    httpClient = clientBuilder.build();
//
//  }
  
  /**
   * Returns a list of users currently on the server in a JSONObject
   * 
   * @param agg_url
   *          the url for the server
   *          
   * @param appId
   *          the app_id for the server
   *          
   * @return a ArrayList of Map of users
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public ArrayList<Map<String,Object>> getUsers(String agg_url, String appId) throws ClientProtocolException,
      IOException, JSONException {
    ArrayList<Map<String,Object>> rolesList = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;

    try {

      String agg_uri = UriUtils.getUsersListUri(agg_url, appId);

      System.out.println("getUsers: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      String res = convertResponseToString(response);
      
      if (response.getStatusLine().getStatusCode() < 200
          || response.getStatusLine().getStatusCode() >= 300) {
        System.out.println("getUsers: return null status code: " + response.getStatusLine().getStatusCode());
        System.out.println("getUsers: response is " + res);
        return null;
      }

      TypeReference<ArrayList<Map<String,Object>>> ref = new TypeReference<ArrayList<Map<String,Object>>>() { };
      
      ObjectMapper mapper = new ObjectMapper();

      rolesList = mapper.readValue(res, ref);

      System.out.println("getUsers: result is " + rolesList.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return rolesList;
  }
  
  /**
   * Returns privilegesInfo for the user
   * 
   * @param agg_url
   *          the url for the server
   * 
   * @param appId
   *          the appId for the server
   * 
   * @return a ArrayList of Map of users
   * 
   * @throws ClientProtocolException
   *           due to http errors
   * @throws IOException
   *           due to file errors
   * @throws JSONException
   *           due to JSON errors
   */
  public PrivilegesInfo getPrivilegesInfo(String agg_url, String appId)
      throws ClientProtocolException, IOException, JSONException {
    PrivilegesInfo privilegesInfo = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;

    try {
      String agg_uri = UriUtils.getPrivilegesInfoUri(agg_url, appId);

      System.out.println("getPrivilegesInfo: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      String res = convertResponseToString(response);

      if (response.getStatusLine().getStatusCode() < 200
          || response.getStatusLine().getStatusCode() >= 300) {
        System.out.println("getPrivilegesInfo: return null status code: "
            + response.getStatusLine().getStatusCode());
        System.out.println("getPrivilegesInfo: response is " + res);
        return null;
      }

      ObjectMapper mapper = new ObjectMapper();

      privilegesInfo = mapper.readValue(res, PrivilegesInfo.class);

      System.out.println("getPrivilegesInfo: result is " + privilegesInfo.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return privilegesInfo;
  }

  /**
   * Gets the schemaETag for the table
   * 
   * @param agg_url
   *          the url for the server
   * @param appId
   *          the application id
   * @param tableId
   *          the table id for the table in question
   * @return String to return schemaETag value
   */
  public String getSchemaETagForTable(String agg_url, String appId, String tableId) {

    try {
      JSONObject obj = getTables(agg_url, appId);

      JSONArray tables = obj.getJSONArray(TABLES_JSON);

      for (int i = 0; i < tables.size(); i++) {
        JSONObject table = tables.getJSONObject(i);
        if (tableId.equals(table.getString(TABLE_ID_JSON))) {
          return table.getString(SCHEMA_ETAG_JSON);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }
  

  /**
   * Gets all data from a uri on the server and saves the data to the specified
   * directory. First, app level and table level files are retrieved and saved
   * to the directory. Then the definition.csv and data.csv files for all of the
   * tables are saved. Finally, any row level attachments are saved.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          the application id
   * @param dirToSaveDataTo
   *          the directory in which the data will be saved
   * @param version
   *          ODK version code, 1 or 2
   *          
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void getAllDataFromUri(String uri, String appId, String dirToSaveDataTo, String version)
      throws ClientProtocolException, FileNotFoundException, IOException, JSONException {

    // Get all App Level Files
    getAllAppLevelFilesFromUri(uri, appId, dirToSaveDataTo, version);

    // Get all the Tables
    JSONObject tableRes = getTables(uri, appId);
    JSONArray tables = tableRes.getJSONArray(TABLES_JSON);

    for (int i = 0; i < tables.size(); i++) {
      JSONObject table = tables.getJSONObject(i);
      String tableId = table.getString(TABLE_ID_JSON);
      String schemaETag = table.getString(SCHEMA_ETAG_JSON);

      // Get all Table Level Files
      getAllTableLevelFilesFromUri(uri, appId, tableId, dirToSaveDataTo, version);

      // Write out Table Definition CSV's
      String tableDefCSVPath = FileUtils.getTableDefinitionFilePath(dirToSaveDataTo, tableId);
      writeTableDefinitionToCSV(uri, appId, tableId, schemaETag, tableDefCSVPath);

      // Write out the Table Data CSV's
      String dataCSVPath = FileUtils.getTableDataCSVFilePath(dirToSaveDataTo, tableId);
      writeRowDataToCSV(uri, appId, tableId, schemaETag, dataCSVPath);

      // Get all Instance Files
      // get all rows - check for attachment
      getAllTableInstanceFilesFromUri(uri, appId, tableId, schemaETag, dirToSaveDataTo);
    }
  }

  /**
   * Descends into the directory tree and find all of the files
   * 
   * @param dir
   *          a file object defining the top level directory
   * @return an ArrayList of strings for all of the file paths
   **/
  public ArrayList<String> recurseDir(File dir) {
    ArrayList<String> filePaths = new ArrayList<String>();
    File listFile[] = dir.listFiles();
    if (listFile != null) {
      for (int i = 0; i < listFile.length; i++) {
        if (listFile[i].isDirectory()) {
          ArrayList<String> interimFilePaths = recurseDir(listFile[i]);
          filePaths.addAll(interimFilePaths);
        } else {
          filePaths.add(listFile[i].getPath());
        }
      }
    }
    return filePaths;
  }

  /**
   * Finds the top level subdirectories within the specified directory
   * 
   * @param dir
   *          file object of the top directory
   * @return an ArrayList of strings for all of the top leve directories
   **/
  public ArrayList<String> findTopLevelSubdirectories(File dir) {
    ArrayList<String> subdirectories = new ArrayList<String>();
    File listFile[] = dir.listFiles();
    if (listFile != null) {
      for (int i = 0; i < listFile.length; i++) {
        if (listFile[i].isDirectory()) {
          subdirectories.add(listFile[i].getName());
        }
      }
    }
    return subdirectories;
  }

  /**
   * Pushes all data from a directory to the server.
   * <p>
   * App level files must be in a subdirectory named "assets".
   * <p>
   * Table definitions must be in a subdirectory named "tables/{tableName}. For
   * example, a definition.csv file for a table named test must be in folder
   * tables/test.
   * <p>
   * Row attachments must be in a folder named
   * tables/{tableName}/instances/{rowId}. For example, an image.jpg file for
   * rowId 1f9e6f19_c50a_4436_9328_1d70cdb73493 of test table would be in
   * tables/test/instances/1f9e6f19_c50a_4436_9328_1d70cdb73493. If the rowId
   * contains anything other than alphanumeric characters, the directory name
   * for the rowId will be converted to replace the non-alphanumeric characters
   * to underscores. For example, row id 1f9e6f19-c50a-4436-9328-1d70cdb73493
   * will be converted to 1f9e6f19_c50a_4436_9328_1d70cdb73493.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          the application id
   * @param dirToGetDataFrom
   *          the directory that has the data to push to the server
   * @param version
   *          ODK version code, 1 or 2
   * 
   * @throws DataFormatException due to CSV file having improper format
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void pushAllDataToUri(String uri, String appId, String dirToGetDataFrom, String version)
      throws ClientProtocolException, DataFormatException, FileNotFoundException, IOException,
      JSONException {
    ArrayList<String> assetsFiles;
    ArrayList<String> tableFiles;
    ArrayList<String> tableIds;
    ArrayList<String> instanceFiles = new ArrayList<String>();

    // Get all files in the assets directory
    String assetsDir = FileUtils.getAssetsDirPath(dirToGetDataFrom);
    assetsFiles = recurseDir(new File(assetsDir));

    // Get all the tableIds in the tables directory
    String tablesDir = FileUtils.getTablesDirPath(dirToGetDataFrom);
    tableIds = findTopLevelSubdirectories(new File(tablesDir));

    for (int i = 0; i < tableIds.size(); i++) {
      // Create a table definition
      JSONObject tableResult = null;
      String schemaETag = null;
      String tableId = tableIds.get(i);
      
      String tableDefPath = FileUtils.getTableDefinitionFilePath(dirToGetDataFrom, tableId);
      File tableDefCSV = new File(tableDefPath);
      if (tableDefCSV.exists()) {
        tableResult = createTableWithCSV(uri, appId, tableId, "", tableDefPath);
        if (!tableResult.isNull(SCHEMA_ETAG_JSON)) {
          schemaETag = tableResult.getString(SCHEMA_ETAG_JSON);
        }
      }

      // Create table rows
      String dataRowPath = FileUtils.getTableDataCSVFilePath(dirToGetDataFrom, tableId);
      File dataRowCSV = new File(dataRowPath);
      if (dataRowCSV.exists()) {
        int fLimit = Integer.parseInt(DFEAULT_FETCH_LIMIT);
        createRowsUsingCSVBulkUpload(uri, appId, tableId, schemaETag, dataRowPath, fLimit);
      }

      LinkedHashMap<String, String> mapRowIdToInstanceDir = new LinkedHashMap<String, String>();

      JSONObject obj = null;
      String resumeCursor = null;
      do {
        // Get all of the rowId's for the table
        obj = getRows(uri, appId, tableId, schemaETag, resumeCursor, DFEAULT_FETCH_LIMIT);
        JSONArray rows = obj.getJSONArray(ROWS_STR_JSON);
        for (int k = 0; k < rows.size(); k++) {
          JSONObject row = rows.getJSONObject(k);
          String rowId = row.getString(ID_JSON);
          mapRowIdToInstanceDir.put(convertRowIdForInstances(rowId), rowId);
        }

        resumeCursor = obj.optString(WEB_SAFE_RESUME_CURSOR_JSON);
      } while (obj.getBoolean(HAS_MORE_RESULTS_JSON));

      // Find table Id Files and push up
      String tableIdPath = FileUtils.getTableIdDirPath(dirToGetDataFrom, tableId);
      tableFiles = recurseDir(new File(tableIdPath));

      String tableInstancesPath = FileUtils.getTableInstancesDirPath(dirToGetDataFrom, tableId);
      for (int j = 0; j < tableFiles.size(); j++) {
        if (tableFiles.get(j).startsWith(tableInstancesPath)) {
          instanceFiles.add(tableFiles.get(j));
        } else {
          String filePath = tableFiles.get(j);
          String relativePathOnServer = filePath.substring(dirToGetDataFrom.length() + 1);
          uploadFile(uri, appId, filePath, relativePathOnServer, version);
        }
      }

      // Find instance Files and push up
      for (int j = 0; j < instanceFiles.size(); j++) {
        String filePath = instanceFiles.get(j);
        File instanceFile = new File(filePath);
        String parentPath = instanceFile.getParent();
        String instanceRowId = parentPath.substring(parentPath.lastIndexOf(File.separator) + 1);
        String rowIdToUse = mapRowIdToInstanceDir.get(instanceRowId);
        // Relative path is just file name here
        putFileForRow(uri, appId, tableId, schemaETag, rowIdToUse, filePath, instanceFile.getName());
      }
    }

    for (int i = 0; i < assetsFiles.size(); i++) {
      String filePath = assetsFiles.get(i);
      String relativePathOnServer = filePath.substring(dirToGetDataFrom.length() + 1);
      uploadFile(uri, appId, filePath, relativePathOnServer, version);
    }
  }

  /**
   * Gets all of the instance files (attachments) for a table and saves them to
   * the specified directory
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param dirToSaveDataTo
   *          the directory in which the data will be saved
   *          
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void getAllTableInstanceFilesFromUri(String uri, String appId, String tableId,
      String schemaETag, String dirToSaveDataTo) throws IOException, JSONException {

    // Get all rows for table
    JSONObject row = null;
    String resumeCursor = null;
    do {
      row = getRows(uri, appId, tableId, schemaETag, resumeCursor, DFEAULT_FETCH_LIMIT);
      
      JSONArray tableRows = row.getJSONArray(ROWS_STR_JSON);

      for (int i = 0; i < tableRows.size(); i++) {
        JSONObject tableRow = tableRows.getJSONObject(i);
        String rowId = tableRow.getString(ID_JSON);
        JSONObject files = null;
        try {
          files = getManifestForRow(uri, appId, tableId, schemaETag, rowId);
        } catch (Exception e) {
          e.printStackTrace();
          if (files == null) {
            System.out.println("getAllTableInstanceFilesFromUri: known issue with table:" + tableId
                + " row: " + rowId + " causes exception");
            continue;
          }
        }
        JSONArray rowFiles = files.getJSONArray(FILES_STR);

        for (int j = 0; j < rowFiles.size(); j++) {
          JSONObject rowFile = rowFiles.getJSONObject(j);
          String fileName = rowFile.getString(FILENAME_STR);

          // Convert rowId for Aggregate
          String rowIdForSave = convertRowIdForInstances(rowId);
          String pathToSaveFile = FileUtils.getRowInstanceFilePath(dirToSaveDataTo, tableId, rowIdForSave, fileName);
          // Should relativeDir be set here
          getFileForRow(uri, appId, tableId, schemaETag, rowId, false, pathToSaveFile, fileName);
        }
      }
      resumeCursor = row.optString(WEB_SAFE_RESUME_CURSOR_JSON);
    } while (row.getBoolean(HAS_MORE_RESULTS_JSON));
  }

  /**
   * Gets all of the table level files for a single table and saves them to the
   * specified directory
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param dirToSaveDataTo
   *          the directory in which the data will be saved
   * @param version
   *          ODK version code, 1 or 2
   * 
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void getAllTableLevelFilesFromUri(String uri, String appId, String tableId,
      String dirToSaveDataTo, String version) throws ClientProtocolException,
      FileNotFoundException, IOException, JSONException {
    String relativeDir;
    JSONObject file;

    // Get all table level files
    JSONObject table = getManifestForTableId(uri, appId, tableId, version);
    JSONArray tableLevelFiles = table.getJSONArray(FILES_STR);

    for (int i = 0; i < tableLevelFiles.size(); i++) {
      file = tableLevelFiles.getJSONObject(i);
      relativeDir = file.getString(FILENAME_STR);
      String pathToSaveFile = FileUtils.getRelativePath(dirToSaveDataTo, relativeDir);
      downloadFile(uri, appId, pathToSaveFile, relativeDir, version);
    }
  }

  /**
   * Gets all of the app level files and saves them to the specified directory
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param dirToSaveDataTo
   *          the directory in which the data will be saved
   * @param version
   *          ODK version code, 1 or 2
   *          
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void getAllAppLevelFilesFromUri(String uri, String appId, String dirToSaveDataTo,
      String version) throws ClientProtocolException, FileNotFoundException, IOException,
      JSONException {
    String relativeDir;
    JSONObject file;

    // Get all App Level Files
    JSONObject app = getManifestForAppLevelFiles(uri, appId, version);
    JSONArray appLevelFiles = app.getJSONArray(FILES_STR);

    for (int i = 0; i < appLevelFiles.size(); i++) {
      file = appLevelFiles.getJSONObject(i);
      relativeDir = file.getString(FILENAME_STR);
      String pathToSaveFile = FileUtils.getRelativePath(dirToSaveDataTo, relativeDir);
      downloadFile(uri, appId, pathToSaveFile, relativeDir, version);
    }
  }

  /**
   * Gets the manifest for the app level files and returns that list in a
   * JSONObject
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param version
   *          ODK version code, 1 or 2
   * @return JSONObject of the list of app level files
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getManifestForAppLevelFiles(String uri, String appId, String version)
      throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
      String agg_uri = UriUtils.getAppLevelManifestUri(uri, appId, version);
      System.out.println("getManifestForAppLevelFiles: agg_uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getManifestForAppLevelFiles: result is " + obj.toString());

    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  }

  private HttpResponse httpRequestExecute(HttpRequestBase request, String contentType, boolean excludeAcceptParams) throws IOException,
      ClientProtocolException {
    if (request == null) {
      throw new IllegalArgumentException("httpRequestExecute: request cannnot be null");
    }
    
    if (contentType == null || contentType.length() == 0) {
      throw new IllegalArgumentException("httpRequestExecute: contentType cannot be null");
    }
    request.addHeader("content-type", contentType + "; charset=utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");
    
    if (excludeAcceptParams == false) {
      request.addHeader("accept", contentType);
      request.addHeader("accept-charset", "utf-8");
    }

    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    return response;
  }

  /**
   * Uploads a file to the server. App level or table level files can be
   * uploaded via this method. The only difference between the two is the
   * relative path on the server.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param wholePathToFile
   *          the file path for the file to upload
   * @param relativePathOnServer
   *          the relative path on the server where the file will be stored
   * @param version
   *          ODK version code, 1 or 2
   * @return Http response status code
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   */
  public int uploadFile(String uri, String appId, String wholePathToFile,
      String relativePathOnServer, String version) throws ClientProtocolException, IOException {

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("uploadFile: uri cannot be null");
    }

    if (wholePathToFile == null || wholePathToFile.isEmpty()) {
      throw new IllegalArgumentException("uploadFile: wholePathToFile cannot be null");
    }

    if (relativePathOnServer == null || relativePathOnServer.isEmpty()) {
      throw new IllegalArgumentException("uploadFile: relativePathOnServer cannot be null");
    }

    HttpPost request = null;
    try {
      String uriRelativePath = relativePathOnServer.replaceAll(File.separator + File.separator,
          SEPARATOR_STR);
      String agg_uri = UriUtils.getFilesWithRelPathUri(uri, appId, version, uriRelativePath);
      System.out.println("uploadFile: agg uri is " + agg_uri);

      File file = new File(wholePathToFile);
      if (!file.exists()) {
        System.out.println("uploadFile: file " + wholePathToFile + " does not exist");
        return -1;
      }

      byte[] data = Files.readAllBytes(file.toPath());

      request = new HttpPost(agg_uri);

      // issue the request
      String contentType = determineContentType(file.getName());

      HttpEntity entity = new ByteArrayEntity(data);
      request.setEntity(entity);
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, contentType, false);

      System.out.println("uploadFile: response for file " + wholePathToFile + " is ");

      BufferedReader responseBuff = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName(UTF8_STR)));
      String line;
      while ((line = responseBuff.readLine()) != null)
        System.out.println(line);
      // in.close();

      return response.getStatusLine().getStatusCode();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

  }

  /**
   * Downloads a file to the server. App level or table level files can be
   * downloaded via this method. The only difference between the two is the
   * relative path on the server.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param pathToSaveFile
   *          the file path where the file should be downloaded
   * @param relativePathOnServer
   *          the relative path on the server where the file is stored
   * @param version
   *          ODK version code, 1 or 2
   * 
   * @return Http response status code
   * 
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   */
  public int downloadFile(String uri, String appId, String pathToSaveFile,
      String relativePathOnServer, String version) throws ClientProtocolException,
      FileNotFoundException, IOException {

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("downloadFile: uri cannot be null");
    }

    if (pathToSaveFile == null || pathToSaveFile.isEmpty()) {
      throw new IllegalArgumentException("downloadFile: wholePathToFile cannot be null");
    }

    if (relativePathOnServer == null || relativePathOnServer.isEmpty()) {
      throw new IllegalArgumentException("downloadFile: relativePathOnServer cannot be null");
    }

    HttpGet request = null;
    try {
      String agg_uri = UriUtils.getFilesWithRelPathUri(uri, appId, version, relativePathOnServer);
      System.out.println("downloadFile: agg_uri is " + agg_uri);

      // File to save
      File file = new File(pathToSaveFile);

      request = new HttpGet(agg_uri);

      String accept = determineContentType(file.getName());
      
      HttpResponse response = null;
      response = httpRequestExecute(request, accept, false);
      
      System.out.println("downloadFile: issued get request for " + relativePathOnServer);

      file.getParentFile().mkdirs();
      if (!file.exists()) {
        file.createNewFile();
      }

      InputStream fis = response.getEntity().getContent();

      FileOutputStream fos = new FileOutputStream(file.getAbsoluteFile());
      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }

      fos.close();
      fis.close();

      return response.getStatusLine().getStatusCode();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Deletes an app level file or a table level file off the server
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param relativePathOnServer
   *          the relative path on the server where the file is stored
   * @param version
   *          ODK version code, 1 or 2
   *          
   * @return HTTP response status code
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   */
  public int deleteFile(String uri, String appId, String relativePathOnServer, String version)
      throws ClientProtocolException, IOException {
    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("deleteFile: uri cannot be null");
    }

    if (relativePathOnServer == null || relativePathOnServer.isEmpty()) {
      throw new IllegalArgumentException("deleteFile: relativePathOnServer cannot be null");
    }

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpDelete request = null;
    try {
      String agg_uri = UriUtils.getFilesWithRelPathUri(uri, appId, version, relativePathOnServer);
      System.out.println("deleteFile: agg_uri is " + agg_uri);

      request = new HttpDelete(agg_uri);

      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      System.out.println("deleteFile: client response is "
          + response.getStatusLine().getStatusCode() + ":"
          + response.getStatusLine().getReasonPhrase());

      return response.getStatusLine().getStatusCode();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Returns a list of tables currently on the server in a JSONObject
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @return a JSONObject with the list of tables
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getTables(String uri, String appId) throws ClientProtocolException,
      IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTablesUri(uri, appId);

      System.out.println("getTables: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);

      System.out.println("getTables: result is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  private JSONObject convertResponseToJSONObject(HttpResponse response) throws IOException,
      JSONException {
    JSONObject obj;
    String res = convertResponseToString(response);
    obj = new JSONObject(res);
    return obj;
  }

  private String convertResponseToString(HttpResponse response) throws IOException {
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName(UTF8_STR)));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();
    return res;
  }

  /**
   * Returns table data for the specified tableId in a JSONObject
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @return a JSONObject with the representation of the table
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getTable(String uri, String appId, String tableId)
      throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTableIdUri(uri, appId, tableId);
      System.out.println("getTable: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);
      
      int statusCode = response.getStatusLine().getStatusCode();
      
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
        return null;
      }

      obj = convertResponseToJSONObject(response);

      System.out.println("getTable: result is for tableId " + tableId + " is " + obj.toString());

    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  };

  /**
   * Returns table data eTag for the specified tableId in a JSONObject
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @return a JSONObject with the representation of the table
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   * 
   */
  public String getTableDataETag(String uri, String appId, String tableId)
      throws ClientProtocolException, IOException, JSONException {
    String dataETag = null;

    JSONObject tableRes = getTable(uri, appId, tableId);
    dataETag = (tableRes.has(DATA_ETAG_JSON) && !tableRes.isNull(DATA_ETAG_JSON)) ? tableRes
        .getString(DATA_ETAG_JSON) : null;

    System.out.println("getTableDataETag: result is for tableId " + tableId + " is " + dataETag);

    return dataETag;
  }

  /**
   * Creates a table on the server with the specified tableId and columns.
   * schemaETag should be null if a new table is being created.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param columns
   *          an ArrayList of Column objects which define the columns of the
   *          table
   * @return a JSONObject with the representation of the newly created table
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject createTable(String uri, String appId, String tableId, String schemaETag,
      ArrayList<Column> columns) throws ClientProtocolException, IOException, JSONException {
    JSONObject tableObj = new JSONObject();
    JSONArray cols = new JSONArray();
    JSONObject col;
    JSONObject result = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("createTable: uri cannot be null");
    }

    if (tableId == null || tableId.isEmpty()) {
      throw new IllegalArgumentException("createTable: tableId cannot be null");
    }

    if (columns == null || columns.isEmpty()) {
      throw new IllegalArgumentException("createTable: columns cannot be null");
    }

    HttpPut request = null;
    try {

      String agg_uri = UriUtils.getTableIdUri(uri, appId, tableId);
      System.out.println("createTable: agg_uri is " + agg_uri);

      // Add the columns to the table object
      for (int i = 0; i < columns.size(); i++) {
        col = new JSONObject();
        col.put(ELEM_KEY_JSON, columns.get(i).getElementKey());
        col.put(ELEM_NAME_JSON, columns.get(i).getElementName());
        col.put(ELEM_TYPE_JSON, columns.get(i).getElementType());
        col.put(LIST_CHILD_ELEM_KEYS_JSON, columns.get(i).getListChildElementKeys());
        cols.add(col);
      }

      tableObj.put(SCHEMA_ETAG_JSON, schemaETag);
      tableObj.put(TABLE_ID_JSON, tableId);
      tableObj.put(ORDERED_COLUMNS_DEF, cols);

      System.out.println("createTable: with object " + tableObj.toString());

      request = new HttpPut(agg_uri);
      StringEntity params = new StringEntity(tableObj.toString(), UTF8_STR);
      request.setEntity(params);
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      String res = convertResponseToString(response);

      System.out.println("createTable: result is for tableId " + tableId + " is " + res.toString());

      result = new JSONObject(res);
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return result;
  }

  /**
   * Creates a table on the server with the specified tableId and JSONObject.
   * schemaETag should be null if a new table is being created.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param jsonTableCreationObject
   *          a jsonObject that holds the values used to create a table
   *          definition
   * @return a JSONObject with the representation of the newly created table
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject createTableWithJSON(String uri, String appId, String tableId,
      String schemaETag, String jsonTableCreationObject) throws ClientProtocolException,
      IOException, JSONException {
    JSONObject tableObj = new JSONObject();
    JSONObject col;
    JSONObject result = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("createTableWithJSON: uri cannot be null");
    }

    if (tableId == null || tableId.isEmpty()) {
      throw new IllegalArgumentException("createTableWithJSON: tableId cannot be null");
    }

    if (jsonTableCreationObject == null || jsonTableCreationObject.isEmpty()) {
      throw new IllegalArgumentException(
          "createTableWithJSON: jsonTableCreationObject cannot be null");
    }

    HttpPut request = null;
    try {

      String agg_uri = UriUtils.getTableIdUri(uri, appId, tableId);
      System.out.println("createTableWithJSON: agg_uri is " + agg_uri);

      tableObj = new JSONObject(jsonTableCreationObject);

      if (!tableObj.containsKey(SCHEMA_ETAG_JSON)) {
        throw new IllegalArgumentException(
            "createTableWithJSON: jsonTableCreationObject does not have schemaETag");
      }

      if (!tableObj.containsKey(TABLE_ID_JSON)) {
        throw new IllegalArgumentException(
            "createTableWithJSON: jsonTableCreationObject does not have tableId");
      }

      if (!tableObj.containsKey(ORDERED_COLUMNS_DEF)) {
        throw new IllegalArgumentException(
            "createTableWithJSON: jsonTableCreationObject does not have orderedColumns");
      }

      JSONArray columns = tableObj.getJSONArray(ORDERED_COLUMNS_DEF);

      // Add the columns to the table object
      for (int i = 0; i < columns.size(); i++) {
        col = columns.getJSONObject(i);

        if (!col.containsKey(ELEM_KEY_JSON)) {
          throw new IllegalArgumentException(
              "createTableWithJSON: jsonTableCreationObject does orderedColumns " + i
                  + " does not have elementKey");
        }

        if (!col.containsKey(ELEM_NAME_JSON)) {
          throw new IllegalArgumentException(
              "createTableWithJSON: jsonTableCreationObject does orderedColumns " + i
                  + " does not have elementName");
        }

        if (!col.containsKey(ELEM_TYPE_JSON)) {
          throw new IllegalArgumentException(
              "createTableWithJSON: jsonTableCreationObject does orderedColumns " + i
                  + " does not have elementType");
        }

        if (!col.containsKey(LIST_CHILD_ELEM_KEYS_JSON)) {
          throw new IllegalArgumentException(
              "createTableWithJSON: jsonTableCreationObject does orderedColumns " + i
                  + " does not have listChildElementKeys");
        }
      }

      System.out.println("createTableWithJSON: with object " + tableObj.toString());

      request = new HttpPut(agg_uri);
      StringEntity params = new StringEntity(tableObj.toString(), UTF8_STR);
      request.setEntity(params);
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      String res = convertResponseToString(response);

      System.out.println("createTableWithJSON: result is for tableId " + tableId + " is "
          + res.toString());

      result = new JSONObject(res);
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return result;
  }

  /**
   * Creates a table on the server with the specified tableId using a csv file
   * which specifies the table definition. schemaETag should be null if a new
   * table is being created.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvFilePath
   *          file path to the definition.csv file
   * @return a JSONObject with the representation of the newly created table
   * 
   * @throws DataFormatException due to CSV file having improper format
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject createTableWithCSV(String uri, String appId, String tableId, String schemaETag,
      String csvFilePath) throws DataFormatException, FileNotFoundException, IOException,
      JSONException {
    ArrayList<Column> cols = new ArrayList<Column>();
    RFC4180CsvReader reader;

    File file = new File(csvFilePath);
    if (!file.exists()) {
      throw new IllegalArgumentException("createTableWithCSV: file " + csvFilePath
          + " does not exist");
    }
    InputStream in = new FileInputStream(file);
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName(UTF8_STR));
    reader = new RFC4180CsvReader(inputStream);

    return createTableWithCSVProcessing(uri, appId, tableId, schemaETag, cols, reader);
  }

  /**
   * Creates a table on the server with the specified tableId using a file input
   * stream. schemaETag should be null if a new table is being created.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvInputStream
   *          input stream for a table definition csv file
   * @return a JSONObject with the representation of the newly created table
   *  
   * @throws DataFormatException due to CSV file having improper format
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject createTableWithCSVInputStream(String uri, String appId, String tableId,
      String schemaETag, InputStream csvInputStream) throws DataFormatException, IOException,
      JSONException {
    ArrayList<Column> cols = new ArrayList<Column>();
    RFC4180CsvReader reader;

    if (csvInputStream.available() <= 0) {
      throw new IllegalArgumentException(
          "createTableWithCSVInputStream: csvInputStream is not available");
    }

    InputStream in = csvInputStream;
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName(UTF8_STR));
    reader = new RFC4180CsvReader(inputStream);

    return createTableWithCSVProcessing(uri, appId, tableId, schemaETag, cols, reader);
  }

  private JSONObject createTableWithCSVProcessing(String uri, String appId, String tableId,
      String schemaETag, ArrayList<Column> cols, RFC4180CsvReader reader) throws IOException,
      DataFormatException, JSONException {
    Column col;
    JSONObject resultingTable;
    // Make sure that the first line of the csv file
    // has the right header
    String[] firstLine = reader.readNext();

    int elementKeyIndex=-1;
    int elementNameIndex=-1;
    int elementTypeIndex=-1;
    int listChildElementKeysIndex=-1;

    for(int i=0;i<firstLine.length;i++)
    {
      if(firstLine[i].equals(ELEM_KEY_TABLE_DEF))
      {
        elementKeyIndex = i;
      }
      else if(firstLine[i].equals(ELEM_NAME_TABLE_DEF))
      {
        elementNameIndex = i;
      }
      else if(firstLine[i].equals(ELEM_TYPE_TABLE_DEF)){
        elementTypeIndex=i;
      }
      else if(firstLine[i].equals(LIST_CHILD_ELEM_KEYS_TABLE_DEF)){
        listChildElementKeysIndex=i;
      }
    }

    // Make sure that the first row of the csv file
    // has the right columns
    // if any index is -1 then that column is not found in the csv
    if (elementKeyIndex==-1||elementNameIndex==-1||elementTypeIndex==-1||listChildElementKeysIndex==-1) {
      throw new DataFormatException(
              "The csv file used to create a table does not have the correct columns in the first row");
    }

    String[] line;

    while ((line = reader.readNext()) != null) {
      col = new Column(line[elementKeyIndex], line[elementNameIndex], line[elementTypeIndex], line[listChildElementKeysIndex]);
      cols.add(col);
    }


    resultingTable = createTable(uri, appId, tableId, schemaETag, cols);

    reader.close();

    return resultingTable;
  }

  /**
   * Writes out the table definition for the specified tableId to a csv file
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvFilePath
   *          file path in which to save the table definition
   *          
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void writeTableDefinitionToCSV(String uri, String appId, String tableId,
      String schemaETag, String csvFilePath) throws ClientProtocolException, IOException,
      JSONException {
    RFC4180CsvWriter writer;

    File file = new File(csvFilePath);
    file.getParentFile().mkdirs();
    if (!file.exists()) {
      file.createNewFile();
    }

    // This fileWriter could be causing the issue with
    // UTF-8 characters - should probably use an OutputStream
    // here instead
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    writer = new RFC4180CsvWriter(fw);

    JSONObject tableDef = getTableDefinition(uri, appId, tableId, schemaETag);
    JSONArray orderedColumns = tableDef.getJSONArray(ORDERED_COLUMNS_DEF);
    String[] colArray = new String[4];

    // Create the first row of the csv
    colArray[0] = ELEM_KEY_TABLE_DEF;
    colArray[1] = ELEM_NAME_TABLE_DEF;
    colArray[2] = ELEM_TYPE_TABLE_DEF;
    colArray[3] = LIST_CHILD_ELEM_KEYS_TABLE_DEF;

    writer.writeNext(colArray);

    for (int i = 0; i < orderedColumns.size(); i++) {
      JSONObject col = orderedColumns.getJSONObject(i);
      colArray[0] = col.getString(ELEM_KEY_JSON);
      colArray[1] = col.getString(ELEM_NAME_JSON);
      colArray[2] = col.getString(ELEM_TYPE_JSON);
      colArray[3] = col.getString(LIST_CHILD_ELEM_KEYS_JSON);
      writer.writeNext(colArray);
    }
    writer.close();

  }

  /**
   * Gets the table definition and returns a JSONObject for the specified
   * tableId and schemaETag
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @return a JSONObject with the table definition
   *          
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors       
   */
  public JSONObject getTableDefinition(String uri, String appId, String tableId, String schemaETag)
      throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTableIdRefUri(uri, appId, tableId, schemaETag);
      System.out.println("getTableDefinition: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);

      System.out.println("getTableDefinition: result is for tableId " + tableId
          + " and schemaEtag " + schemaETag + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Deletes the table definition for a specified tableId and schemaETag
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   *          
   * @return Http response status code
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   */
  public int deleteTableDefinition(String uri, String appId, String tableId, String schemaETag)
      throws ClientProtocolException, IOException {
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpDelete request = null;
    try {
      String agg_uri = UriUtils.getTableIdRefUri(uri, appId, tableId, schemaETag);

      System.out.println("deleteTableDefinition: agg_uri is " + agg_uri);

      request = new HttpDelete(agg_uri);
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);
      
      System.out.println("deleteTableDefinition: client response is "
          + response.getStatusLine().getStatusCode() + ":"
          + response.getStatusLine().getReasonPhrase());

      return response.getStatusLine().getStatusCode();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

  }

  /**
   * Returns a JSONObject with the list of table level files for a given tableId
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param version
   *          ODK version code, 1 or 2
   * @return a JSONObject with the list of table level files
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getManifestForTableId(String uri, String appId, String tableId, String version)
      throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTableLevelManifestUri(uri, appId, version, tableId);
      System.out.println("getManifestForTableId: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getTableIdManifest: result for " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject of the rows that can be found in a table since the
   * given dataETag for the specified tableId and schemaETag
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param cursor
   *          query parameter that identifies the point at which to resume the
   *          query
   * @param fetchLimit
   *          query parameter that defines the number of rows to return
   * @param dataETag
   *          query parameter that defines the point at which to start getting
   *          rows
   * @return a JSONObject with the row data
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getRowsSince(String uri, String appId, String tableId, String schemaETag,
      String cursor, String fetchLimit, String dataETag) throws ClientProtocolException,
      IOException, JSONException {
    JSONObject obj = null;
    boolean useCursor = false;
    boolean useFetchLimit = false;
    boolean useDataETag = false;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);

      if (cursor != null && !cursor.isEmpty()) {
        useCursor = true;
      }

      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        useFetchLimit = true;
      }

      if (dataETag != null && !dataETag.isEmpty()) {
        useDataETag = true;
      }

      if (useCursor || useFetchLimit || useDataETag) {
        agg_uri = agg_uri + "?";
      }

      if (useCursor) {
        agg_uri = agg_uri + CURSOR_QUERY_PARAM + cursor;
        if (useFetchLimit || useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }

      if (useFetchLimit) {
        agg_uri = agg_uri + FETCH_LIMIT_QUERY_PARAM + fetchLimit;
        if (useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }

      if (useDataETag) {
        agg_uri = agg_uri + DATA_ETAG_QUERY_PARAM + dataETag;
      }

      System.out.println("getRowsSince: agg uri is " + agg_uri);
      
      request = new HttpGet(agg_uri);
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getRowsSince: result for " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject of the rows in a table for the specified tableId and
   * schemaETag
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param cursor
   *          query parameter that identifies the point at which to resume the
   *          query
   * @param fetchLimit
   *          query parameter that defines the number of rows to return
   * @return a JSONObject with the row data
   */
  public JSONObject getRows(String uri, String appId, String tableId, String schemaETag,
      String cursor, String fetchLimit) {
    JSONObject obj = null;
    boolean useCursor = false;
    boolean useFetchLimit = false;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);

      if (cursor != null && !cursor.isEmpty()) {
        useCursor = true;
      }

      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        useFetchLimit = true;
      }

      if (useCursor || useFetchLimit) {
        agg_uri = agg_uri + "?";
      }

      if (useFetchLimit) {
        agg_uri = agg_uri + FETCH_LIMIT_QUERY_PARAM + fetchLimit;
        if (useCursor)
          agg_uri = agg_uri + "&";
      }

      if (useCursor) {
        agg_uri = agg_uri + CURSOR_QUERY_PARAM + cursor;
      }
      System.out.println("getRows: agg uri is " + agg_uri);
      
      request = new HttpGet(agg_uri);
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getRows: result for " + tableId + " is " + obj.toString());

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  }

  /**
   * Writes out the row data for a given tableId and schemaETag to a csv file
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvFilePath
   *          the csv file path in which to write the row data
   *          
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void writeRowDataToCSV(String uri, String appId, String tableId, String schemaETag,
      String csvFilePath) throws IOException, JSONException {

    RFC4180CsvWriter writer;
    JSONObject rowWrapper;
    String resumeCursor = null;

    rowWrapper = getRows(uri, appId, tableId, schemaETag, resumeCursor, DFEAULT_FETCH_LIMIT);

    JSONArray rows = rowWrapper.getJSONArray(ROWS_STR_JSON);

    if (rows.size() <= 0) {
      System.out.println("writeRowDataToCSV: There are no rows to write out!");
      return;
    }

    File file = new File(csvFilePath);
    // Make the necessary directories if they are not already created
    file.getParentFile().mkdirs();
    
    if (!file.exists()) {
      file.createNewFile();
    }

    // This fileWriter could be causing the issue with
    // UTF-8 characters - should probably use an OutputStream
    // here instead
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    writer = new RFC4180CsvWriter(fw);

    JSONObject repRow = rows.getJSONObject(0);
    JSONArray orderedColumnsRep = repRow.getJSONArray(ORDERED_COLUMNS_DEF);

    int numberOfColsToMake = orderedColumnsRep.size() + metadataColumns1.length + metadataColumns2.length;
    String[] colArray = new String[numberOfColsToMake];

    int i = 0;
    
    for (int cnt = 0; cnt < metadataColumns1.length; cnt++) {
      colArray[i++] = metadataColumns1[cnt];
    }
    
    for (int j = 0; j < orderedColumnsRep.size(); j++) {
      JSONObject obj = orderedColumnsRep.getJSONObject(j);
      colArray[i++] = obj.getString("column");
    }

    for (int cnt = 0; cnt < metadataColumns2.length; cnt++) {
      colArray[i++] = metadataColumns2[cnt];
    }

    writer.writeNext(colArray);

    do {
      rowWrapper = getRows(uri, appId, tableId, schemaETag, resumeCursor, DFEAULT_FETCH_LIMIT);

      rows = rowWrapper.getJSONArray(ROWS_STR_JSON);

      writeOutFetchLimitRows(writer, rows, colArray);

      resumeCursor = rowWrapper.optString(WEB_SAFE_RESUME_CURSOR_JSON);

    } while (rowWrapper.getBoolean(HAS_MORE_RESULTS_JSON));

    writer.close();
  }

  private void writeOutFetchLimitRows(RFC4180CsvWriter writer, JSONArray rows, String[] colArray)
      throws JSONException, IOException {
    int i;
    String nullString = null;

    for (int k = 0; k < rows.size(); k++) {
      i = 0;
      JSONObject row = rows.getJSONObject(k);
      colArray[i++] = row.getString(ID_JSON);
      colArray[i++] = row.isNull(FORM_ID_JSON) ? nullString : row.getString(FORM_ID_JSON);
      colArray[i++] = row.getString(LOCALE_JSON);
      colArray[i++] = row.getString(SAVEPOINT_TYPE_JSON);
      colArray[i++] = row.getString(SAVEPOINT_TIMESTAMP_JSON);
      colArray[i++] = row.isNull(SAVEPOINT_CREATOR_JSON) ? nullString : row.getString(SAVEPOINT_CREATOR_JSON);

      JSONArray rowsOrderedCols = row.getJSONArray(ORDERED_COLUMNS_DEF);

      for (int l = 0; l < rowsOrderedCols.size(); l++) {
        JSONObject col = rowsOrderedCols.getJSONObject(l);
        if (col.isNull("value")) {
          colArray[i++] = nullString;
        } else {
          colArray[i++] = col.getString("value");
        }
      }

      colArray[i++] = row.isNull(ROW_ETAG_JSON) ? nullString : row.getString(ROW_ETAG_JSON);
      
      JSONObject filterScope = row.getJSONObject(FILTER_SCOPE_JSON);
      
      colArray[i++] = filterScope.isNull(DEFAULT_ACCESS_JSON) ? nullString : filterScope.getString(DEFAULT_ACCESS_JSON);
      colArray[i++] = filterScope.isNull(ROW_OWNER_JSON) ? nullString : filterScope.getString(ROW_OWNER_JSON);
      colArray[i++] = filterScope.isNull(GROUP_READ_ONLY_JSON) ? nullString : filterScope.getString(GROUP_READ_ONLY_JSON);
      colArray[i++] = filterScope.isNull(GROUP_MODIFY_JSON) ? nullString : filterScope.getString(GROUP_MODIFY_JSON);
      colArray[i++] = filterScope.isNull(GROUP_PRIVILEGED_JSON) ? nullString : filterScope.getString(GROUP_PRIVILEGED_JSON);

      writer.writeNext(colArray);
    }
  }

  /**
   * Returns a JSONObject with the row data for the specified tableId,
   * schemaETag, and rowId
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param rowId
   *          the unique id for a row in the table
   * @return a JSONObject that contains the row data
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors     
   */
  public JSONObject getRow(String uri, String appId, String tableId, String schemaETag, String rowId)
      throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getRowIdUri(uri, appId, tableId, schemaETag, rowId);
      System.out.println("getRow: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getRow: result for table " + tableId + " row " + rowId + " is "
          + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Converts a given rowId to a String that has only alphanumeric characters
   * and underscores. This is useful for creating a directory name in which to
   * store instance files or row level attachments.
   * 
   * @param rowId
   *          the unique identifier for the row
   * @return a String with only alphanumeric characters and underscores
   */
  public static String convertRowIdForInstances(String rowId) {
    String rowIdToUse = null;
    if (rowId == null || rowId.length() == 0) {
      throw new IllegalArgumentException(
          "convertRowIdForManifest: rowId cannot be null or the empty string!");
    } else {
      rowIdToUse = rowId.replaceAll("[\\p{Punct}\\p{Space}]", "_");
    }
    return rowIdToUse;
  }

  /**
   * Returns a JSONObject with the attachments for a given row in a specified
   * table
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param rowId
   *          the unique identifier for a row in the table
   * @return a JSONObject with the attachments for the row
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getManifestForRow(String uri, String appId, String tableId, String schemaETag,
      String rowId) throws ClientProtocolException, IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {

      String agg_uri = UriUtils.getRowLevelFileManifestUri(uri, appId, tableId, schemaETag, rowId);
      System.out.println("getManifestForRow: agg uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);

      System.out.println("getManifestForRow: result for " + tableId + " with rowId " + rowId
          + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Creates rows in the table associated with the tableId and schemaETag using
   * bulk upload
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param rowArrayList
   *          an ArrayList of rows to create
   * @param batchSize
   *          used to set the batch size of rows sent to the server - if 0 is
   *          passed in, the default of 500 is used
   *          
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void createRowsUsingBulkUpload(String uri, String appId, String tableId,
      String schemaETag, ArrayList<Row> rowArrayList, int batchSize)
      throws ClientProtocolException, IOException, JSONException {

    // Default values for rows
    String rowId = null;
    String locale = Locale.ENGLISH.getLanguage();
    String savepointType = SavepointTypeManipulator.complete();
    String savepointTimestamp = null;
    String savepointCreator = "anonymous";
    
    RowFilterScope defaultScope = RowFilterScope.EMPTY_ROW_FILTER;
    String dataETag = null;

    if (batchSize == 0) {
      batchSize = 500;
    }

    // No Row Id for bulk upload
    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);

    ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

    for (int i = 0; i < rowArrayList.size(); i++) {
      Row row = rowArrayList.get(i);
      Row rowObj = Row.forInsert(row.getRowId(), row.getFormId(), row.getLocale(),
          row.getSavepointType(), row.getSavepointTimestamp(), row.getSavepointCreator(),
          row.getRowFilterScope(), row.getValues());

      // Ensure that adequate defaults are set for all rows
      if (rowObj.getRowId() == null || rowObj.getRowId().length() == 0) {
        rowId = "uuid:" + UUID.randomUUID().toString();
        rowObj.setRowId(rowId);
      }

      if (rowObj.getLocale() == null || rowObj.getLocale().length() == 0) {
        rowObj.setLocale(locale);
      }

      if (rowObj.getSavepointType() == null || rowObj.getSavepointType().length() == 0) {
        rowObj.setSavepointType(savepointType);
      }

      if (rowObj.getSavepointTimestamp() == null || rowObj.getSavepointTimestamp().length() == 0) {
        savepointTimestamp =
            TableConstants.nanoSecondsFromMillis(System.currentTimeMillis(), Locale.ROOT);
        rowObj.setSavepointTimestamp(savepointTimestamp);
      }

      if (rowObj.getSavepointCreator() == null || rowObj.getSavepointCreator().length() == 0) {
        rowObj.setSavepointCreator(savepointCreator);
      }

      if (rowObj.getRowFilterScope() == null) {
        rowObj.setRowFilterScope(defaultScope);
      }

      processedRowArrayList.add(rowObj);

      if (processedRowArrayList.size() >= batchSize) {
        dataETag = getTableDataETag(uri, appId, tableId);
        bulkRowsSender(processedRowArrayList, agg_uri, tableId, dataETag, false);

        processedRowArrayList = new ArrayList<Row>();
      }
    }

    if (processedRowArrayList.size() > 0) {
      dataETag = getTableDataETag(uri, appId, tableId);
      bulkRowsSender(processedRowArrayList, agg_uri, tableId, dataETag, false);
    }
  }

  /**
   * Creates rows in the table associated with the tableId and schemaETag using
   * a JSONArray of rows. This function should be used when there are thousands
   * of rows of data to upload.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param jsonRows
   *          a JSONArray of rows to insert into the database
   * @param batchSize
   *          the number of rows that will be uploaded to the server at one time
   *          
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void createRowsUsingJSONBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String jsonRows, int batchSize) throws IOException, JSONException {

    // Default values for rows
    String rowId = "uuid:" + UUID.randomUUID().toString();
    String formId = null;
    String locale = Locale.ENGLISH.getLanguage();
    String savepointType = SavepointTypeManipulator.complete();
    String savepointTimestamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis(), Locale.ROOT);
    String savepointCreator = "anonymous";
    RowFilterScope defaultScope = RowFilterScope.EMPTY_ROW_FILTER;
    String dataETag = null;

    if (batchSize == 0) {
      batchSize = 500;
    }

    JSONObject rowWrapperObj = new JSONObject(jsonRows);
    JSONArray rowsObj = rowWrapperObj.getJSONArray(ROWS_STR_JSON);
    JSONObject rowObj = null;

    // No Row Id for bulk upload
    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);

    ArrayList<Row> rowArrayList = new ArrayList<Row>();

    for (int i = 0; i < rowsObj.size(); i++) {
      rowObj = rowsObj.getJSONObject(i);

      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();

      JSONArray orderedCols = rowObj.getJSONArray(ORDERED_COLUMNS_DEF);

      for (int j = 0; j < orderedCols.size(); j++) {
        JSONObject col = orderedCols.getJSONObject(j);
        DataKeyValue dkv = new DataKeyValue(col.getString("column"), col.getString("value"));
        dkvl.add(dkv);
      }

      if (rowObj.containsKey(ID_JSON)) {
        rowId = rowObj.getString(ID_JSON);
      }

      if (rowObj.containsKey(FORM_ID_JSON)) {
        formId = rowObj.getString(FORM_ID_JSON);
      }

      if (rowObj.containsKey(LOCALE_JSON)) {
        locale = rowObj.getString(LOCALE_JSON);
      }

      if (rowObj.containsKey(SAVEPOINT_TYPE_JSON)) {
        savepointType = rowObj.getString(SAVEPOINT_TYPE_JSON);
      }

      if (rowObj.containsKey(SAVEPOINT_TIMESTAMP_JSON)) {
        savepointTimestamp = rowObj.getString(SAVEPOINT_TIMESTAMP_JSON);
      }

      if (rowObj.containsKey(SAVEPOINT_CREATOR_JSON)) {
        savepointCreator = rowObj.getString(SAVEPOINT_CREATOR_JSON);
      }

      if (rowObj.containsKey(FILTER_SCOPE_JSON)) {
        JSONObject filterObj = rowObj.getJSONObject(FILTER_SCOPE_JSON);
        // TODO: CAL: Fix this for real
        defaultScope = RowFilterScope.asRowFilter(filterObj.getString(TYPE_STR), filterObj.getString("value"), "DEFAULT", null, null);
      }

      Row row = Row.forInsert(rowId, formId, locale, savepointType, savepointTimestamp,
          savepointCreator, defaultScope, dkvl);

      rowArrayList.add(row);

      if (rowArrayList.size() >= batchSize) {
        dataETag = getTableDataETag(uri, appId, tableId);
        bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);

        rowArrayList = new ArrayList<Row>();
      }
    }

    if (rowArrayList.size() > 0) {
      dataETag = getTableDataETag(uri, appId, tableId);
      bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);
    }
  }

  /**
   * Creates rows in the table associated with the tableId and schemaETag using
   * a CSV file in batches. This function should be used when there are
   * thousands of rows of data to upload.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvFilePath
   *          the file path from which to retrieve the row data
   * @param batchSize
   *          the number of rows that will be uploaded to the server at one time
   *          
   * @throws DataFormatException due to CSV file having improper format
   * @throws FileNotFoundException due to accessing non-existent files
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void createRowsUsingCSVBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String csvFilePath, int batchSize) throws FileNotFoundException,
      IOException, DataFormatException, JSONException {
    RFC4180CsvReader reader;

    File file = new File(csvFilePath);
    if (!file.exists()) {
      System.out.println("createRowsUsingCSVBulkUpload: file " + csvFilePath + " does not exist");
    }

    InputStream in = new FileInputStream(file);
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName(UTF8_STR));
    reader = new RFC4180CsvReader(inputStream);

    createRowsUsingCSVBulkUploadProcessing(uri, appId, tableId, schemaETag, batchSize, reader);
  }

  /**
   * Creates rows in the table associated with the tableId and schemaETag using
   * an input stream in batches. This function should be used when there are
   * thousands of rows of data to upload.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param csvInputStream
   *          the input stream from which to retrieve the row data
   * @param batchSize
   *          the number of rows that will be uploaded to the server at one time
   *          
   * @throws DataFormatException due to CSV file having improper format
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void createRowsUsingCSVInputStreamBulkUpload(String uri, String appId, String tableId,
      String schemaETag, InputStream csvInputStream, int batchSize) throws IOException,
      DataFormatException, JSONException {
    RFC4180CsvReader reader;

    if (csvInputStream.available() <= 0) {
      throw new IllegalArgumentException(
          "createTableWithCSVInputStream: csvInputStream is not available");
    }

    InputStream in = csvInputStream;
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName(UTF8_STR));
    reader = new RFC4180CsvReader(inputStream);

    createRowsUsingCSVBulkUploadProcessing(uri, appId, tableId, schemaETag, batchSize, reader);
  }

  private void createRowsUsingCSVBulkUploadProcessing(String uri, String appId, String tableId,
      String schemaETag, int batchSize, RFC4180CsvReader reader) throws IOException,
      DataFormatException, JsonProcessingException, UnsupportedEncodingException,
      ClientProtocolException, JSONException {

    String dataETag = null;

    if (batchSize == 0) {
      batchSize = 500;
    }

    // No Row Id for bulk upload
    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);

    // Make sure that the first line of the csv file
    // has the right header
    String[] firstLine = reader.readNext();
    int numOfCols = firstLine.length;

    // Make sure that the first row of the csv file
    // has the right columns
    for (int i = 0; i < metadataColumns1.length; i++) {
      if (!firstLine[i].equals(metadataColumns1[i])) {
        throw new DataFormatException(
            "The csv file used to create rows does not have the correct columns at the beginning of the first row");
      }
    }


    // Make sure that the first row of the csv file
    // has the right columns
    for (int i = 0; i < metadataColumns2.length; i++) {
      int firstLineIdx = numOfCols - (metadataColumns2.length - i);
      if(!firstLine[firstLineIdx].equals(metadataColumns2[i])) {
        throw new DataFormatException(
            "The csv file used to create rows does not have the correct columns at the end of the first row");
      }
    }

      String[] line;
      ArrayList<Row> rowArrayList = new ArrayList<Row>();
      line = reader.readNext();
      while (line != null) {

        if (line.length == 0) {
          line = reader.readNext();
          continue;
        }

        ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();

        for (int i = 6; i < numOfCols - 6; i++) {
          DataKeyValue dkv = new DataKeyValue(firstLine[i], line[i]);
          dkvl.add(dkv);
        }

        RowFilterScope.Access defaultAccess = RowFilterScope.Access.FULL;
        String defAccess = line[numOfCols - 6];
        String groupModify = line[numOfCols - 5];
        String groupPrivileged = line[numOfCols - 4];
        String groupReadOnly = line[numOfCols - 3];
        String rowOwner = line[numOfCols - 1];
        
        if (defAccess == null) {
          defaultAccess = RowFilterScope.Access.FULL;
        } else {
          defaultAccess = RowFilterScope.Access.valueOf(defAccess);
        }
        
        RowFilterScope rowFS = new RowFilterScope(defaultAccess, rowOwner, groupReadOnly, groupModify, groupPrivileged);
        Row row = Row.forInsert(line[0], line[1], line[2], line[3], line[4], line[5], rowFS, dkvl);

        rowArrayList.add(row);

        if (rowArrayList.size() >= batchSize) {
          dataETag = getTableDataETag(uri, appId, tableId);
          bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);

          rowArrayList = new ArrayList<Row>();
        }
        line = reader.readNext();
      }

      if (rowArrayList.size() > 0) {
        dataETag = getTableDataETag(uri, appId, tableId);
        bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);
      }
  }

  private RowOutcomeList bulkRowsSender(ArrayList<Row> rowArrayList, String agg_uri,
      String tableId, String dataETag, boolean print) throws JsonProcessingException,
      UnsupportedEncodingException, IOException, ClientProtocolException, JSONException {
    RowOutcomeList outcome = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }
    if (print) {
      System.out.println("Entering send");
    }

    HttpPut request = null;
    try {
      RowList rowList = new RowList();
      rowList.setRows(rowArrayList);

      if (dataETag != null && !dataETag.isEmpty()) {
        rowList.setDataETag(dataETag);
      }

      ObjectMapper mapper = new ObjectMapper();
      String rowRes = mapper.writeValueAsString(rowList);

      System.out.println("agg_uri is " + agg_uri);
      System.out.println("Params for request: " + rowRes);

      request = new HttpPut(agg_uri);
      StringEntity params = new StringEntity(rowRes, UTF8_STR);
      request.setEntity(params);
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      String res = convertResponseToString(response);

      if (print) {
        System.out.println("bulkRowsSender: result with tableId " + tableId + " is " + res);

        System.out.println("Exiting send");
      }

      outcome = mapper.readValue(res, RowOutcomeList.class);
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return outcome;
  }

  /**
   * Update row(s) in the table associated with the tableId and schemaETag using
   * bulk upload
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param dataETagVal
   *          identifies the last change of the table
   * @param rowArrayList
   *          an ArrayList of rows to create
   * @param batchSize
   *          used to set the batch size of rows sent to the server - if 0 is
   *          passed in, the default of 500 is used
   *          
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void updateRowsUsingBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String dataETagVal, ArrayList<Row> rowArrayList, int batchSize)
      throws ClientProtocolException, IOException, JSONException {
    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (!dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }

    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);
    System.out.println("updateRowsUsingBulkUpload: agg_uri is " + agg_uri);

    if (batchSize == 0) {
      batchSize = 500;
    }

    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(),
            row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
            row.getSavepointCreator(), row.getRowFilterScope(), row.getValues());

        processedRowArrayList.add(rowObj);

        if (processedRowArrayList.size() >= batchSize) {
          dataETag = getTableDataETag(uri, appId, tableId);
          bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);

          processedRowArrayList = new ArrayList<Row>();
        }
      }

      if (processedRowArrayList.size() > 0) {
        dataETag = getTableDataETag(uri, appId, tableId);
        bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Deletes row(s) in the table associated with the tableId and schemaETag
   * using bulk upload
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param dataETagVal
   *          identifies the last change of the table
   * @param rowArrayList
   *          an ArrayList of rows to create
   * @param batchSize
   *          used to set the batch size of rows sent to the server - if 0 is
   *          passed in, the default of 500 is used
   *          
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public void deleteRowsUsingBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String dataETagVal, ArrayList<Row> rowArrayList, int batchSize)
      throws ClientProtocolException, IOException, JSONException {
    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (!dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }

    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);
    System.out.println("deleteRowsUsingBulkUpload: agg_uri is " + agg_uri);

    if (batchSize == 0) {
      batchSize = 500;
    }

    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(),
            row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
            row.getSavepointCreator(), row.getRowFilterScope(), row.getValues());

        // Make sure that all of these rows are marked for deletion
        rowObj.setDeleted(true);

        processedRowArrayList.add(rowObj);

        if (processedRowArrayList.size() >= batchSize) {
          dataETag = getTableDataETag(uri, appId, tableId);
          bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);

          processedRowArrayList = new ArrayList<Row>();
        }
      }

      if (processedRowArrayList.size() > 0) {
        dataETag = getTableDataETag(uri, appId, tableId);
        bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Alters row(s) in the table associated with the tableId and schemaETag for
   * one batch - the size of the batch should not exceed 500.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param dataETagVal
   *          identifies the last change of the table
   * @param rowArrayList
   *          an ArrayList of rows to create
   * @return a RowOutcomeList with the row outcome and row data if applicable
   * 
   * @throws ClientProtocolException due to http errors
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   * 
   */
  public RowOutcomeList alterRowsUsingSingleBatch(String uri, String appId, String tableId,
      String schemaETag, String dataETagVal, ArrayList<Row> rowArrayList)
      throws ClientProtocolException, IOException, JSONException {
    RowOutcomeList outcome = new RowOutcomeList();

    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (dataETag == null && dataETagVal != null) {
      throw new IllegalArgumentException("The dataETag should be null");
    }

    if (dataETag != null && !dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }

    String agg_uri = UriUtils.getTableIdRowsUri(uri, appId, tableId, schemaETag);
    System.out.println("alterRowsUsingSingleBatch: agg_uri is " + agg_uri);

    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(),
            row.getLocale(), row.getSavepointType(), row.getSavepointTimestamp(),
            row.getSavepointCreator(), row.getRowFilterScope(), row.getValues());

        processedRowArrayList.add(rowObj);
      }

      if (processedRowArrayList.size() > 0) {
        outcome = bulkRowsSender(rowArrayList, agg_uri, tableId, dataETag, false);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return outcome;
  }

  /**
   * Get the file attachment and save it to the specified file for a given row
   * of a table.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param userRowId
   *          the unique identifier for a row
   * @param asAttachment
   *          false to save the data as a file, true to download the data
   * @param pathToSaveFile
   *          file path in which to save the attachment
   * @param relativePathOnServer
   *          the path on the server where the attachment resides
   * @throws IOException
   *           exception encountered is thrown to the caller
   * 
   */
  public void getFileForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, boolean asAttachment, String pathToSaveFile, String relativePathOnServer)
      throws IOException {
    // There can be multiple files per row
    // Relative path is valid for rows

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("getFileForRow: uri cannot be null");
    }

    if (pathToSaveFile == null || pathToSaveFile.isEmpty()) {
      throw new IllegalArgumentException("getFileForRow: pathToSaveFile cannot be null");
    }

    if (relativePathOnServer == null || relativePathOnServer.isEmpty()) {
      throw new IllegalArgumentException("getFileForRow: relativePathOnServer cannot be null");
    }

    HttpGet request = null;
    try {
      String agg_uri = UriUtils.getRowIdFileUriWithRelPathUri(uri, appId, tableId, schemaETag, userRowId, relativePathOnServer);

      if (asAttachment) {
        agg_uri = agg_uri + AS_ATTACHMENT_URI_FRAGMENT;
      }

      System.out.println("getFileForRow: agg_uri is " + agg_uri);

      File file = new File(pathToSaveFile);
      file.getParentFile().mkdirs();
      if (!file.exists()) {
        file.createNewFile();
      }
      
      System.out.println("getFileForRow: agg_uri is " + agg_uri);

      request = new HttpGet(agg_uri);
      String accept = determineContentType(file.getName());
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, accept, false);
      
      System.out.println("getFileForRow: issued get request for " + relativePathOnServer);

      InputStream fis = response.getEntity().getContent();

      FileOutputStream fos = new FileOutputStream(file.getAbsoluteFile());
      byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }

      fos.close();
      fis.close();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Get a batch of file attachments and save them to specified files for a
   * given row of a table.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param userRowId
   *          the unique identifier for a row
   * @param dirToSaveFiles
   *          file path in which to save the attachments
   * @param filesToGet
   *          JSONObject of files - same structure as returned in
   *          getManifestForRow
   * @param batchSizeInBytes
   *          the number of bytes to transfer in a batch default is 10MB
   * @throws JSONException
   *           exception encountered is thrown to the caller
   * 
   */
  public void batchGetFilesForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, String dirToSaveFiles, JSONObject filesToGet, int batchSizeInBytes)
      throws JSONException {

    int batchSizeToUse = MAX_BATCH_SIZE;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: uri cannot be null");
    }

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: appId cannot be null");
    }

    if (tableId == null || tableId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: tableId cannot be null");
    }

    if (schemaETag == null || schemaETag.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: schemaETag cannot be null");
    }

    if (userRowId == null || userRowId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: userRowId cannot be null");
    }

    if (dirToSaveFiles == null || dirToSaveFiles.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: dirToSaveFiles cannot be null");
    }

    if (filesToGet == null || filesToGet.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: filesToGet cannot be null");
    }

    if (batchSizeInBytes > 0 && batchSizeInBytes < MAX_BATCH_SIZE) {
      batchSizeToUse = batchSizeInBytes;
    }

    JSONArray totalFiles = filesToGet.getJSONArray(FILES_STR);

    JSONArray batchFilesArray = new JSONArray();

    int batchSize = 0;
    for (int i = 0; i < totalFiles.length(); i++) {
      JSONObject file = totalFiles.getJSONObject(i);
      batchSize += file.getInt("contentLength");
      batchFilesArray.add(file);

      if (batchSize >= batchSizeToUse) {
        JSONObject batchFiles = new JSONObject();
        batchFiles.put(FILES_STR, batchFilesArray);
        downloadBatchForRow(uri, appId, tableId, schemaETag, userRowId, dirToSaveFiles, batchFiles);
        batchSize = 0;
        batchFilesArray.clear();
      }
    }

    if (batchSize > 0) {
      JSONObject batchFiles = new JSONObject();
      batchFiles.put(FILES_STR, batchFilesArray);
      downloadBatchForRow(uri, appId, tableId, schemaETag, userRowId, dirToSaveFiles, batchFiles);
    }
  }
  
  /**
   * Upload permissions CSV
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param csvFilePath
   *          file path of the CSV
   * @return responseCode
   *          the http response code          
   *          
   * @throws IOException due to file errors
   */
  public int uploadPermissionCSV(String uri, String appId, String csvFilePath) throws IOException {
    
    int responseCode = 0;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("uploadPermissionCSV: uri cannot be null");
    }

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("uploadPermissionCSV: appId cannot be null");
    }

    if (csvFilePath == null || csvFilePath.isEmpty()) {
      throw new IllegalArgumentException("uploadPermissionCSV: csvFilePath cannot be null");
    }
    
    File file = new File(csvFilePath);
    if (!file.exists()) {
      System.out.println("uploadPermissionCSV: file " + csvFilePath + " does not exist");
      throw new IllegalArgumentException("uploadPermissionCSV: csvFilePath cannot be null");
    }
    
    HttpPost request = new HttpPost();
    HttpResponse response = null;
    
    String agg_uri = UriUtils.getUserPermissionsUri(uri);

    System.out.println("uploadPermissionCSV: agg_uri is " + agg_uri);

    request = new HttpPost(agg_uri);

    String boundary = "ref" + UUID.randomUUID();
    String accessDefFile = "access_def_file";

    NameValuePair params = new BasicNameValuePair("boundary", boundary);
    ContentType mt = ContentType.create(ContentType.MULTIPART_FORM_DATA.getMimeType(), params);

    MultipartEntityBuilder mpEntBuilder = MultipartEntityBuilder.create();

    mpEntBuilder.setBoundary(boundary);

    // Handle the user permission CSV file
    String contentType = determineContentType(file.getName());

    FormBodyPartBuilder formPartBodyBld = FormBodyPartBuilder.create();
    formPartBodyBld.addField("Content-Type", contentType);
    formPartBodyBld.addField("name", accessDefFile);

    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    InputStream is = null;
    try {
      is = new BufferedInputStream(new FileInputStream(csvFilePath));
      int length = 1024;
      // Transfer bytes from in to out
      byte[] data = new byte[length];
      int len;
      while ((len = is.read(data, 0, length)) >= 0) {
        if (len != 0) {
          bo.write(data, 0, len);
        }
      }
    } finally {
      is.close();
    }

    byte[] content = bo.toByteArray();
    
    System.out.println("The content of the file is " + new String(content));

    ByteArrayBody byteArrayBod = new ByteArrayBody(content, accessDefFile);
    formPartBodyBld.setBody(byteArrayBod);
    formPartBodyBld.setName(accessDefFile);
    mpEntBuilder.addPart(formPartBodyBld.build());

    HttpEntity mpFormEntity = mpEntBuilder.build();
    request.setEntity(mpFormEntity);

    try {
      response = httpRequestExecute(request, mt.toString(), true);

      System.out.println("uploadPermissionCSV: client response is "
          + response.getStatusLine().getStatusCode() + ":"
          + response.getStatusLine().getReasonPhrase());
      
      responseCode = response.getStatusLine().getStatusCode();

      if (response.getStatusLine().getStatusCode() < 200
          || response.getStatusLine().getStatusCode() >= 300) {
        return responseCode;
      }

    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    
    return responseCode;
  }
  

  /**
   * Get a batch of file attachments and save them to specified files for a
   * given row of a table.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param userRowId
   *          the unique identifier for a row
   * @param dirToSaveFiles
   *          file path in which to save the attachments
   * @param filesToGet
   *          JSONObject of files - same structure as returned in
   *          getManifestForRow
   * 
   */
  public void downloadBatchForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, String dirToSaveFiles, JSONObject filesToGet) {

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: uri cannot be null");
    }

    if (appId == null || appId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: appId cannot be null");
    }

    if (tableId == null || tableId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: tableId cannot be null");
    }

    if (schemaETag == null || schemaETag.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: schemaETag cannot be null");
    }

    if (userRowId == null || userRowId.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: userRowId cannot be null");
    }

    if (dirToSaveFiles == null || dirToSaveFiles.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: dirToSaveFiles cannot be null");
    }

    if (filesToGet == null || filesToGet.isEmpty()) {
      throw new IllegalArgumentException("batchGetFilesForRow: filesToGet cannot be null");
    }

    HttpPost request = null;
    try {
      String agg_uri = UriUtils.getRowIdAttachmentsDownloadUri(uri, appId, tableId, schemaETag, userRowId);

      System.out.println("batchGetFilesForRow: agg_uri is " + agg_uri);

      request = new HttpPost(agg_uri);

      StringEntity params = new StringEntity(filesToGet.toString(), UTF8_STR);
      request.setEntity(params);
      
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), true);

      System.out.println("batchGetFilesForRow: client response is "
          + response.getStatusLine().getStatusCode() + ":"
          + response.getStatusLine().getReasonPhrase());

      if (response.getStatusLine().getStatusCode() < 200
          || response.getStatusLine().getStatusCode() >= 300) {
        return;
      }

      String boundaryVal = null;
      Header hdr = response.getEntity().getContentType();
      HeaderElement[] hdrElem = hdr.getElements();
      for (HeaderElement elm : hdrElem) {
        int cnt = elm.getParameterCount();
        for (int i = 0; i < cnt; i++) {
          NameValuePair nvp = elm.getParameter(i);
          String nvp_name = nvp.getName();
          String nvp_value = nvp.getValue();
          if (nvp_name.equals(BOUNDARY)) {
            boundaryVal = nvp_value;
            break;
          }
        }
      }

      // Best to return at this point if we can't
      // determine the boundary to parse the multi-part form
      if (boundaryVal == null) {
        return;
      }

      InputStream inStream = response.getEntity().getContent();

      byte[] msParam = boundaryVal.getBytes(Charset.forName(UTF8_STR));
      MultipartStream multipartStream = new MultipartStream(inStream, msParam,
          DEFAULT_BOUNDARY_BUFSIZE, null);

      OutputStream os = null;

      // Parse the request
      boolean nextPart = multipartStream.skipPreamble();
      while (nextPart) {
        String header = multipartStream.readHeaders();
        System.out.println("Headers: " + header);

        // Get the file name
        int firstIndex = header.indexOf(MULTIPART_FILE_HEADER) + MULTIPART_FILE_HEADER.length();
        int lastIndex = header.lastIndexOf("\"");
        String instFileName = header.substring(firstIndex, lastIndex);

        File instFile = new File(dirToSaveFiles + File.separator + instFileName);
        instFile.getParentFile().mkdirs();
        if (!instFile.exists()) {
          instFile.createNewFile();
        }

        try {
          os = new BufferedOutputStream(new FileOutputStream(instFile));

          multipartStream.readBodyData(os);
          os.flush();
          os.close();
          os = null;
        } catch (IOException e) {
          e.printStackTrace();
          System.out
              .println("batchGetFilesForRow: Download file batches: Unable to read attachment");
          return;
        } finally {
          if (os != null) {
            try {
              os.close();
            } catch (IOException e) {
              e.printStackTrace();	
              System.out
                  .println("batchGetFilesForRow: Download file batches: Error closing output stream");
            }
          }
        }
        nextPart = multipartStream.readBoundary();
      }
    } catch (Exception e) {
    	e.printStackTrace();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Upload the file attachment for a given row of a table.
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param userRowId
   *          the unique identifier for a row
   * @param wholePathToFile
   *          file path of the file to upload
   * @param relativePathOnServer
   *          the path on the server for the attachment
   * @throws IOException
   *           exception encountered is thrown to the caller
   * @return Http response status code
   */
  public int putFileForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, String wholePathToFile, String relativePathOnServer) throws IOException {
    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("putFileForRow: uri cannot be null");
    }

    if (wholePathToFile == null || wholePathToFile.isEmpty()) {
      throw new IllegalArgumentException("putFileForRow: wholePathToFile cannot be null");
    }

    if (relativePathOnServer == null || relativePathOnServer.isEmpty()) {
      throw new IllegalArgumentException("putFileForRow: relativePathOnServer cannot be null");
    }

    HttpPost request = null;
    try {
      String agg_uri = UriUtils.getRowIdFileUriWithRelPathUri(uri, appId, tableId, schemaETag, userRowId, relativePathOnServer);
      System.out.println("putFileForRow: agg_uri is " + agg_uri);

      File file = new File(wholePathToFile);
      if (!file.exists()) {
        System.out.println("putFileForRow: file " + wholePathToFile + " does not exist");
        throw new IllegalArgumentException("putFileForRow: wholePathToFile cannot be null");
      }

      byte[] data = Files.readAllBytes(file.toPath());
      
      System.out.println("putFileForRow: response for file " + wholePathToFile + " is ");

      request = new HttpPost(agg_uri);
      HttpEntity entity = new ByteArrayEntity(data);
      request.setEntity(entity);
      String contentType = determineContentType(file.getName());
      
      HttpResponse response = null;

      // issue the request
      response = httpRequestExecute(request, contentType, false);
      
      BufferedReader responseBuff = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName(UTF8_STR)));
      String line;
      while ((line = responseBuff.readLine()) != null)
        System.out.println(line);

      return response.getStatusLine().getStatusCode();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Returns a JSONObject of the rows that can be found in a table specified
   * tableId and schemaETag in the range of a specified startTime and endTime
   * using lastUpdateDate
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param startTime
   *          a required timestamp used to get all rows with a time greater than
   *          or equal to it. The format of startTime is
   *          yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param endTime
   *          an optional timestamp used to get all rows with a time less than
   *          or equal to it. The format for endTime is
   *          yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param cursor
   *          query parameter that identifies the point at which to resume the
   *          query
   * @param fetchLimit
   *          query parameter that defines the number of rows to return
   * @return a JSONObject with the row data
   * 
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject queryRowsInTimeRangeWithLastUpdateDate(String uri, String appId,
      String tableId, String schemaETag, String startTime, String endTime, String cursor,
      String fetchLimit) throws IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (startTime == null || startTime.isEmpty()) {
      throw new IllegalArgumentException(
          "startTime must have a valid value in the format yyyy-MM-dd:HH:mm:ss.SSSSSSSSS");
    }

    HttpGet request = null;
    try {
      // RestClient restClient = new RestClient();

      String agg_uri = UriUtils.getTableIdLastUpdateDateQueryUri(uri, appId, tableId, schemaETag);

      agg_uri = agg_uri + "?" + START_TIME_QUERY_PARAM + startTime;

      if (endTime != null && !endTime.isEmpty()) {
        agg_uri = agg_uri + "&" + END_TIME_QUERY_PARAM + endTime;
      }

      if (cursor != null && !cursor.isEmpty()) {
        agg_uri = agg_uri + "&" + CURSOR_QUERY_PARAM + cursor;
      }

      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        agg_uri = agg_uri + "&" + FETCH_LIMIT_QUERY_PARAM + fetchLimit;
      }

      System.out.println("queryRowsInTimeRangeWithLastUpdateDate: agg uri is " + agg_uri);
      
      request = new HttpGet(agg_uri);
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("queryRowsInTimeRangeWithLastUpdateDate: result for " + tableId + " is "
          + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }
  
  /**
   * Returns a JSONObject of the changed rows that can be found in a table specified
   * tableId and schemaETag since the dataETag
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param dataETag
   *          identifies a set of changes to the table
   * @param cursor
   *          query parameter that identifies the point at which to resume the
   *          query
   * @param fetchLimit
   *          query parameter that defines the number of rows to return
   * @return a JSONObject with the row data
   * 
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject getAllDataChangesSince(String uri, String appId,
      String tableId, String schemaETag, String dataETag, String cursor,
      String fetchLimit) throws IOException, JSONException {
    JSONObject obj = null;
    boolean useCursor = false;
    boolean useFetchLimit = false;
    boolean useDataETag = false;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
      // RestClient restClient = new RestClient();

      String agg_uri = UriUtils.getTableIdDiffUri(uri, appId, tableId, schemaETag);
      
      if (dataETag != null && !dataETag.isEmpty()) {
        useDataETag = true;
      }
      
      if (cursor != null && !cursor.isEmpty()) {
        useCursor = true;
      }

      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        useFetchLimit = true;
      }

      
      if (useCursor || useFetchLimit || useDataETag) {
        agg_uri = agg_uri + "?";
      }

      if (useCursor) {
        agg_uri = agg_uri + CURSOR_QUERY_PARAM + cursor;
        if (useFetchLimit || useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }

      if (useFetchLimit) {
        agg_uri = agg_uri + FETCH_LIMIT_QUERY_PARAM + fetchLimit;
        if (useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }

      if (useDataETag) {
        agg_uri = agg_uri + DATA_ETAG_QUERY_PARAM + dataETag;
      }

      System.out.println("getAllDataChangesSince: agg uri is " + agg_uri);
      
      request = new HttpGet(agg_uri);
      HttpResponse response = null;
      
      response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("getAllDataChangesSince: result for " + tableId + " is "
          + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject of the rows that can be found in a table specified
   * tableId and schemaETag in the range of a specified startTime and endTime
   * using savepointTimestamp
   * 
   * @param uri
   *          the url for the server
   * @param appId
   *          identifies the application
   * @param tableId
   *          the table identifier or name
   * @param schemaETag
   *          identifies an instance of the table
   * @param startTime
   *          a required timestamp used to get all rows with a time greater than
   *          or equal to it. The format of startTime is
   *          yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param endTime
   *          an optional timestamp used to get all rows with a time less than
   *          or equal to it. The format for endTime is
   *          yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param cursor
   *          query parameter that identifies the point at which to resume the
   *          query
   * @param fetchLimit
   *          query parameter that defines the number of rows to return
   * @return a JSONObject with the row data
   * 
   * @throws IOException due to file errors
   * @throws JSONException due to JSON errors
   */
  public JSONObject queryRowsInTimeRangeWithSavepointTimestamp(String uri, String appId,
      String tableId, String schemaETag, String startTime, String endTime, String cursor,
      String fetchLimit) throws IOException, JSONException {
    JSONObject obj = null;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (startTime == null || startTime.isEmpty()) {
      throw new IllegalArgumentException(
          "startTime must have a valid value in the format yyyy-MM-dd:HH:mm:ss.SSSSSSSSS");
    }

    HttpGet request = null;
    try {
      String agg_uri = UriUtils.getTableIdSavepointTimestampQueryUri(uri, appId, tableId, schemaETag);

      agg_uri = agg_uri + "?" + START_TIME_QUERY_PARAM + startTime;

      if (endTime != null && !endTime.isEmpty()) {
        agg_uri = agg_uri + "&" + END_TIME_QUERY_PARAM + endTime;
      }

      if (cursor != null && !cursor.isEmpty()) {
        agg_uri = agg_uri + "&" + CURSOR_QUERY_PARAM + cursor;
      }

      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        agg_uri = agg_uri + "&" + FETCH_LIMIT_QUERY_PARAM + fetchLimit;
      }

      System.out.println("queryRowsInTimeRangeWithSavepointTimestamp: agg uri is " + agg_uri);
      
      request = new HttpGet(agg_uri);
      HttpResponse response = httpRequestExecute(request, mimeMapping.get(JSON_STR), false);

      obj = convertResponseToJSONObject(response);
      System.out.println("queryRowsInTimeRangeWithSavepointTimestamp: result for " + tableId
          + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  }

  public void close() {
    if (httpClient != null) {
      try {
        httpClient.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
    	}
    }
  }
}
