package org.opendatakit.wink.client;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.zip.DataFormatException;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
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
import org.opendatakit.aggregate.odktables.rest.entity.RowList;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.aggregate.odktables.rest.entity.Scope;
import org.apache.commons.fileupload.MultipartStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;

/**
 * Class used to communicate with the ODK and Mezuri servers
 * via the REST API.  This class uploads data 
 * to the server and downloads data from the server.
 *   
 * @author clarlars@gmail.com
 *
 */
public class WinkClient {
  public static final String queryParamDataETag = "data_etag=";

  public static final String queryParamFetchLimit = "fetchLimit=";

  public static final String queryParamCursor = "cursor=";
  
  public static final String queryParamStartTime = "startTime=";
  
  public static final String queryParamEndTime = "endTime=";

  public static final String t = "WinkClient";

  public static final String uriFilesFragment = "/files/2/";

  public static final String uriTablesFragment = "/tables";

  public static final String uriManifest = "/manifest/2";
  
  public static final String uriLastUpdateDateFragment = "/lastUpdateDate";
  
  public static final String uriSavepointTimestamp = "/savepointTimestamp";
  
  public static final String uriQueryFragment = "/query";

  public static final String uriRefFragment = "/ref/";

  public static final String uriRowsFragment = "/rows";

  public static final String uriAttachmentsFragment = "/attachments/";

  public static final String uriFileFragment = "/file/";
  
  public static final String downloadFragment = "/download";

  public static final String uriManifestFragment = "/manifest";

  public static final String uriRowETagFragment = "?row_etag=";

  public static final String uriAsAttachmentFragment = "?as_attachment=true";

  public static final String separator = "/";

  // Maybe better to have a function to map this?
  public static final String jsonElemKey = "elementKey";

  public static final String jsonElemName = "elementName";

  public static final String jsonElemType = "elementType";

  public static final String jsonListChildElemKeys = "listChildElementKeys";

  public static final String tableDefElemKey = "_element_key";

  public static final String tableDefElemName = "_element_name";

  public static final String tableDefElemType = "_element_type";

  public static final String tableDefListChildElemKeys = "_list_child_element_keys";

  // Row definitions - mapping here?
  public static final String jsonId = "id";

  public static final String jsonFormId = "formId";

  public static final String jsonLocale = "locale";

  public static final String jsonSavepointType = "savepointType";

  public static final String jsonSavepointTimestamp = "savepointTimestamp";

  public static final String jsonSavepointCreator = "savepointCreator";

  public static final String jsonRowETag = "rowETag";

  public static final String jsonRowsString = "rows";
  
  public static final String jsonFilterScope = "filterScope";
  
  public static final String jsonTableId = "tableId";
  
  public static final String jsonTables = "tables";
  
  public static final String jsonSchemaETag = "schemaETag";
  
  public static final String jsonDataETag = "dataETag";

  public static final String rowDefId = "_id";

  public static final String rowDefFormId = "_form_id";

  public static final String rowDefLocale = "_locale";

  public static final String rowDefSavepointType = "_savepoint_type";

  public static final String rowDefSavepointTimestamp = "_savepoint_timestamp";

  public static final String rowDefSavepointCreator = "_savepoint_creator";

  public static final String rowDefRowETag = "_row_etag";

  public static final String rowDefFilterType = "_filter_type";

  public static final String rowDefFilterValue = "_filter_value";

  public static final String orderedColumnsDef = "orderedColumns";
  
  public static final String defaultFetchLimit = "1000";
  
  public static final String BOUNDARY = "boundary";
  
  protected static final int DEFAULT_BOUNDARY_BUFSIZE = 4096;
  
  public static final String multipartFileHeader = "filename=\"";
  
  public static final int MAX_BATCH_SIZE = 10485760;

  private CloseableHttpClient httpClient = null;

  private HttpContext localContext = null;

  private CookieStore cookieStore = null;
  
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
    httpClient = HttpClientBuilder.create().build();
	  
  }
  
  /**
   * Init client with parameters for digest auth
   * 
   * @param host the host name to authenticate against
   * @param userName the user name to use for authentication 
   * @param password the password to use for authentication
   */
  public void init(String host, String userName, String password) {
    int CONNECTION_TIMEOUT = 60000;
 
    // Context 
    // context holds authentication state machine, so it cannot be
    // shared across independent activities.
    localContext = new BasicHttpContext();

    cookieStore = new BasicCookieStore();
    credsProvider = new BasicCredentialsProvider();
    
    //AuthScope a = new AuthScope("adapt.epi-ucsf.org", -1, null, AuthPolicy.DIGEST);
    AuthScope a = new AuthScope(host, -1, null, AuthSchemes.DIGEST);
    Credentials c = new UsernamePasswordCredentials(userName, password);
    credsProvider.setCredentials(a, c);
    
    localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
    localContext.setAttribute(HttpClientContext.CREDS_PROVIDER, credsProvider);
    
    SocketConfig socketConfig = SocketConfig.copy(SocketConfig.DEFAULT).setSoTimeout(2*CONNECTION_TIMEOUT).build();
	  
    // if possible, bias toward digest auth (may not be in 4.0 beta 2)
    List<String> targetPreferredAuthSchemes = new ArrayList<String>();
    targetPreferredAuthSchemes.add(AuthSchemes.DIGEST);
    targetPreferredAuthSchemes.add(AuthSchemes.BASIC);

    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
        .setConnectTimeout(CONNECTION_TIMEOUT)
        // support authenticating
        .setAuthenticationEnabled(true)
        // support redirecting to handle http: => https: transition
        .setRedirectsEnabled(true)
        // max redirects is set to 4
        .setMaxRedirects(4)
        .setCircularRedirectsAllowed(true)
        .setTargetPreferredAuthSchemes(targetPreferredAuthSchemes)
        .setCookieSpec(CookieSpecs.DEFAULT)
        .build();
	
    httpClient = HttpClientBuilder.create()
        .setDefaultSocketConfig(socketConfig)
        .setDefaultRequestConfig(requestConfig).build();
  }
  
  /**
   * Init client parameters for authentication  
   * 
   * @param host the host to authenticate against
   * @param userName the user name  to use for auth
   * @param password the password to use for auth
   * @param socketConfig the socket config parameters to use
   * @param reqConfig the request config parameters to use
   * @param basicStore the cookie store to use 
   * @param basicProvider the credentials provider to use
   */
  public void init(String host, String userName, String password, SocketConfig socketConfig,
      RequestConfig reqConfig, BasicCookieStore basicStore, BasicCredentialsProvider basicProvider) {
	  
    localContext = new BasicHttpContext();

    if (basicStore != null) {
      cookieStore = basicStore;
      localContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
    }

    if  (basicProvider != null) {
      credsProvider = basicProvider;
    }
    
    if (credsProvider != null && userName != null && password != null && host != null) {
      AuthScope a = new AuthScope(host, -1, null, AuthSchemes.DIGEST);
      Credentials c = new UsernamePasswordCredentials(userName, password);
      credsProvider.setCredentials(a, c);
    }

    if (credsProvider != null) {
      localContext.setAttribute(HttpClientContext.CREDS_PROVIDER, credsProvider);
    }
    
    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    
    if (socketConfig != null) {
    	clientBuilder.setDefaultSocketConfig(socketConfig);	
    }
    
    if (reqConfig != null) {
       clientBuilder.setDefaultRequestConfig(reqConfig);	
    }
    
    httpClient = clientBuilder.build();
   
  }
  
  /**
   * Gets the schemaETag for the table   
   * 
   * @param agg_url the url for the server
   * @param appId the application id 
   * @param tableId the table id for the table in question
   * @return String to return schemaETag value
   */
  public String getSchemaETagForTable(String agg_url, String appId, String tableId) {

    try {
      //WinkClient wc = new WinkClient();
      //JSONObject obj = wc.getTables(agg_url, appId);
      JSONObject obj = getTables(agg_url, appId);

      JSONArray tables = obj.getJSONArray(jsonTables);

      for (int i = 0; i < tables.size(); i++) {
        JSONObject table = tables.getJSONObject(i);
        if (tableId.equals(table.getString(jsonTableId))) {
          return table.getString(jsonSchemaETag);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return null;
  }

  /**
   * Gets all data from a uri on the server and saves
   * the data to the specified directory.  First, app level and 
   * table level files are retrieved and saved to the directory.  
   * Then the definition.csv and data.csv files for all of the tables
   * are saved.  Finally, any row level attachments are saved.   
   * 
   * @param uri the url for the server
   * @param appId the application id 
   * @param dirToSaveDataTo the directory in which the data will be saved
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void getAllDataFromUri(String uri, String appId, String dirToSaveDataTo) throws Exception {

    // Get all App Level Files
    getAllAppLevelFilesFromUri(uri, appId, dirToSaveDataTo);

    // Get all the Tables
    JSONObject tableRes = getTables(uri, appId);
    JSONArray tables = tableRes.getJSONArray("tables");

    for (int i = 0; i < tables.size(); i++) {
      JSONObject table = tables.getJSONObject(i);
      String tableId = table.getString("tableId");
      String schemaETag = table.getString("schemaETag");

      // Get all Table Level Files
      getAllTableLevelFilesFromUri(uri, appId, tableId, dirToSaveDataTo);

      // Write out Table Definition CSV's
      String tableDefinitionCSVPath = dirToSaveDataTo + separator + "tables" + separator + tableId
          + separator + "definition.csv";
      writeTableDefinitionToCSV(uri, appId, tableId, schemaETag, tableDefinitionCSVPath);

      // Write out the Table Data CSV's
      String dataCSVPath = dirToSaveDataTo + separator + "assets" + separator + "csv" + separator
          + tableId + ".csv";
      writeRowDataToCSV(uri, appId, tableId, schemaETag, dataCSVPath);

      // Get all Instance Files
      // get all rows - check for attachment
      getAllTableInstanceFilesFromUri(uri, appId, tableId, schemaETag, dirToSaveDataTo);
    }
  }

  /**
   * Descends into the directory tree and
   * find all of the files
   * 
   * @param dir a file object defining the top level directory
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
   * Finds the top level subdirectories within
   * the specified directory
   * 
   * @param dir file object of the top directory
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
   * Table definitions must be in
   * a subdirectory named "tables/{tableName}.  
   * For example, a definition.csv file
   * for a table named test must be in folder tables/test.  
   * <p>
   * Row attachments must be 
   * in a folder named tables/{tableName}/instances/{rowId}.  
   * For example, an image.jpg file
   * for rowId 1f9e6f19_c50a_4436_9328_1d70cdb73493 of test table would be in 
   * tables/test/instances/1f9e6f19_c50a_4436_9328_1d70cdb73493.  
   * If the rowId contains anything other than alphanumeric characters, 
   * the directory name for the rowId will be converted to 
   * replace the non-alphanumeric characters to underscores.  
   * For example, row id 1f9e6f19-c50a-4436-9328-1d70cdb73493 
   * will be converted to 1f9e6f19_c50a_4436_9328_1d70cdb73493.
   * @param uri the url for the server
   * @param appId the application id 
   * @param dirToGetDataFrom the directory that has the data to push to the server 
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void pushAllDataToUri(String uri, String appId, String dirToGetDataFrom) throws Exception {
    ArrayList<String> assetsFiles;
    ArrayList<String> tableFiles;
    ArrayList<String> tableIds;
    ArrayList<String> instanceFiles = new ArrayList<String>();

    // Get all files in the assets directory
    String assetsDir = dirToGetDataFrom + File.separator + "assets";
    assetsFiles = recurseDir(new File(assetsDir));

    // Get all the tableIds in the tables directory
    String tablesDir = dirToGetDataFrom + File.separator + "tables";
    tableIds = findTopLevelSubdirectories(new File(tablesDir));

    for (int i = 0; i < tableIds.size(); i++) {
      // Create a table definition
      JSONObject tableResult = null;
      String schemaETag = null;
      String tableId = tableIds.get(i);
      String tableDefPath = tablesDir + File.separator + tableId + File.separator + "definition.csv";
      File tableDefCSV = new File(tableDefPath);
      if (tableDefCSV.exists()) {
        tableResult = createTableWithCSV(uri, appId, tableId, "", tableDefPath);
        if (!tableResult.isNull("schemaETag")) {
          schemaETag = tableResult.getString("schemaETag");
        }
      }

      // Create table rows
      String dataRowPath = assetsDir + File.separator + "csv" + File.separator + tableId + ".csv";
      File dataRowCSV = new File(dataRowPath);
      if (dataRowCSV.exists()) {
        // createRowsUsingCSV(uri, appId, tableId, schemaETag, dataRowPath);
        int fLimit = Integer.parseInt(defaultFetchLimit);
        createRowsUsingCSVBulkUpload(uri, appId, tableId, schemaETag, dataRowPath, fLimit);
      }

      LinkedHashMap <String, String> mapRowIdToInstanceDir = new LinkedHashMap<String, String> ();
      
      JSONObject obj = null;
      String resumeCursor = null;
       do {
         // Get all of the rowId's for the table
         obj = getRows(uri, appId, tableId, schemaETag, resumeCursor, defaultFetchLimit);
         JSONArray rows = obj.getJSONArray(jsonRowsString);
         for (int k = 0; k < rows.size(); k++) {
           JSONObject row = rows.getJSONObject(k);
           String rowId = row.getString("id");
           mapRowIdToInstanceDir.put(convertRowIdForInstances(rowId), rowId);
         }

        resumeCursor = obj.getString("webSafeResumeCursor");
      } while(obj.getBoolean("hasMoreResults"));
      
      // Find table Id Files and push up
      String tableIdPath = tablesDir + File.separator + tableId;
      tableFiles = recurseDir(new File(tableIdPath));

      String tableInstancesPath = tableIdPath + File.separator + "instances";
      for (int j = 0; j < tableFiles.size(); j++) {
        if (tableFiles.get(j).startsWith(tableInstancesPath)) {
          instanceFiles.add(tableFiles.get(j));
        } else {
          String filePath = tableFiles.get(j);
          String relativePathOnServer = filePath.substring(dirToGetDataFrom.length() + 1);
          uploadFile(uri, appId, filePath, relativePathOnServer);
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
        putFileForRow(uri, appId, tableId, schemaETag, rowIdToUse, filePath,
            instanceFile.getName());
      }
    }

    for (int i = 0; i < assetsFiles.size(); i++) {
      String filePath = assetsFiles.get(i);
      String relativePathOnServer = filePath.substring(dirToGetDataFrom.length() + 1);
      uploadFile(uri, appId, filePath, relativePathOnServer);
    }
  }

  /**
   * Gets all of the instance files (attachments) for a table   
   * and saves them to the specified directory
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param dirToSaveDataTo the directory in which the data will be saved
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void getAllTableInstanceFilesFromUri(String uri, String appId, String tableId,
      String schemaETag, String dirToSaveDataTo) throws Exception {

    // Get all rows for table
    JSONObject row = null;
    String resumeCursor = null;
    do {
      row = getRows(uri, appId, tableId, schemaETag, resumeCursor, defaultFetchLimit);
      //JSONObject row = getRows(uri, appId, tableId, schemaETag, null, defaultFetchLimit);
      JSONArray tableRows = row.getJSONArray(jsonRowsString);

      for (int i = 0; i < tableRows.size(); i++) {
        JSONObject tableRow = tableRows.getJSONObject(i);
        String rowId = tableRow.getString("id");
        JSONObject files = null;
        try {
          files = getManifestForRow(uri, appId, tableId, schemaETag, rowId);
        } catch (Exception e) {
          e.printStackTrace();
          if (files == null) {
            System.out.println("getAllTableInstanceFilesFromUri: known issue with table:" + tableId + " row: " + rowId + " causes exception");
            continue;
          }
        }
        JSONArray rowFiles = files.getJSONArray("files");

        for (int j = 0; j < rowFiles.size(); j++) {
          JSONObject rowFile = rowFiles.getJSONObject(j);
          String fileName = rowFile.getString("filename");

          // Convert rowId for Aggregate
          String rowIdForSave = convertRowIdForInstances(rowId);
          String pathToSaveFile = dirToSaveDataTo + separator + "tables" + separator + tableId
            + separator + "instances" + separator + rowIdForSave + separator + fileName;
          // Should relativeDir be set here
          getFileForRow(uri, appId, tableId, schemaETag, rowId, false, pathToSaveFile, fileName);
        }
      }
      resumeCursor = row.getString("webSafeResumeCursor");
    }while(row.getBoolean("hasMoreResults"));
  }

  /**
   * Gets all of the table level files for a single table 
   * and saves them to the specified directory
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param dirToSaveDataTo the directory in which the data will be saved
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void getAllTableLevelFilesFromUri(String uri, String appId, String tableId,
      String dirToSaveDataTo) throws Exception {
    String relativeDir;
    JSONObject file;

    // Get all table level files
    JSONObject table = getManifestForTableId(uri, appId, tableId);
    JSONArray tableLevelFiles = table.getJSONArray("files");

    for (int i = 0; i < tableLevelFiles.size(); i++) {
      file = tableLevelFiles.getJSONObject(i);
      relativeDir = file.getString("filename");
      String pathToSaveFile = dirToSaveDataTo + separator + relativeDir;
      downloadFile(uri, appId, pathToSaveFile, relativeDir);
    }
  }

  /**
   * Gets all of the app level files and 
   * saves them to the specified directory
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param dirToSaveDataTo the directory in which the data will be saved
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void getAllAppLevelFilesFromUri(String uri, String appId, String dirToSaveDataTo)
      throws Exception {
    String relativeDir;
    JSONObject file;

    // Get all App Level Files
    JSONObject app = getManifestForAppLevelFiles(uri, appId);
    JSONArray appLevelFiles = app.getJSONArray("files");

    for (int i = 0; i < appLevelFiles.size(); i++) {
      file = appLevelFiles.getJSONObject(i);
      relativeDir = file.getString("filename");
      String pathToSaveFile = dirToSaveDataTo + separator + relativeDir;
      downloadFile(uri, appId, pathToSaveFile, relativeDir);
    }
  }

  /**
   * Gets the manifest for the app level files
   * and returns that list in a JSONObject
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @return JSONObject of the list of app level files
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getManifestForAppLevelFiles(String uri, String appId) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
      //RestClient restClient = new RestClient();
      String agg_uri = uri + separator + appId + uriManifest;
      System.out.println("getManifestForAppLevelFiles: agg_uri is " + agg_uri);
      
      //Resource tableResource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
         response = httpClient.execute(request);
      }
    
      //String tableRes = tableResource.accept("application/json").get(String.class);
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String res = strLine.toString();
    
      
      //obj = new JSONObject(tableRes);
      obj = new JSONObject(res);
      System.out.println("getManifestForAppLevelFiles: result is " + obj.toString());

    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  }

  /**
   * Uploads a file to the server.  App level or table
   * level files can be uploaded via this method.  The only 
   * difference between the two is the relative path on the server.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param wholePathToFile the file path for the file to upload
   * @param relativePathOnServer the relative path on the server where
   * the file will be stored
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void uploadFile(String uri, String appId, String wholePathToFile,
      String relativePathOnServer) throws Exception {
    
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
      String uriRelativePath = relativePathOnServer.replaceAll(File.separator + File.separator, separator);
      String agg_uri = uri + separator + appId + uriFilesFragment + uriRelativePath;
      System.out.println("uploadFile: agg uri is " + agg_uri);
  
      File file = new File(wholePathToFile);
      if (!file.exists()) {
        System.out.println("uploadFile: file " + wholePathToFile + " does not exist");
        return;
      }
  
      //InputStream in = new FileInputStream(file);
      byte[] data = Files.readAllBytes(file.toPath());
  
      // create the rest client instance
      //RestClient client = new RestClient();
  
      // create the resource instance to interact with
      //Resource resource = client.resource(agg_uri);
      request = new HttpPost(agg_uri);
  
      // issue the request
      String contentType = this.determineContentType(file.getName());
      //InputStream response = resource.contentType(contentType).accept(contentType)
      //    .post(InputStream.class, in);
      request.addHeader("content-type", contentType + "; charset=utf-8");
      request.addHeader("accept", contentType);
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      HttpEntity entity = new ByteArrayEntity(data);
      request.setEntity(entity);
      
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      System.out.println("uploadFile: response for file " + wholePathToFile + " is ");
  
      BufferedReader responseBuff = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), 
          Charset.forName("UTF-8")));
      String line;
      while ((line = responseBuff.readLine()) != null)
        System.out.println(line);
      //in.close();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

  }

  /**
   * Downloads a file to the server.  App level or table
   * level files can be downloaded via this method.  The only 
   * difference between the two is the relative path on the server.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param pathToSaveFile the file path where the file should be downloaded
   * @param relativePathOnServer the relative path on the server where
   * the file is stored
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void downloadFile(String uri, String appId, String pathToSaveFile,
      String relativePathOnServer) throws Exception {

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
      String agg_uri = uri + separator + appId + uriFilesFragment + relativePathOnServer;
      System.out.println("downloadFile: agg_uri is " + agg_uri);
  
      // File to save
      File file = new File(pathToSaveFile);
  
      // create the rest client instance
      //RestClient client = new RestClient();
  
      // create the resource instance to interact with
      //Resource resource = client.resource(agg_uri);
      request = new HttpGet(agg_uri);
  
      String accept = determineContentType(file.getName());
      request.addHeader("content-type", accept + "; charset=utf-8");
      request.addHeader("accept", accept);
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      //InputStream fis = resource.accept(accept).get(InputStream.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
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
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Deletes an app level file or a table level file 
   * off the server
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param relativePathOnServer the relative path on the server where
   * the file is stored
   * @throws Exception any exception encountered during this function
   */
  public void deleteFile(String uri, String appId, String relativePathOnServer) throws Exception {
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
      String agg_uri = uri + separator + appId + uriFilesFragment + relativePathOnServer;
      System.out.println("deleteFile: agg_uri is " + agg_uri);
  
      //RestClient restClient = new RestClient();
  
      //Resource resource = restClient.resource(agg_uri);
      request = new HttpDelete(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
  
      //ClientResponse response = resource.accept("application/json").delete();
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      System.out.println("deleteFile: client response is " + response.getStatusLine().getStatusCode() + ":" +
          response.getStatusLine().getReasonPhrase());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Returns a list of tables currently on the
   * server in a JSONObject
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @return a JSONObject with the list of tables
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getTables(String uri, String appId) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment;

    System.out.println("getTables: agg uri is " + agg_uri);

    //Resource resource = restClient.resource(agg_uri);
    request = new HttpGet(agg_uri);
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");

    //String res = resource.accept("application/json").get(String.class);
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();

    obj = new JSONObject(res);

    System.out.println("getTables: result is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns table data for the specified tableId
   * in a JSONObject
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @return a JSONObject with the representation of the table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getTable(String uri, String appId, String tableId) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId;
    System.out.println("getTable: agg uri is " + agg_uri);

    //Resource resource = restClient.resource(agg_uri);
    request = new HttpGet(agg_uri);
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");
    
    //String res = resource.accept("application/json").get(String.class);
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();

    obj = new JSONObject(res);

    System.out.println("getTable: result is for tableId " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    return obj;
  }
  
  /**
   * Returns table data eTag for the specified tableId
   * in a JSONObject
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @return a JSONObject with the representation of the table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public String getTableDataETag(String uri, String appId, String tableId) throws Exception {
    String dataETag = null;
    
    JSONObject tableRes = getTable(uri, appId, tableId);
    dataETag = (tableRes.has(jsonDataETag) && !tableRes.isNull(jsonDataETag)) ? tableRes.getString(jsonDataETag) : null;

    System.out.println("getTableDataETag: result is for tableId " + tableId + " is " + dataETag);

    return dataETag;
  }

  /**
   * Creates a table on the server with the specified
   * tableId and columns.  schemaETag should be null if
   * a new table is being created.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param columns an ArrayList of Column objects which define the columns of the table
   * @return a JSONObject with the representation of the newly created table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject createTable(String uri, String appId, String tableId, String schemaETag,
      ArrayList<Column> columns) throws Exception {
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
      //RestClient restClient = new RestClient();
  
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId;
      System.out.println("createTable: agg_uri is " + agg_uri);
  
      // Add the columns to the table object
      for (int i = 0; i < columns.size(); i++) {
        col = new JSONObject();
        col.put("elementKey", columns.get(i).getElementKey());
        col.put("elementName", columns.get(i).getElementName());
        col.put("elementType", columns.get(i).getElementType());
        col.put("listChildElementKeys", columns.get(i).getListChildElementKeys());
        cols.add(col);
      }
  
      tableObj.put("schemaETag", schemaETag);
      tableObj.put("tableId", tableId);
      tableObj.put("orderedColumns", cols);
  
      System.out.println("createTable: with object " + tableObj.toString());
  
      //Resource resource = restClient.resource(agg_uri);
      request = new HttpPut(agg_uri);
      StringEntity params = new StringEntity(tableObj.toString(), "UTF-8");
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      request.setEntity(params);
      
      //String res = resource.accept("application/json").contentType("application/json")
      //    .put(String.class, tableObj.toString());
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String res = strLine.toString();
  
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
   * Creates a table on the server with the specified
   * tableId and JSONObject.  schemaETag should be null if
   * a new table is being created.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param jsonTableCreationObject a jsonObject that holds the values used to create a table definition
   * @return a JSONObject with the representation of the newly created table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject createTableWithJSON(String uri, String appId, String tableId, String schemaETag,
      String jsonTableCreationObject) throws Exception {
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
      throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject cannot be null");
    }

    HttpPut request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId;
    System.out.println("createTableWithJSON: agg_uri is " + agg_uri);
    
    tableObj = new JSONObject(jsonTableCreationObject);
    
    if (!tableObj.containsKey("schemaETag")) {
      throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does not have schemaETag");
    }

    if (!tableObj.containsKey("tableId")) {
      throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does not have tableId");
    }
    
    if (!tableObj.containsKey("orderedColumns")) {
      throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does not have orderedColumns");
    }
    
    JSONArray columns = tableObj.getJSONArray("orderedColumns");
    
    // Add the columns to the table object
    for (int i = 0; i < columns.size(); i++) {
      col = columns.getJSONObject(i);
      
      if (!col.containsKey("elementKey")) {
        throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does orderedColumns " + i + " does not have elementKey");
      }
      
      if (!col.containsKey("elementName")) {
        throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does orderedColumns " + i + " does not have elementName");
      }
      
      if (!col.containsKey("elementType")) {
        throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does orderedColumns " + i + " does not have elementType");
      }
      
      if (!col.containsKey("listChildElementKeys")) {
        throw new IllegalArgumentException("createTableWithJSON: jsonTableCreationObject does orderedColumns " + i + " does not have listChildElementKeys");
      }
    }

    System.out.println("createTableWithJSON: with object " + tableObj.toString());

    //Resource resource = restClient.resource(agg_uri);
    request = new HttpPut(agg_uri);
    StringEntity params = new StringEntity(tableObj.toString(), "UTF-8");
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");
    request.setEntity(params);
    
    //String res = resource.accept("application/json").contentType("application/json")
    //    .put(String.class, tableObj.toString());
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();

    System.out.println("createTableWithJSON: result is for tableId " + tableId + " is " + res.toString());

    result = new JSONObject(res);
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return result;
  }

  /**
   * Creates a table on the server with the specified
   * tableId using a csv file which specifies the table
   * definition.  
   * schemaETag should be null if a new table is being 
   * created.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvFilePath file path to the definition.csv file 
   * @return a JSONObject with the representation of the newly created table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject createTableWithCSV(String uri, String appId, String tableId, String schemaETag,
      String csvFilePath) throws Exception {
    ArrayList<Column> cols = new ArrayList<Column>();
    RFC4180CsvReader reader;

    File file = new File(csvFilePath);
    if (!file.exists()) {
      throw new IllegalArgumentException("createTableWithCSV: file " + csvFilePath + " does not exist");
    }
    InputStream in = new FileInputStream(file);
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName("UTF-8"));
    reader = new RFC4180CsvReader(inputStream);

    return createTableWithCSVProcessing(uri, appId, tableId, schemaETag, cols, reader);
  }
  
  /**
   * Creates a table on the server with the specified
   * tableId using a file input stream.  
   * schemaETag should be null if a new table is being 
   * created.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvInputStream input stream for a table definition csv file 
   * @return a JSONObject with the representation of the newly created table
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject createTableWithCSVInputStream(String uri, String appId, String tableId, String schemaETag,
      InputStream csvInputStream) throws Exception {
    ArrayList<Column> cols = new ArrayList<Column>();
    RFC4180CsvReader reader;

    if (csvInputStream.available() <= 0) {
      throw new IllegalArgumentException("createTableWithCSVInputStream: csvInputStream is not available");
    }
    
    InputStream in = csvInputStream;
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName("UTF-8"));
    reader = new RFC4180CsvReader(inputStream);

    return createTableWithCSVProcessing(uri, appId, tableId, schemaETag, cols, reader);
  }

  private JSONObject createTableWithCSVProcessing(String uri, String appId, String tableId,
      String schemaETag, ArrayList<Column> cols, RFC4180CsvReader reader) throws IOException,
      DataFormatException, Exception {
    Column col;
    JSONObject resultingTable;
    // Make sure that the first line of the csv file
    // has the right header
    String[] firstLine = reader.readNext();

    if (firstLine.length != 4) {
      throw new DataFormatException(
          "The csv file used to create a table does not have the correct number of columns");
    }

    // Make sure that the first row of the csv file
    // has the right columns
    if (!firstLine[0].equals(tableDefElemKey) || !firstLine[1].equals(tableDefElemName)
        || !firstLine[2].equals(tableDefElemType)
        || !firstLine[3].equals(tableDefListChildElemKeys)) {
      throw new DataFormatException(
          "The csv file used to create a table does not have the correct columns in the first row");
    }

    String[] line;

    while ((line = reader.readNext()) != null) {
      col = new Column(line[0], line[1], line[2], line[3]);
      cols.add(col);
    }

    resultingTable = createTable(uri, appId, tableId, schemaETag, cols);

    reader.close();

    return resultingTable;
  }

  /**
   * Writes out the table definition for the 
   * specified tableId to a csv file
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvFilePath file path in which to save the table definition
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void writeTableDefinitionToCSV(String uri, String appId, String tableId,
      String schemaETag, String csvFilePath) throws Exception {
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
    JSONArray orderedColumns = tableDef.getJSONArray(orderedColumnsDef);
    String[] colArray = new String[4];

    // Create the first row of the csv
    colArray[0] = tableDefElemKey;
    colArray[1] = tableDefElemName;
    colArray[2] = tableDefElemType;
    colArray[3] = tableDefListChildElemKeys;

    writer.writeNext(colArray);

    for (int i = 0; i < orderedColumns.size(); i++) {
      JSONObject col = orderedColumns.getJSONObject(i);
      colArray[0] = col.getString(jsonElemKey);
      colArray[1] = col.getString(jsonElemName);
      colArray[2] = col.getString(jsonElemType);
      colArray[3] = col.getString(jsonListChildElemKeys);
      writer.writeNext(colArray);
    }
    writer.close();

  }

  /**
   * Gets the table definition and returns a
   * JSONObject for the specified tableId and 
   * schemaETag
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @return a JSONObject with the table definition
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getTableDefinition(String uri, String appId, String tableId, String schemaETag)
      throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag;
    System.out.println("getTableDefinition: agg uri is " + agg_uri);

    //Resource resource = restClient.resource(agg_uri);
    request = new HttpGet(agg_uri);
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");
    
    //String res = resource.accept("application/json").contentType("application/json")
    //    .get(String.class);
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();

    obj = new JSONObject(res);

    System.out.println("getTableDefinition: result is for tableId " + tableId + " and schemaEtag "
        + schemaETag + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Deletes the table definition for a specified
   * tableId and schemaETag 
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @throws Exception any exception encountered during this function
   */
  public void deleteTableDefinition(String uri, String appId, String tableId, String schemaETag) throws Exception {
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpDelete request = null;
    try {
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag;
  
      //RestClient restClient = new RestClient();
  
      System.out.println("deleteTableDefinition: agg_uri is " + agg_uri);
      //Resource resource = restClient.resource(agg_uri);
      request = new HttpDelete(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
  
      //ClientResponse response = resource.accept("application/json").delete();
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      System.out.println("deleteTableDefinition: client response is " + 
          response.getStatusLine().getStatusCode() + ":" + 
          response.getStatusLine().getReasonPhrase());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

  }

  /**
   * Returns a JSONObject with the list of 
   * table level files for a given tableId
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @return a JSONObject with the list of table level files
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getManifestForTableId(String uri, String appId, String tableId)
      throws Exception {
    JSONObject obj = null;

    HttpGet request = null;
    try {
      //RestClient restClient = new RestClient();
  
      String agg_uri = uri + separator + appId + uriManifest + separator + tableId;
      System.out.println("getManifestForTableId: agg uri is " + agg_uri);
  
      //Resource resource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
  
      //String res = resource.accept("application/json").get(String.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String res = strLine.toString();
  
      obj = new JSONObject(res);
      System.out.println("getTableIdManifest: result for " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject of the rows that can 
   * be found in a table since the given
   * dataETag for the specified tableId and 
   * schemaETag
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param cursor query parameter that identifies the point at which to 
   * resume the query
   * @param fetchLimit query parameter that defines the number of rows to 
   * return
   * @param dataETag query parameter that defines the point at which 
   * to start getting rows
   * @return a JSONObject with the row data
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getRowsSince(String uri, String appId, String tableId, String schemaETag,
      String cursor, String fetchLimit, String dataETag) throws Exception {
    JSONObject obj = null;
    boolean useCursor = false;
    boolean useFetchLimit = false;
    boolean useDataETag = false;

    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }
    
    HttpGet request = null;
    try {
      //RestClient restClient = new RestClient();
  
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriRowsFragment;
  
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
        agg_uri = agg_uri + queryParamCursor + cursor;
        if (useFetchLimit || useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }
  
      if (useFetchLimit) {
        agg_uri = agg_uri + queryParamFetchLimit + fetchLimit;
        if (useDataETag) {
          agg_uri = agg_uri + "&";
        }
      }
  
      if (useDataETag) {
        agg_uri = agg_uri + queryParamDataETag + dataETag;
      }
  
      //Resource tableResource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      System.out.println("getRowsSince: agg uri is " + agg_uri);
  
      //String tableRes = tableResource.accept("application/json").get(String.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String tableRes = strLine.toString();
      
      obj = new JSONObject(tableRes);
      System.out.println("getRowsSince: result for " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject of the rows in a table 
   * for the specified tableId and schemaETag
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param cursor query parameter that identifies the point at which to 
   * resume the query
   * @param fetchLimit query parameter that defines the number of rows to 
   * return
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
      
      //RestClient restClient = new RestClient();

      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriRowsFragment;

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
        agg_uri = agg_uri + queryParamFetchLimit + fetchLimit;
        if (useCursor)
          agg_uri = agg_uri + "&";
      }

      if (useCursor) {
        agg_uri = agg_uri + queryParamCursor + cursor;
      }

      //Resource tableResource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      System.out.println("getRows: agg uri is " + agg_uri);

      //String tableRes = tableResource.accept("application/json").get(String.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String tableRes = strLine.toString();
      
      obj = new JSONObject(tableRes);
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
   * Writes out the row data for a given
   * tableId and schemaETag to a csv
   * file
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvFilePath the csv file path in which to write the row data
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void writeRowDataToCSV(String uri, String appId, String tableId, String schemaETag,
      String csvFilePath) throws Exception {
	  
    RFC4180CsvWriter writer;
    JSONObject rowWrapper;
    String resumeCursor = null;

    rowWrapper = getRows(uri, appId, tableId, schemaETag, resumeCursor, defaultFetchLimit);

    JSONArray rows = rowWrapper.getJSONArray(jsonRowsString);
    
    if (rows.size() <= 0) {
    	System.out.println("writeRowDataToCSV: There are no rows to write out!");
    	return;
    }
    
    File file = new File(csvFilePath);
    if (!file.exists()) {
      file.createNewFile();
    }
    
    // This fileWriter could be causing the issue with
    // UTF-8 characters - should probably use an OutputStream
    // here instead
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    writer = new RFC4180CsvWriter(fw);

    JSONObject repRow = rows.getJSONObject(0);
    JSONArray orderedColumnsRep = repRow.getJSONArray(orderedColumnsDef);
    int numberOfColsToMake = 9 + orderedColumnsRep.size();
    String[] colArray = new String[numberOfColsToMake];

    int i = 0;
    colArray[i++] = rowDefId;
    colArray[i++] = rowDefFormId;
    colArray[i++] = rowDefLocale;
    colArray[i++] = rowDefSavepointType;
    colArray[i++] = rowDefSavepointTimestamp;
    colArray[i++] = rowDefSavepointCreator;

    for (int j = 0; j < orderedColumnsRep.size(); j++) {
      JSONObject obj = orderedColumnsRep.getJSONObject(j);
      colArray[i++] = obj.getString("column");
    }

    colArray[i++] = rowDefRowETag;
    colArray[i++] = rowDefFilterType;
    colArray[i++] = rowDefFilterValue;

    writer.writeNext(colArray);

    do {
      rowWrapper = getRows(uri, appId, tableId, schemaETag, resumeCursor, defaultFetchLimit);

      rows = rowWrapper.getJSONArray(jsonRowsString);
      
      writeOutFetchLimitRows(writer, rows, colArray);
      
      resumeCursor = rowWrapper.getString("webSafeResumeCursor");
      
    } while (rowWrapper.getBoolean("hasMoreResults"));

    writer.close();
  }

  private void writeOutFetchLimitRows(RFC4180CsvWriter writer, JSONArray rows,
      String[] colArray) throws JSONException, IOException {
    int i;
    String nullString = null;
   
    for (int k = 0; k < rows.size(); k++) {
      i = 0;
      JSONObject row = rows.getJSONObject(k);
      colArray[i++] = row.getString(jsonId);
      String formId = nullString;
      if (!row.isNull(jsonFormId)) {
        formId = row.getString(jsonFormId);
      }
      colArray[i++] = formId;
      colArray[i++] = row.getString(jsonLocale);
      colArray[i++] = row.getString(jsonSavepointType);
      colArray[i++] = row.getString(jsonSavepointTimestamp);
 
      String creator = nullString;
      if (!row.isNull(jsonSavepointCreator)) {
        creator = row.getString(jsonSavepointCreator);
      }
      colArray[i++] = creator;
 
      JSONArray rowsOrderedCols = row.getJSONArray(orderedColumnsDef);
 
      for (int l = 0; l < rowsOrderedCols.size(); l++) {
        JSONObject col = rowsOrderedCols.getJSONObject(l);
        if (col.isNull("value")) {
          colArray[i++] = nullString;
        } else {
          colArray[i++] = col.getString("value");
        }
      }
 
      colArray[i++] = nullString;
      JSONObject filterScope = row.getJSONObject(jsonFilterScope);
      colArray[i++] = filterScope.getString("type");
      if (filterScope.isNull("value")) {
        colArray[i++] = nullString;
      } else {
        colArray[i++] = filterScope.getString("value");
      }
 
      writer.writeNext(colArray);
    }
  }

  /**
   * Returns a JSONObject with the row data
   * for the specified tableId, schemaETag, and
   * rowId
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param rowId the unique id for a row in the table
   * @return a JSONObject that contains the row data
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getRow(String uri, String appId, String tableId, String schemaETag, String rowId)
      throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
      //RestClient restClient = new RestClient();
  
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriRowsFragment + separator + rowId;
      System.out.println("getRow: agg uri is " + agg_uri);
      //Resource tableResource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
  
      //String tableRes = tableResource.accept("application/json").get(String.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String tableRes = strLine.toString();
      
      obj = new JSONObject(tableRes);
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
   * Converts a given rowId to a String
   * that has only alphanumeric characters
   * and underscores.  This is useful for 
   * creating a directory name in which to 
   * store instance files or row level
   * attachments.  
   * 
   * @param rowId the unique identifier for the row
   * @return a String with only alphanumeric characters and underscores
   */
  public static String convertRowIdForInstances(String rowId) {
    // This is similar to ODKFileUtils.getInstanceFolder
    // in AndroidCommon, but I didn't want to include android
    // specific code.
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
   * Returns a JSONObject with the 
   * attachments for a given row in a
   * specified table
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param rowId the unique identifier for a row in the table
   * @return a JSONObject with the attachments for the row
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject getManifestForRow(String uri, String appId, String tableId, String schemaETag,
      String rowId) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    HttpGet request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriAttachmentsFragment + rowId + uriManifestFragment;
    System.out.println("getManifestForRow: agg uri is " + agg_uri);

    //Resource resource = restClient.resource(agg_uri);
    request = new HttpGet(agg_uri);
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");

    //String res = resource.accept("application/json").contentType("application/json").get(String.class);
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String res = strLine.toString();
    
    obj = new JSONObject(res);
    
    System.out.println("getManifestForRow: result for " + tableId + " with rowId " + rowId + " is "
        + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }

    return obj;
  }

  /**
   * Returns a JSONObject representation
   * for a Row object
   * 
   * @param row a Row object
   * @return a JSONObject representation of row
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject mapRowToJSONObject(Row row) throws Exception {
    JSONObject rowObj = new JSONObject();
    JSONObject col;
    JSONArray cols = new JSONArray();
    JSONObject filterScope = new JSONObject();
    String nullString = null;

    String timeStamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());

    ArrayList<DataKeyValue> dkvl = row.getValues();
    for (int i = 0; i < dkvl.size(); i++) {
      DataKeyValue dkv = dkvl.get(i);
      String key = dkv.column;
      String val = dkv.value;
      col = new JSONObject();
      col.put("column", key);
      col.put("value", val);
      cols.add(col);
    }

    filterScope.put("type", "DEFAULT");
    filterScope.put("value", nullString);

    rowObj.put("rowETag", row.getRowETag());

    rowObj.put("id", row.getRowId());
    rowObj.put("deleted", "false");
    rowObj.put("formId", row.getFormId());

    String locale = row.getLocale();
    if (locale == null || locale.isEmpty()) {
      rowObj.put("locale", Locale.ENGLISH.getLanguage());
    } else {
      rowObj.put("locale", locale);
    }

    String savepointType = row.getSavepointType();
    if (savepointType == null || savepointType.isEmpty()) {
      rowObj.put("savepointType", SavepointTypeManipulator.complete());
    } else {
      rowObj.put("savepointType", savepointType);
    }

    String savepointTimestamp = row.getSavepointTimestamp();
    if (savepointTimestamp == null || savepointTimestamp.isEmpty()) {
      rowObj.put("savepointTimestamp", timeStamp);
    } else {
      rowObj.put("savepointTimestamp", savepointTimestamp);
    }

    rowObj.put("savepointCreator", row.getSavepointCreator());

    rowObj.put("filterScope", filterScope);

    rowObj.put("orderedColumns", cols);

    return rowObj;
  }

  public void mapUserCSVToProprietaryCSV(String pathToUserCSV, String pathToCSV) {

  }
   
  /**
   * Creates rows in the table associated with
   * the tableId and schemaETag using bulk upload
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param rowArrayList an ArrayList of rows to create
   * @param batchSize used to set the batch size of rows sent to the server - 
   * if 0 is passed in, the default of 500 is used 
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void createRowsUsingBulkUpload(String uri, String appId, String tableId, String schemaETag,
      ArrayList<Row> rowArrayList, int batchSize) throws Exception {
      
    // Default values for rows
    String rowId = null;
    String locale = Locale.ENGLISH.getLanguage();
    String savepointType = SavepointTypeManipulator.complete();
    String savepointTimestamp = null;
    String savepointCreator = "anonymous";
    Scope defaultScope = Scope.EMPTY_SCOPE;
    String dataETag = null;

    if (batchSize == 0) {
      batchSize = 500;
    }
    
    // No Row Id for bulk upload
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;

    ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

    for (int i = 0; i < rowArrayList.size(); i++) {
      Row row = rowArrayList.get(i);
      Row rowObj = Row.forInsert(row.getRowId(), row.getFormId(), row.getLocale(), row.getSavepointType(), 
          row.getSavepointTimestamp(), row.getSavepointCreator(), row.getFilterScope(), row.getValues());
      
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
        savepointTimestamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
        rowObj.setSavepointTimestamp(savepointTimestamp);
      }
      
      if (rowObj.getSavepointCreator() == null || rowObj.getSavepointCreator().length() == 0) {
        rowObj.setSavepointCreator(savepointCreator);
      }
      
      if (rowObj.getFilterScope() == null) {
        rowObj.setFilterScope(defaultScope);
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
   * Creates rows in the table associated with
   * the tableId and schemaETag using a JSONArray
   * of rows.  This function should be used
   * when there are thousands of rows of data to
   * upload. 
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param jsonRows a JSONArray of rows to insert into the database
   * @param batchSize the number of rows that will be uploaded to the server at one time
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void createRowsUsingJSONBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String jsonRows, int batchSize) throws Exception {
    
    // Default values for rows
    String rowId = "uuid:" + UUID.randomUUID().toString();
    String formId = null;
    String locale = Locale.ENGLISH.getLanguage();
    String savepointType = SavepointTypeManipulator.complete();
    String savepointTimestamp = TableConstants.nanoSecondsFromMillis(System.currentTimeMillis());
    String savepointCreator = "anonymous";
    Scope defaultScope = Scope.EMPTY_SCOPE;
    String dataETag = null;

    if (batchSize == 0) {
      batchSize = 500;
    }

    JSONObject rowWrapperObj = new JSONObject(jsonRows);
    JSONArray rowsObj = rowWrapperObj.getJSONArray(jsonRowsString);
    JSONObject rowObj = null;
    
    // No Row Id for bulk upload
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;

    ArrayList<Row> rowArrayList = new ArrayList<Row>();

    for (int i = 0; i < rowsObj.size(); i++) {
      rowObj = rowsObj.getJSONObject(i);

      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();

      JSONArray orderedCols = rowObj.getJSONArray(orderedColumnsDef);
      
      for (int j = 0; j < orderedCols.size(); j++) {
        JSONObject col = orderedCols.getJSONObject(j);
        DataKeyValue dkv = new DataKeyValue(col.getString("column"), col.getString("value"));
        dkvl.add(dkv);
      }
      
      if (rowObj.containsKey(jsonId)) {
        rowId = rowObj.getString(jsonId);
      }
      
      if (rowObj.containsKey(jsonFormId)) {
        formId = rowObj.getString(jsonFormId);
      }
      
      if (rowObj.containsKey(jsonLocale)) {
        locale = rowObj.getString(jsonLocale);
      }
      
      if (rowObj.containsKey(jsonSavepointType)) {
        savepointType = rowObj.getString(jsonSavepointType);
      }
      
      if (rowObj.containsKey(jsonSavepointTimestamp)) {
        savepointTimestamp = rowObj.getString(jsonSavepointTimestamp);
      }
      
      if (rowObj.containsKey(jsonSavepointCreator)) {
        savepointCreator = rowObj.getString(jsonSavepointCreator);
      }
      
      if (rowObj.containsKey(jsonFilterScope)) {
        JSONObject filterObj = rowObj.getJSONObject(jsonFilterScope);
        defaultScope = Scope.asScope(filterObj.getString("type"), filterObj.getString("value"));
      }
      
      Row row = Row.forInsert(rowId, formId, locale, savepointType, savepointTimestamp, savepointCreator, defaultScope, dkvl);

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
   * Creates rows in the table associated with
   * the tableId and schemaETag using a CSV file 
   * in batches.  This function should be used
   * when there are thousands of rows of data to
   * upload. 
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvFilePath the file path from which to retrieve the row data
   * @param batchSize the number of rows that will be uploaded to the server at one time
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void createRowsUsingCSVBulkUpload(String uri, String appId, String tableId,
      String schemaETag, String csvFilePath, int batchSize) throws Exception {
    RFC4180CsvReader reader;
    
    File file = new File(csvFilePath);
    if (!file.exists()) {
      System.out.println("createRowsUsingCSVBulkUpload: file " + csvFilePath + " does not exist");
    }

    InputStream in = new FileInputStream(file);
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName("UTF-8"));
    reader = new RFC4180CsvReader(inputStream);

    createRowsUsingCSVBulkUploadProcessing(uri, appId, tableId, schemaETag, batchSize, reader);
  }
  
  /**
   * Creates rows in the table associated with
   * the tableId and schemaETag using an input stream 
   * in batches.  This function should be used
   * when there are thousands of rows of data to
   * upload. 
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param csvInputStream the input stream from which to retrieve the row data
   * @param batchSize the number of rows that will be uploaded to the server at one time
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void createRowsUsingCSVInputStreamBulkUpload(String uri, String appId, String tableId,
      String schemaETag, InputStream csvInputStream, int batchSize) throws Exception {
    RFC4180CsvReader reader;
    
    if (csvInputStream.available() <= 0) {
      throw new IllegalArgumentException("createTableWithCSVInputStream: csvInputStream is not available");
    }
    
    InputStream in = csvInputStream;
    InputStreamReader inputStream = new InputStreamReader(in, Charset.forName("UTF-8"));
    reader = new RFC4180CsvReader(inputStream);

    createRowsUsingCSVBulkUploadProcessing(uri, appId, tableId, schemaETag, batchSize, reader);
  }

  private void createRowsUsingCSVBulkUploadProcessing(String uri, String appId, String tableId,
      String schemaETag, int batchSize, RFC4180CsvReader reader) throws IOException,
      DataFormatException, JsonProcessingException, UnsupportedEncodingException,
      ClientProtocolException {
    
    String dataETag = null;
    
    if (batchSize == 0) {
      batchSize = 500;
    }

    // No Row Id for bulk upload
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;

    // Make sure that the first line of the csv file
    // has the right header
    String[] firstLine = reader.readNext();
    int numOfCols = firstLine.length;

    // Make sure that the first row of the csv file
    // has the right columns
    if (!firstLine[0].equals(rowDefId) || !firstLine[1].equals(rowDefFormId)
        || !firstLine[2].equals(rowDefLocale) || !firstLine[3].equals(rowDefSavepointType)
        || !firstLine[4].equals(rowDefSavepointTimestamp)
        || !firstLine[5].equals(rowDefSavepointCreator)) {
      throw new DataFormatException(
          "The csv file used to create rows does not have the correct columns in the first row");
    }

    // Make sure that the first row of the csv file
    // has the right columns
    if (!firstLine[numOfCols - 3].equals(rowDefRowETag)
        || !firstLine[numOfCols - 2].equals(rowDefFilterType)
        || !firstLine[numOfCols - 1].equals(rowDefFilterValue)) {
      throw new DataFormatException(
          "The csv file used to create rows does not have the correct columns in the first row");
    }

    try {
      String[] line;
      ArrayList<Row> rowArrayList = new ArrayList<Row>();
      line = reader.readNext();
      while (line != null) {
  
        if (line.length == 0) {
          line = reader.readNext();
          continue;
        }
  
        ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
  
        for (int i = 6; i < numOfCols - 3; i++) {
          DataKeyValue dkv = new DataKeyValue(firstLine[i], line[i]);
          dkvl.add(dkv);
        }
  
        Row row = Row.forInsert(line[0], line[1], line[2], line[3], line[4], line[5], null, dkvl);
  
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private RowOutcomeList bulkRowsSender(ArrayList<Row> rowArrayList, String agg_uri, String tableId, String dataETag, 
      boolean print) throws JsonProcessingException, UnsupportedEncodingException, IOException,
      ClientProtocolException, JSONException {
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
      
      //RestClient restClient = new RestClient();
  
      //Resource resource = restClient.resource(agg_uri);
      request = new HttpPut(agg_uri);
      StringEntity params = new StringEntity(rowRes, "UTF-8");
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      request.setEntity(params);
      
      //String res = resource.accept("application/json").contentType("application/json")
      //    .put(String.class, rowRes);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String res = strLine.toString();
      
      if (print) {
        System.out.println("bulkRowsSender: result with tableId " + tableId + " is "
            + res);
        
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
   * Update row(s) in the table associated with
   * the tableId and schemaETag using bulk upload
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param dataETagVal identifies the last change of the table
   * @param rowArrayList an ArrayList of rows to create
   * @param batchSize used to set the batch size of rows sent to the server - 
   * if 0 is passed in, the default of 500 is used 
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void updateRowsUsingBulkUpload(String uri, String appId, String tableId, String schemaETag, String dataETagVal, 
      ArrayList<Row> rowArrayList, int batchSize) throws Exception{
    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (!dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }
    
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;
    System.out.println("updateRowsUsingBulkUpload: agg_uri is " + agg_uri);
    
    if (batchSize == 0) {
      batchSize = 500;
    }
    
    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(), row.getLocale(), row.getSavepointType(), 
            row.getSavepointTimestamp(), row.getSavepointCreator(), row.getFilterScope(), row.getValues());
        
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
   * Deletes row(s) in the table associated with
   * the tableId and schemaETag using bulk upload
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param dataETagVal identifies the last change of the table
   * @param rowArrayList an ArrayList of rows to create
   * @param batchSize used to set the batch size of rows sent to the server - 
   * if 0 is passed in, the default of 500 is used 
   * @throws Exception any exception encountered is thrown to the caller
   */
  public void deleteRowsUsingBulkUpload(String uri, String appId, String tableId, String schemaETag, String dataETagVal, 
      ArrayList<Row> rowArrayList, int batchSize) throws Exception{
    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (!dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }
    
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;
    System.out.println("deleteRowsUsingBulkUpload: agg_uri is " + agg_uri);
    
    if (batchSize == 0) {
      batchSize = 500;
    }
    
    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(), row.getLocale(), row.getSavepointType(), 
            row.getSavepointTimestamp(), row.getSavepointCreator(), row.getFilterScope(), row.getValues());
        
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
   * Alters row(s) in the table associated with
   * the tableId and schemaETag for one batch - the size 
   * of the batch should not exceed 500.
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param dataETagVal identifies the last change of the table
   * @param rowArrayList an ArrayList of rows to create
   * @return a RowOutcomeList with the row outcome and row data if applicable
   * @throws Exception any exception encountered is thrown to the caller
   */
  public RowOutcomeList alterRowsUsingSingleBatch(String uri, String appId, String tableId, String schemaETag, String dataETagVal, 
      ArrayList<Row> rowArrayList) throws Exception{
    RowOutcomeList outcome = new RowOutcomeList();
    
    String dataETag = getTableDataETag(uri, appId, tableId);

    // Check that the dataETag is valid before beginning
    if (!dataETag.equals(dataETagVal)) {
      throw new IllegalArgumentException("The dataETag supplied is not correct");
    }
    
    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriRowsFragment;
    System.out.println("alterRowsUsingSingleBatch: agg_uri is " + agg_uri);
    
    try {
      ArrayList<Row> processedRowArrayList = new ArrayList<Row>();

      for (int i = 0; i < rowArrayList.size(); i++) {
        Row row = rowArrayList.get(i);
        Row rowObj = Row.forUpdate(row.getRowId(), row.getRowETag(), row.getFormId(), row.getLocale(), row.getSavepointType(), 
            row.getSavepointTimestamp(), row.getSavepointCreator(), row.getFilterScope(), row.getValues());
        
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
   * Get the file attachment and save it to 
   * the specified file for a given row of a
   * table.   
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param userRowId the unique identifier for a row
   * @param asAttachment false to save the data as a file, true to download the data 
   * @param pathToSaveFile file path in which to save the attachment
   * @param relativePathOnServer the path on the server where the attachment resides
   * @throws Exception any exception encountered is thrown to the caller
   * 
   */
  public void getFileForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, boolean asAttachment, String pathToSaveFile, String relativePathOnServer)
      throws Exception {
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
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriAttachmentsFragment + userRowId + uriFileFragment
          + relativePathOnServer;
  
      if (asAttachment) {
        agg_uri = agg_uri + uriAsAttachmentFragment;
      }
  
      System.out.println("getFileForRow: agg_uri is " + agg_uri);
  
      File file = new File(pathToSaveFile);
      file.getParentFile().mkdirs();
      if (!file.exists()) {
        file.createNewFile();
      }
  
      // create the rest client instance
      //RestClient client = new RestClient();
  
      // create the resource instance to interact with
      //Resource resource = client.resource(agg_uri);
      request = new HttpGet(agg_uri);
      System.out.println("getFileForRow: agg_uri is " + agg_uri);
  
      String accept = determineContentType(file.getName());
      request.addHeader("content-type", accept + "; charset=utf-8");
      request.addHeader("accept", accept);
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      //InputStream fis = resource.accept(accept).get(InputStream.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
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
   * Get a batch of file attachments and save them to 
   * specified files for a given row of a
   * table.   
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param userRowId the unique identifier for a row
   * @param dirToSaveFiles file path in which to save the attachments
   * @param filesToGet JSONObject of files - same structure as returned in getManifestForRow
   * @param batchSizeInBytes the number of bytes to transfer in a batch default is 10MB
   * @throws Exception any exception encountered is thrown to the caller
   * 
   */
  public void batchGetFilesForRow(String uri, String appId, String tableId, String schemaETag,
	      String userRowId, String dirToSaveFiles, JSONObject filesToGet, int batchSizeInBytes)
	      throws Exception {

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
    
    JSONArray totalFiles = filesToGet.getJSONArray("files");

    JSONArray batchFilesArray = new JSONArray();

    int batchSize = 0;
    for (int i = 0; i < totalFiles.length(); i++) {
      JSONObject file = totalFiles.getJSONObject(i);
      batchSize += file.getInt("contentLength");
      batchFilesArray.add(file);
      
      if (batchSize >= batchSizeToUse) {
    	JSONObject batchFiles = new JSONObject();
    	batchFiles.put("files", batchFilesArray);
        downloadBatchForRow(uri, appId, tableId, schemaETag,
          userRowId, dirToSaveFiles, batchFiles); 
        batchSize = 0;
        batchFilesArray.clear();
      }
    }
    
    if (batchSize > 0) {
      JSONObject batchFiles = new JSONObject();
      batchFiles.put("files", batchFilesArray);
      downloadBatchForRow(uri, appId, tableId, schemaETag,userRowId, dirToSaveFiles, batchFiles); 	
    } 
  }
  
  /**
   * Get a batch of file attachments and save them to 
   * specified files for a given row of a
   * table.   
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param userRowId the unique identifier for a row
   * @param dirToSaveFiles file path in which to save the attachments
   * @param filesToGet JSONObject of files - same structure as returned in getManifestForRow
   * @throws Exception any exception encountered is thrown to the caller
   * 
   */
  public void downloadBatchForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, String dirToSaveFiles, JSONObject filesToGet)
      throws Exception {
    
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
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriAttachmentsFragment + userRowId + downloadFragment;
  
      System.out.println("batchGetFilesForRow: agg_uri is " + agg_uri);
  
      // create the rest client instance
      //RestClient client = new RestClient();
  
      // create the resource instance to interact with
      //Resource resource = client.resource(agg_uri);
      request = new HttpPost(agg_uri);
  
      // Takes json by default - just put a dummy file here for now
      String accept = determineContentType("test.json");
      
      // TBD: Make constants for this!!
      request.addHeader("content-type", accept + "; charset=utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      
      StringEntity params = new StringEntity(filesToGet.toString(), "UTF-8");
      request.setEntity(params);
      
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      System.out.println("batchGetFilesForRow: client response is " + response.getStatusLine().getStatusCode() + ":" +
              response.getStatusLine().getReasonPhrase());
      
      if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() >= 300) {
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
      
      byte[] msParam = boundaryVal.getBytes(Charset.forName("UTF-8"));
      MultipartStream multipartStream = new MultipartStream(inStream, msParam, DEFAULT_BOUNDARY_BUFSIZE, null);

      OutputStream os = null;
      
      // Parse the request
      boolean nextPart = multipartStream.skipPreamble();
      while (nextPart) {
        String header = multipartStream.readHeaders();
        System.out.println("Headers: " + header);
        
        // Get the file name 
        int firstIndex = header.indexOf(multipartFileHeader) + multipartFileHeader.length();
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
          System.out.println("batchGetFilesForRow: Download file batches: Unable to read attachment");
          return;
        } finally {
          if (os != null) {
            try {
              os.close();
            } catch (IOException e) {
              e.printStackTrace();
              System.out.println("batchGetFilesForRow: Download file batches: Error closing output stream");
            }
          }
        }
        nextPart = multipartStream.readBoundary();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }

  /**
   * Upload the file attachment for a given row of a
   * table.  
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param userRowId the unique identifier for a row
   * @param wholePathToFile file path of the file to upload
   * @param relativePathOnServer the path on the server for the attachment 
   * @throws Exception any exception encountered is thrown to the caller
   * 
   */
  public void putFileForRow(String uri, String appId, String tableId, String schemaETag,
      String userRowId, String wholePathToFile, String relativePathOnServer) throws Exception {
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
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriAttachmentsFragment + userRowId + uriFileFragment
          + relativePathOnServer;
      System.out.println("putFileForRow: agg_uri is " + agg_uri);
  
      File file = new File(wholePathToFile);
      if (!file.exists()) {
        System.out.println("putFileForRow: file " + wholePathToFile + " does not exist");
        throw new IllegalArgumentException("putFileForRow: wholePathToFile cannot be null");
      }
  
      //InputStream in = new FileInputStream(file);
      byte[] data = Files.readAllBytes(file.toPath());
  
      // create the rest client instance
      //RestClient client = new RestClient();
  
      // create the resource instance to interact with
      //Resource resource = client.resource(agg_uri);
      request = new HttpPost(agg_uri);
      
      // issue the request
      String contentType = this.determineContentType(file.getName());
      //InputStream response = resource.contentType(contentType).accept(contentType)
      //    .post(InputStream.class, in);
      request.addHeader("content-type", contentType + "; charset=utf-8");
      request.addHeader("accept", contentType);
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      System.out.println("putFileForRow: response for file " + wholePathToFile + " is ");
      
      HttpEntity entity = new ByteArrayEntity(data);
      request.setEntity(entity);
      
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
  
      BufferedReader responseBuff = new BufferedReader(new InputStreamReader(response.getEntity().getContent(),
          Charset.forName("UTF-8")));
      String line;
      while ((line = responseBuff.readLine()) != null)
        System.out.println(line);
      
      //in.close();
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
  }
  
  /**
   * Returns a JSONObject of the rows that can 
   * be found in a table specified tableId and 
   * schemaETag in the range of a specified
   * startTime and endTime using lastUpdateDate
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param startTime a required timestamp used to get all rows with a time greater than or equal to it.  
   * The format of startTime is yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param endTime an optional timestamp used to get all rows with a time less than or equal to it.  
   * The format for endTime is yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param cursor query parameter that identifies the point at which to 
   * resume the query
   * @param fetchLimit query parameter that defines the number of rows to 
   * return
   * @return a JSONObject with the row data
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject queryRowsInTimeRangeWithLastUpdateDate(String uri, String appId, String tableId, String schemaETag,
      String startTime, String endTime, String cursor, String fetchLimit) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (startTime == null || startTime.isEmpty()) {
      throw new IllegalArgumentException("startTime must have a valid value in the format yyyy-MM-dd:HH:mm:ss.SSSSSSSSS");
    }
    
    HttpGet request = null;
    try {
    //RestClient restClient = new RestClient();

    String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
        + uriRefFragment + schemaETag + uriQueryFragment + uriLastUpdateDateFragment;
    
    agg_uri = agg_uri + "?" + queryParamStartTime + startTime;
    
    if (endTime != null && !endTime.isEmpty()) {
      agg_uri = agg_uri + "&" + queryParamEndTime + endTime;
    }

    if (cursor != null && !cursor.isEmpty()) {
      agg_uri = agg_uri + "&" + queryParamCursor + cursor;
    }

    if (fetchLimit != null && !fetchLimit.isEmpty()) {
      agg_uri = agg_uri + "&" + queryParamFetchLimit + fetchLimit;
    }

    //Resource tableResource = restClient.resource(agg_uri);
    request = new HttpGet(agg_uri);
    request.addHeader("content-type", "application/json; charset=utf-8");
    request.addHeader("accept", "application/json");
    request.addHeader("accept-charset", "utf-8");
    request.addHeader("X-OpenDataKit-Version", "2.0");
    System.out.println("queryRowsInTimeRangeWithLastUpdateDate: agg uri is " + agg_uri);

    //String tableRes = tableResource.accept("application/json").get(String.class);
    HttpResponse response = null;
    if (localContext != null) {
      response = httpClient.execute(request, localContext);
    } else {
      response = httpClient.execute(request);
    }
    
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
        .getContent(), Charset.forName("UTF-8")));
    StringBuilder strLine = new StringBuilder();
    String resLine;
    while ((resLine = rd.readLine()) != null) {
      strLine.append(resLine);
    }
    String tableRes = strLine.toString();
    
    obj = new JSONObject(tableRes);
    System.out.println("queryRowsInTimeRangeWithLastUpdateDate: result for " + tableId + " is " + obj.toString());
    } finally {
      if (request != null) {
        request.releaseConnection();
      }
    }
    
    return obj;
  }
    
  /**
   * Returns a JSONObject of the rows that can 
   * be found in a table specified tableId and 
   * schemaETag in the range of a specified
   * startTime and endTime using savepointTimestamp
   * 
   * @param uri the url for the server
   * @param appId identifies the application
   * @param tableId the table identifier or name
   * @param schemaETag identifies an instance of the table
   * @param startTime a required timestamp used to get all rows with a time greater than or equal to it.  
   * The format of startTime is yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param endTime an optional timestamp used to get all rows with a time less than or equal to it.  
   * The format for endTime is yyyy-MM-dd:HH:mm:ss.SSSSSSSSS.
   * @param cursor query parameter that identifies the point at which to 
   * resume the query
   * @param fetchLimit query parameter that defines the number of rows to 
   * return
   * @return a JSONObject with the row data
   * @throws Exception any exception encountered is thrown to the caller
   */
  public JSONObject queryRowsInTimeRangeWithSavepointTimestamp(String uri, String appId, String tableId, String schemaETag,
      String startTime, String endTime, String cursor, String fetchLimit) throws Exception {
    JSONObject obj = null;
    
    if (httpClient == null) {
      throw new IllegalStateException("The initialization function must be called");
    }

    if (startTime == null || startTime.isEmpty()) {
      throw new IllegalArgumentException("startTime must have a valid value in the format yyyy-MM-dd:HH:mm:ss.SSSSSSSSS");
    }
    
    HttpGet request = null;
    try {
      //RestClient restClient = new RestClient();
  
      String agg_uri = uri + separator + appId + uriTablesFragment + separator + tableId
          + uriRefFragment + schemaETag + uriQueryFragment + uriSavepointTimestamp;
      
      agg_uri = agg_uri + "?" + queryParamStartTime + startTime;
      
      if (endTime != null && !endTime.isEmpty()) {
        agg_uri = agg_uri + "&" + queryParamEndTime + endTime;
      }
  
      if (cursor != null && !cursor.isEmpty()) {
        agg_uri = agg_uri + "&" + queryParamCursor + cursor;
      }
  
      if (fetchLimit != null && !fetchLimit.isEmpty()) {
        agg_uri = agg_uri + "&" + queryParamFetchLimit + fetchLimit;
      }
  
      //Resource tableResource = restClient.resource(agg_uri);
      request = new HttpGet(agg_uri);
      request.addHeader("content-type", "application/json; charset=utf-8");
      request.addHeader("accept", "application/json");
      request.addHeader("accept-charset", "utf-8");
      request.addHeader("X-OpenDataKit-Version", "2.0");
      System.out.println("queryRowsInTimeRangeWithSavepointTimestamp: agg uri is " + agg_uri);
  
      //String tableRes = tableResource.accept("application/json").get(String.class);
      HttpResponse response = null;
      if (localContext != null) {
        response = httpClient.execute(request, localContext);
      } else {
        response = httpClient.execute(request);
      }
      
      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity()
          .getContent(), Charset.forName("UTF-8")));
      StringBuilder strLine = new StringBuilder();
      String resLine;
      while ((resLine = rd.readLine()) != null) {
        strLine.append(resLine);
      }
      String tableRes = strLine.toString();
      
      obj = new JSONObject(tableRes);
      System.out.println("queryRowsInTimeRangeWithSavepointTimestamp: result for " + tableId + " is " + obj.toString());
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
