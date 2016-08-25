package org.opendatakit.wink.client;

public class UriUtils {
  
  public static String getUsersListUri(String uri) {
    String usersUri = uri + WinkClient.SEPARATOR_STR 
        + WinkClient.USERS_STR + WinkClient.SEPARATOR_STR + WinkClient.LIST_STR;
    return usersUri;
  }
  
  public static String getAppLevelManifestUri(String uri, String appId, String version) {
    String appLevelManifestUri = uri + WinkClient.SEPARATOR_STR + appId + WinkClient.SEPARATOR_STR 
        + WinkClient.MANIFEST_STR + WinkClient.SEPARATOR_STR + version;
    return appLevelManifestUri;
  }
  
  public static String getTableLevelManifestUri(String uri, String appId, String version, String tableId) {
    String tableLevelManifestUri = getAppLevelManifestUri(uri, appId, version)
        + WinkClient.SEPARATOR_STR + tableId;
    return tableLevelManifestUri;
  }
  
  public static String getFilesUri(String uri, String appId, String version) {
    String filesUri = uri + WinkClient.SEPARATOR_STR + appId + WinkClient.SEPARATOR_STR 
        + WinkClient.FILES_STR + WinkClient.SEPARATOR_STR  + version;
    return filesUri;
  }
  public static String getFilesWithRelPathUri(String uri, String appId, String version, String relPath) {
    String fileswithRelPathUri = getFilesUri(uri, appId, version) + WinkClient.SEPARATOR_STR + relPath;
    return fileswithRelPathUri;
  }
  
  public static String getTablesUri(String uri, String appId) {
    String tablesUri = uri + WinkClient.SEPARATOR_STR + appId + WinkClient.SEPARATOR_STR + WinkClient.TABLES_DIR;
    return tablesUri;
  }
  
  public static String getTableIdUri(String uri, String appId, String tableId) {
    String tableIdUri = getTablesUri(uri, appId) + WinkClient.SEPARATOR_STR + tableId;
    return tableIdUri;
  }
  
  public static String getTableIdRefUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdRefUri = getTableIdUri(uri, appId, tableId) 
        + WinkClient.SEPARATOR_STR + "ref" + WinkClient.SEPARATOR_STR + schemaETag;
    return tableIdRefUri;
  }
  
  public static String getTableIdRowsUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdRowsUri = getTableIdRefUri(uri, appId, tableId, schemaETag) 
        + WinkClient.SEPARATOR_STR + WinkClient.ROWS_STR;
    return tableIdRowsUri;
  }
  
  public static String getRowIdUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) {
    String rowIdUri = getTableIdRowsUri(uri, appId, tableId, schemaETag) 
        + WinkClient.SEPARATOR_STR + rowId;
    return rowIdUri;
  }
  
  public static String getRowLevelFileManifestUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) { 
    String rowLevelManifestUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId) 
        + WinkClient.SEPARATOR_STR + WinkClient.MANIFEST_STR; 
    return rowLevelManifestUri;
  }
  
  public static String getRowIdAttachmentsUri(String uri, String appId, String tableId, String schemaETag, 
      String rowId) {
    String rowIdFileUri = getTableIdRefUri(uri, appId, tableId, schemaETag)
        + WinkClient.SEPARATOR_STR + "attachments" + WinkClient.SEPARATOR_STR + rowId;
    return rowIdFileUri;
  }
  
  public static String getRowIdAttachmentsDownloadUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) {
    String rowIdAttachmentsDownloadUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId)
        + WinkClient.SEPARATOR_STR + "download";
    return rowIdAttachmentsDownloadUri;
  }
  
  public static String getRowIdFileUriWithRelPathUri(String uri, String appId, String tableId, String schemaETag, 
      String rowId, String relPath) {
    String rowIdFileUriWithRelPathUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId)
        + WinkClient.SEPARATOR_STR + "file" + WinkClient.SEPARATOR_STR + relPath;
    return rowIdFileUriWithRelPathUri;
  }
  
  public static String getTableIdQueryUri (String uri, String appId, String tableId, String schemaETag) {
    String tableIdQueryUri = getTableIdRefUri(uri, appId, tableId, schemaETag) 
        + WinkClient.SEPARATOR_STR + "query"; 
    return tableIdQueryUri;
  }
  
  public static String getTableIdLastUpdateDateQueryUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdLastUpdateDateQueryUri = getTableIdQueryUri(uri, appId, tableId, schemaETag)
        + WinkClient.SEPARATOR_STR + "lastUpdateDate";
    return tableIdLastUpdateDateQueryUri;
  }
  
  public static String getTableIdSavepointTimestampQueryUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdSavepointTimestampQueryUri = getTableIdQueryUri(uri, appId, tableId, schemaETag)
        + WinkClient.SEPARATOR_STR + "savepointTimestamp";
    return tableIdSavepointTimestampQueryUri;
  }
}
