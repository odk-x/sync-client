package org.opendatakit.sync.client;

public class UriUtils {
  
  public static String getUserPermissionsUri(String uri) {
    String usersUri = uri + SyncClient.SEPARATOR_STR 
        + SyncClient.SSL_STR + SyncClient.SEPARATOR_STR + SyncClient.RESET_USERS_AND_PERMISSIONS;
    return usersUri;
  }
  
  public static String getPrivilegesInfoUri(String uri, String appId) {
	    String usersUri = uri + SyncClient.SEPARATOR_STR 
	        + appId + SyncClient.SEPARATOR_STR + SyncClient.PRIVILEGES_INFO_STR;
	    return usersUri;
	  }

  public static String getUsersListUri(String uri, String appId) {
    String usersUri = uri + SyncClient.SEPARATOR_STR + appId + SyncClient.SEPARATOR_STR + SyncClient.USERS_STR;
    return usersUri;
  }
  
  public static String getAppLevelManifestUri(String uri, String appId, String version) {
    String appLevelManifestUri = uri + SyncClient.SEPARATOR_STR + appId + SyncClient.SEPARATOR_STR 
        + SyncClient.MANIFEST_STR + SyncClient.SEPARATOR_STR + version;
    return appLevelManifestUri;
  }
  
  public static String getTableLevelManifestUri(String uri, String appId, String version, String tableId) {
    String tableLevelManifestUri = getAppLevelManifestUri(uri, appId, version)
        + SyncClient.SEPARATOR_STR + tableId;
    return tableLevelManifestUri;
  }
  
  public static String getFilesUri(String uri, String appId, String version) {
    String filesUri = uri + SyncClient.SEPARATOR_STR + appId + SyncClient.SEPARATOR_STR 
        + SyncClient.FILES_STR + SyncClient.SEPARATOR_STR  + version;
    return filesUri;
  }
  public static String getFilesWithRelPathUri(String uri, String appId, String version, String relPath) {
    String fileswithRelPathUri = getFilesUri(uri, appId, version) + SyncClient.SEPARATOR_STR + relPath;
    return fileswithRelPathUri;
  }
  
  public static String getTablesUri(String uri, String appId) {
    String tablesUri = uri + SyncClient.SEPARATOR_STR + appId + SyncClient.SEPARATOR_STR + SyncClient.TABLES_DIR;
    return tablesUri;
  }
  
  public static String getTableIdUri(String uri, String appId, String tableId) {
    String tableIdUri = getTablesUri(uri, appId) + SyncClient.SEPARATOR_STR + tableId;
    return tableIdUri;
  }
  
  public static String getTableIdRefUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdRefUri = getTableIdUri(uri, appId, tableId) 
        + SyncClient.SEPARATOR_STR + "ref" + SyncClient.SEPARATOR_STR + schemaETag;
    return tableIdRefUri;
  }
  
  public static String getTableIdDiffUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdDiffUri = getTableIdRefUri(uri, appId, tableId, schemaETag) 
        + SyncClient.SEPARATOR_STR + "diff";
    return tableIdDiffUri;
  }
  
  public static String getTableIdRowsUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdRowsUri = getTableIdRefUri(uri, appId, tableId, schemaETag) 
        + SyncClient.SEPARATOR_STR + SyncClient.ROWS_STR;
    return tableIdRowsUri;
  }
  
  public static String getRowIdUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) {
    String rowIdUri = getTableIdRowsUri(uri, appId, tableId, schemaETag) 
        + SyncClient.SEPARATOR_STR + rowId;
    return rowIdUri;
  }
  
  public static String getRowLevelFileManifestUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) { 
    String rowLevelManifestUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId) 
        + SyncClient.SEPARATOR_STR + SyncClient.MANIFEST_STR; 
    return rowLevelManifestUri;
  }
  
  public static String getRowIdAttachmentsUri(String uri, String appId, String tableId, String schemaETag, 
      String rowId) {
    String rowIdFileUri = getTableIdRefUri(uri, appId, tableId, schemaETag)
        + SyncClient.SEPARATOR_STR + "attachments" + SyncClient.SEPARATOR_STR + rowId;
    return rowIdFileUri;
  }
  
  public static String getRowIdAttachmentsDownloadUri(String uri, String appId, String tableId, String schemaETag,
      String rowId) {
    String rowIdAttachmentsDownloadUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId)
        + SyncClient.SEPARATOR_STR + "download";
    return rowIdAttachmentsDownloadUri;
  }
  
  public static String getRowIdFileUriWithRelPathUri(String uri, String appId, String tableId, String schemaETag, 
      String rowId, String relPath) {
    String rowIdFileUriWithRelPathUri = getRowIdAttachmentsUri(uri, appId, tableId, schemaETag, rowId)
        + SyncClient.SEPARATOR_STR + "file" + SyncClient.SEPARATOR_STR + relPath;
    return rowIdFileUriWithRelPathUri;
  }
  
  public static String getTableIdQueryUri (String uri, String appId, String tableId, String schemaETag) {
    String tableIdQueryUri = getTableIdRefUri(uri, appId, tableId, schemaETag) 
        + SyncClient.SEPARATOR_STR + "query"; 
    return tableIdQueryUri;
  }
  
  public static String getTableIdLastUpdateDateQueryUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdLastUpdateDateQueryUri = getTableIdQueryUri(uri, appId, tableId, schemaETag)
        + SyncClient.SEPARATOR_STR + "lastUpdateDate";
    return tableIdLastUpdateDateQueryUri;
  }
  
  public static String getTableIdSavepointTimestampQueryUri(String uri, String appId, String tableId, String schemaETag) {
    String tableIdSavepointTimestampQueryUri = getTableIdQueryUri(uri, appId, tableId, schemaETag)
        + SyncClient.SEPARATOR_STR + "savepointTimestamp";
    return tableIdSavepointTimestampQueryUri;
  }
}
