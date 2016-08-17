package org.opendatakit.wink.client;

import java.io.File;

public class FileUtils {
  
  public static String getTableDefinitionFilePath(String dir, String tableId) {
    String tableDefinitionFileCSVPath = getTableIdDirPath(dir, tableId) 
        + File.separator + "definition.csv";
    return tableDefinitionFileCSVPath;
  }
  
  public static String getTableDataCSVFilePath(String dir, String tableId) {
    String tableDataCSVFilePath = getAssetsDirPath(dir)   
        + File.separator + "csv" + File.separator + tableId + ".csv";
    return tableDataCSVFilePath;
  }
  
  public static String getTableInstancesDirPath(String dir, String tableId) {
    String tableInstancesDirPath = getTableIdDirPath(dir, tableId) 
        + File.separator + WinkClient.INSTANCES_DIR;
    return tableInstancesDirPath;
  }
  
  public static String getTableIdDirPath(String dir, String tableId) {
    String tableIdDirPath = getTablesDirPath(dir) + File.separator + tableId;
    return tableIdDirPath;
  }
  
  public static String getTablesDirPath(String dir) {
    String tableDirPath = dir + File.separator + WinkClient.TABLES_DIR;
    return tableDirPath;
  }
  
  public static String getAssetsDirPath(String dir) {
    String assetsDirPath = dir + File.separator + WinkClient.ASSETS_DIR;
    return assetsDirPath;
  }
  
  public static String getRowInstanceFilePath(String dir, String tableId, String rowId, String fileName) {
    String rowInstanceFilePath = getTableIdDirPath(dir, tableId) + File.separator + WinkClient.INSTANCES_DIR 
        + File.separator + rowId + File.separator + fileName;
    return rowInstanceFilePath;
  }
  
  public static String getRelativePath(String dir, String relativePath) {
    String relPath = dir + File.separator + relativePath;
    return relPath;
  }
}
