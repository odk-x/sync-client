package opendatakit.wink.client.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvReader;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.Row;
import org.opendatakit.aggregate.odktables.rest.entity.RowFilterScope;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcome.OutcomeType;
import org.opendatakit.aggregate.odktables.rest.entity.RowOutcomeList;
import org.opendatakit.wink.client.FileUtils;
import org.opendatakit.wink.client.UriUtils;
import org.opendatakit.wink.client.WinkClient;

import junit.framework.TestCase;

public class WinkClientTest extends TestCase {
  String agg_url;
  String appId;
  String absolutePathOfTestFiles;
  String host;
  String userName;
  String password;
  int batchSize;
  String version;

  /*
   * Perform setup for test if necessary
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    //agg_url = System.getProperty("test.aggUrl");
    //appId = System.getProperty("test.appId");
    //absolutePathOfTestFiles = System.getProperty("test.absolutePathOfTestFiles");
    //batchSize = Integer.valueOf(System.getProperty("test.batchSize"));
    
    agg_url = "";
    appId = "odktables/default";
    absolutePathOfTestFiles = "testfiles/test/";
    batchSize = 1000;
    userName = "";
    password = "";
    URL url = new URL(agg_url);
    host = url.getHost();
    version = "2";
    
    WinkClient wc = new WinkClient();
    wc.init(host, userName, password);
    
    // Delete all files on the server 
    JSONObject appFiles = wc.getManifestForAppLevelFiles(agg_url, appId, version);

    JSONArray files = appFiles.getJSONArray(WinkClient.FILES_STR);
    
    for (int j = 0; j < files.size(); j++) {
      wc.deleteFile(agg_url, appId, files.getJSONObject(j).getString(WinkClient.FILENAME_STR), version);
    }
    
    // Delete all tables on the server 
    JSONObject tablesObj = wc.getTables(agg_url, appId);

    JSONArray tables = tablesObj.getJSONArray(WinkClient.TABLES_JSON);
    
    for (int i = 0; i < tables.size(); i++) {
      wc.deleteTableDefinition(agg_url, appId, tables.getJSONObject(i).getString(WinkClient.TABLE_ID_JSON), tables.getJSONObject(i).getString(WinkClient.SCHEMA_ETAG_JSON));
    }
  }

  /*
   * Perform tear down for tests if necessary
   */
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /*
   * Check preConditions if necessary
   */
  public void testPreConditions() {

  }

  public boolean checkThatFileExistsOnServer(WinkClient wc, String agg_url, String appId, String relativeFileNameOnServer) {
    // Make sure the server has added the file
    boolean found = false;

    try {
      wc.init(host, userName, password);
            
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId, version);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        if (fileName.equals(relativeFileNameOnServer)) {
          found = true;
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    return found;
  }
  
  public boolean checkThatTableLevelFileExistsOnServer(WinkClient wc, String agg_url, String appId, String tableId, String relativeFileNameOnServer) {
	    // Make sure the server has added the file
	    boolean found = false;

	    try {
	      wc.init(host, userName, password);
	      	      
	      JSONObject obj = wc.getManifestForTableId(agg_url, appId, tableId, version);
	      JSONArray files = obj.getJSONArray("files");

	      for (int i = 0; i < files.size(); i++) {
	        JSONObject file = files.getJSONObject(i);
	        String fileName = file.getString("filename");
	        if (fileName.equals(relativeFileNameOnServer)) {
	          found = true;
	          break;
	        }
	      }

	    } catch (Exception e) {
	      e.printStackTrace();
	    }

	    return found;
	  }

  public boolean checkThatInstanceFileExistsOnServer(WinkClient wc, String agg_url, String appId, String tableId,
      String schemaETag, String rowId, String relativeFileNameOnServer) {
    // Make sure the server has added the file
    boolean found = false;

    try {
      wc.init(host, userName, password);
      
      JSONObject obj = wc.getManifestForRow(agg_url, appId, tableId, schemaETag, rowId);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        if (fileName.equals(relativeFileNameOnServer)) {
          found = true;
          break;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return found;
  }

  public boolean checkThatTwoFilesAreTheSame(String firstFile, String secondFile) {
    // Make sure the server has added the file
    boolean same = true;

    try {
      File file1 = new File(firstFile);
      File file2 = new File(secondFile);

      // Two files cannot be the same
      if (file1.length() != file2.length()) {
        return false;
      }

      InputStream fis1 = new FileInputStream(file1);
      InputStream fis2 = new FileInputStream(file2);

      byte[] buf1 = new byte[1024];
      byte[] buf2 = new byte[1024];

      while ((fis1.read(buf1) > 0) && (fis2.read(buf2) > 0)) {
        if (!Arrays.equals(buf1, buf2)) {
          same = false;
        }
      }

      fis1.close();
      fis2.close();

    } catch (Exception e) {
      e.printStackTrace();
    }

    return same;
  }

  public boolean checkThatTableDefAndCSVDefAreEqual(String csvFile, JSONObject tableDef) {
    boolean same = false;
    InputStream in;

    try {
      in = new FileInputStream(new File(csvFile));

      InputStreamReader inputStream = new InputStreamReader(in);
      RFC4180CsvReader reader = new RFC4180CsvReader(inputStream);

      // Skip the first line - it's just headers
      reader.readNext();

      if (tableDef.containsKey("orderedColumns")) {
        JSONArray cols = tableDef.getJSONArray("orderedColumns");
        String[] csvDef;
        while ((csvDef = reader.readNext()) != null) {
          same = false;
          for (int i = 0; i < cols.size(); i++) {
            JSONObject col = cols.getJSONObject(i);
            String testElemKey = col.getString("elementKey");
            if (csvDef[0].equals(testElemKey)) {
              same = true;
              // Remove the index so we don't keep
              // comparing old defs
              cols.remove(i);
              break;
            }
          }

          if (!same) {
            return same;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return same;
  }
  
  public boolean checkThatRowHasId(String RowId, JSONObject rowRes) {
    boolean same = false;

    try {
      if (RowId.equals(rowRes.getString("id"))) {
        same = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return same;
  }
  
  public boolean checkThatRowHasColValue(String colValue, JSONObject rowRes) {
    boolean same = false;

    try {
      
      if (rowRes.has(WinkClient.ORDERED_COLUMNS_DEF)) {
        JSONArray ordCols = rowRes.getJSONArray(WinkClient.ORDERED_COLUMNS_DEF);
        assertEquals(1, ordCols.size());
        JSONObject col = ordCols.getJSONObject(0);
        String recVal = col.getString("value");
        if (recVal.equals(colValue)) {
          same = true;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return same;
  }
  
  public boolean checkThatRowExists(String RowId, String colValue, JSONObject rowRes) {
    boolean same = false;

    try {
      if (checkThatRowHasId(RowId, rowRes) && checkThatRowHasColValue(colValue, rowRes)) {
        same = true;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return same;
  }
  
  public void deleteFolder(File folder) {
    File[] files = folder.listFiles();
    if (files!=null) { 
      for (File f: files) {
        if (f.isDirectory()) {
          deleteFolder(f);
        } else {
          f.delete();
        }
      }
    }
    folder.delete();
  }
  
  public boolean doesTableExistOnServer(String tableId, String schemaETag) {
    boolean exists = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
           
      JSONObject obj = wc.getTables(agg_url, appId);

      JSONArray tables = obj.getJSONArray("tables");

      for (int i = 0; i < tables.size(); i++) {
        JSONObject table = tables.getJSONObject(i);
        if (tableId.equals(table.getString("tableId"))) {
          if (schemaETag.equals(table.getString("schemaETag"))) {
            exists = true;
          }
        }
      }
      wc.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return exists;
  }

  /*
   * Test uploading binary file to server
   */
  public void testUploadFileWithValidBinaryFile_ExpectPass() {

    String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Make sure that the file is on the server
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();
    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testUploadFileWithValidBinaryFile_ExpectPass: expected pass for " + testFile);
    }
  }

  /*
   * Test uploading ASCII file to server
   */
  public void testUploadFileWithValidAsciiFile_ExpectPass() {

    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Make sure that the file is on the server
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testUploadFileWithValidAsciiFile_ExpectPass: expected pass for " + testFile);
    }
  }
  
  /*
   * Test uploading ASCII file to server with .old extension
   * Currently this test fails on Mezuri but it should pass - commenting this
   * out until further investigation can be done.  
   */
//  public void testUploadFileWithValidAsciiFileAndOldExt_ExpectPass() {
//
//    String relativeTestFilePath = "assets/csv/geotagger.csv.old";
//    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
//    boolean foundFile = false;
//
//    try {
//      WinkClient wc = new WinkClient();
//
//      // Check the data from the file
//      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);
//
//      // Make sure that the file is on the server
//      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);
//
//      assertTrue(foundFile);
//
//      // After we are done clean up the file
//      wc.deleteFile(agg_url, appId, relativeTestFilePath);
//
//      // Make sure the server no longer has the file
//      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);
//
//      assertFalse(foundFile);
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      TestCase.fail("testUploadFileWithValidAsciiFile_ExpectPass: expected pass for " + testFile);
//    }
//  }

  /*
   * Test uploadFile when uri is null
   */
  public void testUploadFileWhenUriIsNull_ExpectFail() {

    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean thrown = false;
    WinkClient wc = null;
    
    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.uploadFile(null, appId, testFile, relativeTestFilePath, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test uploadFile when testFile is null
   */
  public void testUploadFileWhenTestFileIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    boolean thrown = false;
    WinkClient wc = null;
    
    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.uploadFile(agg_url, appId, null, relativeTestFilePath, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test uploadFile when relativePath is null
   */
  public void testUploadFileWhenRelativePathIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, null, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test downloading binary file to server
   */
  public void testDownloadFileWithValidBinaryFile_ExpectPass() {
    String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;

    String dowloadTestFile = absolutePathOfTestFiles + "testDownload/" + relativeTestFilePath;
    boolean sameFile = false;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Get the file off of the server
      wc.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath, version);

      // Check the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testDownloadFileWithValidBinaryFile_ExpectPass: expected pass for downloading file "
              + testFile);
    }
  }

  /*
   * Test downloading ASCII file to server
   */
  public void testDownloadFileWithValidAsciiFile_ExpectPass() {
    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;

    String dowloadTestFile = absolutePathOfTestFiles + "testDownload/" + relativeTestFilePath;
    boolean sameFile = false;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Get the file off of the server
      wc.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath, version);

      // Check that the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testDownloadFileWithValidAsciiFile_ExpectPass: expected pass for downloading file "
              + testFile);
    }
  }

  /*
   * Test downloadFile when uri is null
   */
  public void testDownloadFileWhenUriIsNull_ExpectFail() {

    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      

      // Check the data from the file
      wc.downloadFile(null, appId, testFile, relativeTestFilePath, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);

  }

  /*
   * Test downloadFile when testFile is null
   */
  public void testDownloadFileWhenTestFileIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.downloadFile(agg_url, appId, null, relativeTestFilePath, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test downloadFile when relativePath is null
   */
  public void testDownloadFileWhenRelativePathIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      

      // Check the data from the file
      wc.downloadFile(agg_url, appId, testFile, null, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test delete binary file from server
   */
  public void testDeleteFileWithValidBinaryFile_ExpectPass() {
    String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Check that the server has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testDeleteFileWithValidBinaryFile_ExpectPass: expected pass for deleting file "
              + testFile);
    }
  }

  /*
   * Test delete ASCII file from server
   */
  public void testDeleteFileWithValidAsciiFile_ExpectPass() {
    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Check that the file is on the server
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteFileWithValidAsciiFile_ExpectPass: expected pass for deleting file "
          + testFile);
    }
  }

  /*
   * Test deleteFile when uri is null
   */
  public void testDeleteFileWhenUriIsNull_ExpectFail() {

    String relativeTestFilePath = "assets/index.html";
    boolean thrown = false;
    WinkClient wc = null;
    
    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.deleteFile(null, appId, relativeTestFilePath, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);

  }

  /*
   * Test deleteFile when relativePath is null
   */
  public void testDeleteFileWhenRelativePathIsNull_ExpectFail() {
    boolean thrown = false;
    WinkClient wc = null;
    
    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Check the data from the file
      wc.deleteFile(agg_url, appId, null, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test getting the manifest for app level files with valid files
   */
  public void testGetManifestForAppLevelFilesWithValidFile_ExpectPass() {
    String relativeTestFilePath = "assets/index.html";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);

      // Put the file on the server
      wc.init(host, userName, password);
      
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Test the manifest
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId, version);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        if (fileName.equals(relativeTestFilePath)) {
          foundFile = true;
          break;
        }
      }

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testGetManifestForAppLevelFilesWithValidFile_ExpectPass: expected pass for manifest with file "
              + testFile);
    }
  }

  /*
   * Test getting the manifest for app level files when there are no files
   */
  public void testGetManifestForAppLevelFilesWithNoFiles_ExpectPass() {
    String relativeTestFilePath = "assets/index.html";
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      

      // Test the manifest
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId, version);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        if (fileName.equals(relativeTestFilePath)) {
          foundFile = true;
          break;
        }
      }

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetManifestForAppLevelFilesWithNoFiles_ExpectPass: expected pass");
    }
  }

  /*
   * Test getting the manifest for app level files when uri is null
   */
  public void testGetManifestForAppLevelFilesWhenUriIsNull_ExpectFail() {
    boolean thrown = false;
    WinkClient wc = null;
    
    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
   
      // Test the manifest
      wc.getManifestForAppLevelFiles(null, appId, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }
    assertTrue(thrown);
  }

  /*
   * Test getting the manifest for app level files when uri is null
   */
  public void tesGetAllAppLevelFilesFromUri_ExpectFail() {
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Test the manifest
      wc.getManifestForAppLevelFiles(null, appId, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }
    assertTrue(thrown);
  }

  /*
   * Test getting the app level files with valid files
   */
  public void testGetAllAppLevelFilesFromUriWithValidFile_ExpectPass() {
    String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;

    String dowloadTestDir = absolutePathOfTestFiles + "test/";
    String downloadTestFile = absolutePathOfTestFiles + "test/" + relativeTestFilePath;
    boolean sameFile = false;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(agg_url, appId, dowloadTestDir, version);

      // Check the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, downloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(wc, agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("tesGetAllAppLevelFilesFromUri_ExpectPass: expected pass");
    }
  }

  /*
   * Test getting the app level files when uri is null
   */
  public void testGetAllAppLevelFilesFromUriWhenUriIsNull_ExpectFail() {
    boolean thrown = true;
    String dowloadTestDir = absolutePathOfTestFiles + "testAppLevelFiles/";
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(null, appId, dowloadTestDir, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * Test getting the app level files when dir is null
   */
  public void testGetAllAppLevelFilesFromUriWhenDirToSaveIsNull_ExpectFail() {
    boolean thrown = true;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(agg_url, appId, null, version);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }
  
  /*
   * test createTable with string value
   */
  public void testCreateTableWithString_ExpectPass() {

    String testTableId = "test0";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String testTableSchemaETag = "testCreateTableWithString_ExpectPass";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      if (tableDef.containsKey("orderedColumns")) {
        JSONArray cols = tableDef.getJSONArray("orderedColumns");
        for (int i = 0; i < cols.size(); i++) {
          JSONObject col = cols.getJSONObject(i);
          String testElemKey = col.getString("elementKey");
          assertEquals(colKey, testElemKey);
        }
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateTableWithString: expected pass");
    }
  }

  /*
   * test createTable when uri is null
   */
  public void testCreateTableWhenUriIsNull_ExpectFail() {

    String testTableId = "test1";
    String testTableSchemaETag = "testCreateTableWhenUriIsNull_ExpectFail";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String listOfChildElements = "[]";
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      wc.createTable(null, appId, testTableId, testTableSchemaETag, columns);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * test createTable when tableId is null
   */
  public void testCreateTableWhenTableIdIsNull_ExpectFail() {
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String listOfChildElements = "[]";
    String testTableSchemaETag = "testCreateTableWhenTableIdIsNull_ExpectFail";
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      wc.createTable(agg_url, appId, null, testTableSchemaETag, columns);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * test createTable when columns is null
   */
  public void testCreateTableWhenColumnsIsNull_ExpectFail() {
    String testTableId = "test2";
    String testTableSchemaETag = "testCreateTableWhenColumnsIsNull_ExpectFail";
    boolean thrown = false;
    WinkClient wc = null;

    try {
      wc = new WinkClient();
      wc.init(host, userName, password);
      
      wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    } finally {
      if (wc != null) {
        wc.close();
      }
    }

    assertTrue(thrown);
  }

  /*
   * test createTableWithCSV with valid data
   */
  public void testCreateTableWithCSVAndValidData_ExpectPass() {
    String testTableId = "test3";
    String tableSchemaETag = null;
    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testCreateTableWithCSVAndValidData_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      // Make sure it is the same as the csv definition
      assertTrue(checkThatTableDefAndCSVDefAreEqual(csvFile, tableDef));

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testCreateTableWithCSVAndValidData_ExpectPass: expected pass for creating table with CSV");
    }

  }
  
  /*
   * test createTableWithCSV with valid data
   */
  public void testCreateTableWithCSVInputStream_ExpectPass() {
    String testTableId = "test4";
    String tableSchemaETag = null;
    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      File file = new File(csvFile);

      InputStream in = new FileInputStream(file);
      
      JSONObject result = wc.createTableWithCSVInputStream(agg_url, appId, testTableId, null, in);
      System.out.println("testCreateTableWithCSVAndValidData_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      // Make sure it is the same as the csv definition
      assertTrue(checkThatTableDefAndCSVDefAreEqual(csvFile, tableDef));

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateTableWithCSVInputStream_ExpectPass: expected pass for creating table with CSV");
    }

  }

  /*
   * test deleteTableDefintion when the table is valid
   */
  public void testDeleteTableDefinitionWithValidValues_ExpectPass() {

    String testTableId = "test5";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testDeleteTableDefinitionWithValidValues_ExpectPass: expected pass deleting table defintion");
    }
  }
  
  /*
   * test createTable when schemaETag is null
   */
  public void testCreateTableWhenSchemaETagIsNull_ExpectPass() {
    String testTableId = "test6";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      if (tableDef.containsKey("orderedColumns")) {
        JSONArray cols = tableDef.getJSONArray("orderedColumns");
        for (int i = 0; i < cols.size(); i++) {
          JSONObject col = cols.getJSONObject(i);
          String testElemKey = col.getString("elementKey");
          assertEquals(colKey, testElemKey);
        }
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateTableWhenSchemaETagIsNull_ExpectPass: expected pass");
    }
  }

  public void testGetTablesWhenTableExists_ExpectPass() {
    String testTableId = "test7";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    boolean found = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject obj = wc.getTables(agg_url, appId);
      JSONArray tables = obj.getJSONArray("tables");

      for (int i = 0; i < tables.size(); i++) {
        JSONObject table = tables.getJSONObject(i);
        if (testTableId.equals(table.getString("tableId"))) {
          found = true;
          break;
        }
      }

      assertTrue(found);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTablesWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }
  
  public void testGetTablesForTestNeedToDelete_ExpectPass() {
    String testTableId = "test71";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    boolean found = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject obj = wc.getTables(agg_url, appId);
      JSONArray tables = obj.getJSONArray("tables");

      for (int i = 0; i < tables.size(); i++) {
        JSONObject table = tables.getJSONObject(i);
        if (testTableId.equals(table.getString("tableId"))) {
          found = true;
          break;
        }
      }

      assertTrue(found);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTablesWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testGetTableWhenTableExists_ExpectPass() {
    String testTableId = "test8";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject obj = wc.getTable(agg_url, appId, testTableId);
      assertEquals(testTableId, obj.getString("tableId"));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTableWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }
  
  public void testGetTableWhenTableDoesNotExists_ExpectPass() {
    String testTableId = "test81";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject obj = wc.getTable(agg_url, appId, testTableId);
      assertNull(obj);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTableWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testWriteTableDefinitionToCSVWhenTableExists_ExpectPass() {
    String testTableId = "test9";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      String testCSVFile = absolutePathOfTestFiles + "writeTest.csv";
      wc.writeTableDefinitionToCSV(agg_url, appId, testTableId, tableSchemaETag, testCSVFile);

      checkThatTableDefAndCSVDefAreEqual(testCSVFile, result);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteTableDefinitionToCSVWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testGetTableDefinitionWhenTableExists_ExpectPass() {
    String testTableId = "test10";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject obj = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      // Check that the table definition has the right table id
      if (obj.containsKey("tableId")) {
        String tableId = obj.getString("tableId");
        assertEquals(tableId, testTableId);
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTableDefinitionWhenTableExists_ExpectPass: expected pass");
    }
  }

  public void testGetRowWhenRowExists_ExpectPass() {
    String testTableId = "test11";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, rowRes));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetRowWhenRowExists_ExpectPass: expected pass");
    }
  }

  public void testCreateRowsUsingCSVWithValidFile_ExpectPass() {
    String testTableId = "test12";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testCreateRowsUsingCSVWithValidFile_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

      JSONObject res = wc.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVWithValidFile_ExpectPass: expected pass");
    }
  }

  public void testGetRowsWhenRowsExist_ExpectPass() {
    String testTableId = "test13";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testGetRowsWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

      JSONObject res = wc.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetRowsWhenRowsExist_ExpectPass: expected pass");
    }
  }

  public void testGetRowsSinceWhenRowsExist_ExpectPass() {
    String testTableId = "test14";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testGetRowsSinceWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetRowsSinceWhenRowsExist_ExpectPass: expected pass");
    }
  }

  public void testWriteRowDataToCSVWhenRowsExist_ExpectPass() {
    String testTableId = "test15";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";
    String csvOutputFile = absolutePathOfTestFiles + "downloadedData/geotaggerTest/geotagger.output.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testWriteRowDataToCSVWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

      wc.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvOutputFile);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
    }
  }
  
  public void testDeleteRowWhenRowExists_ExpectPass() {
    String testTableId = "test16";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowArrayList = new ArrayList<Row>();
      rowArrayList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      
      String dataETag = res.getString("dataETag");
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Now delete the row
      // wc.deleteRow(agg_url, appId, testTableId, tableSchemaETag, RowId, res.getString("rowETag"));
      Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString("rowETag"), row.getFormId(), row.getLocale(), row.getSavepointType(), 
          row.getSavepointTimestamp(), row.getSavepointCreator(), row.getRowFilterScope(), row.getValues());
      
      // Make sure that all of these rows are marked for deletion
      rowObj.setDeleted(true);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(rowObj);
      wc.deleteRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, dataETag, rowList, 0);

      // Check that the row was deleted
      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      assertTrue(rowRes.getBoolean("deleted"));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
    }
  }

  /*
   * Test getting the manifest for app level files with valid files
   */
  public void testGetManifestForTableIdWhenTableFileExists_ExpectPass() {
    String testTableId = "test17";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    boolean foundFile = false;
    String relativeTestFilePath = "tables/" + testTableId + "/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + testTableId + File.separator + "spaceNeedle_CCLicense_goCardUSA.jpg";


    // manufacture a rowId for this record... String RowId = "uuid:" +
    UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Add a file for table
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

      // Make sure that the file is on the server
      foundFile = checkThatTableLevelFileExistsOnServer(wc, agg_url, appId, testTableId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath, version);

      // Make sure the server no longer has the file
      foundFile = checkThatTableLevelFileExistsOnServer(wc, agg_url, appId, testTableId, relativeTestFilePath);

      assertFalse(foundFile);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetManifestForTableIdWhenTableFileExists_ExpectPass: expected pass");
    }
  }
  
  public void testGetManifestForRowWithNoFiles_ExpectPass() {
    String testTableId = "test18";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      
      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Get the manifest to check that the file is there
      JSONObject obj = wc.getManifestForRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

      // Make sure there are no files returned
      JSONArray files = obj.getJSONArray("files");
      int numOfFiles = files.size();
      
      assertEquals(numOfFiles, 0);

      // Delete the table and all data
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetManifestForRowWithNoFiles_ExpectPass: expected pass");
    }
  }

  public void testPutFileForRowWithValidBinaryFile_ExpectPass() {
    String testTableId = "test19";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    boolean foundFile = false;
    String relativePathOnServer = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String wholePathToFile = this.absolutePathOfTestFiles + relativePathOnServer;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      
      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Put file for row
      wc.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
          relativePathOnServer);

      // Make sure that the file is on the server foundFile =
      foundFile = checkThatInstanceFileExistsOnServer(wc, agg_url, appId, testTableId, tableSchemaETag, RowId, relativePathOnServer);

      assertTrue(foundFile);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testPutFileForRowWithValidBinaryFile_ExpectPass: expected pass");
    }
  }

  public void testGetFileForRowWithValidBinaryFile_ExpectPass() {
    String testTableId = "test20";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    boolean foundFile = false;
    String relativePathOnServer = "spaceNeedle_CCLicense_goCardUSA.jpg";
    String wholePathToFile = absolutePathOfTestFiles + "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String pathToSaveFile = absolutePathOfTestFiles + "downloadInstance/" + relativePathOnServer;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      
      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Put file for row
      wc.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile, relativePathOnServer);

      // Make sure that the file is on the server
      foundFile = checkThatInstanceFileExistsOnServer(wc, agg_url, appId, testTableId, tableSchemaETag, RowId, relativePathOnServer);

      assertTrue(foundFile);

      // Now get the file
      wc.getFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, false, pathToSaveFile, relativePathOnServer);

      boolean sameFile = checkThatTwoFilesAreTheSame(wholePathToFile, pathToSaveFile);

      assertTrue(sameFile);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetFileForRowWithValidBinaryFile_ExpectPass: expected pass");
    }
  }
  
//  public void testWriteRowDataToCSVWithLotsOfRows_ExpectPass() {
//    String testTableId = "test21";
//    String tableSchemaETag = null;
//
//    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
//    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";
//    String csvFilePathToWriteTo = absolutePathOfTestFiles + "writeLargeRowData.csv";
//
//    try {
//      WinkClient wc = new WinkClient();
//      wc.init(host, userName, password);
//  
//      // Temp hack until Aggregate is fixed
//      wc.getUsers(agg_url);
// 
//      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
//      System.out.println("testWriteRowDataToCSVWithLotsOfRows_ExpectPass: result is " + result);
//
//      if (result.containsKey("tableId")) {
//        String tableId = result.getString("tableId");
//        assertEquals(tableId, testTableId);
//        tableSchemaETag = result.getString("schemaETag");
//      }
//      
//      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, batchSize);
//
//      wc.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvFilePathToWriteTo);
//      
//      InputStream in = new FileInputStream(csvFilePathToWriteTo);
//      InputStreamReader inputStream = new InputStreamReader(in);
//      RFC4180CsvReader reader = new RFC4180CsvReader(inputStream);
//      int lineCnt = 0;
//      while (reader.readNext() != null) {
//        lineCnt++;
//      }
//      
//      // This will need to be changed for lots of data
//      assertEquals(lineCnt, 8193);
//
//      // Now delete the table
//      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
//      
//      wc.close();
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      TestCase.fail("testWriteRowDataToCSVWithLotsOfRows_ExpectPass: expected pass");
//    }
//  }
  
  public void testWriteRowDataToCSVWhenNoRowsExist_ExpectPass() {
    String testTableId = "test22";
    String tableSchemaETag = null;

    String relativeFileNameOnServer = "geotagger.shouldnotexist.csv";
    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvOutputFile = absolutePathOfTestFiles + "geotaggerTest/" + relativeFileNameOnServer;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testWriteRowDataToCSVWhenRowsExist_ExpectPass: result of create table is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvOutputFile);
      
      File csvOutFile = new File(csvOutputFile);
      
      assertFalse(csvOutFile.exists());

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
    }
  }
  
  /*
   * test createTableWithJSON with valid data
   */
  public void testCreateTableWithJSON_ExpectPass() {

    String testTableId = "test23";
    String tableSchemaETag = null;
    String jsonString = "{\"orderedColumns\":[{\"elementKey\":\"Date_and_Time\",\"elementType\":\"dateTime\",\"elementName\":\"Date_and_Time\",\"listChildElementKeys\":\"[]\"}],\"tableId\":\"testDate\",\"schemaETag\":null}";
    
    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
      System.out.println("testCreateTableWithJSON_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      if (tableDef.containsKey(WinkClient.ORDERED_COLUMNS_DEF)) {
        JSONArray cols = tableDef.getJSONArray(WinkClient.ORDERED_COLUMNS_DEF);
        assertEquals(cols.size(), 1);
        JSONObject col = cols.getJSONObject(cols.size() - 1);
        assertEquals("Date_and_Time", col.getString(WinkClient.ELEM_KEY_JSON));
      }

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testCreateTableWithCSVAndValidData_ExpectPass: expected pass for creating table with CSV");
    }
  }
  
  public void testCreateRowsUsingJSONBulkUpload_ExpectPass() {
    String testTableId = "test24";
    String tableSchemaETag = null;
    String jsonString = "{\"orderedColumns\":[{\"elementKey\":\"Date_and_Time\",\"elementType\":\"dateTime\",\"elementName\":\"Date_and_Time\",\"listChildElementKeys\":\"[]\"}],\"tableId\":\"testDate\",\"schemaETag\":null}";
    JSONObject rowsWrapper = new JSONObject();
    JSONObject tempRow = null;
    JSONArray rowsObj = new JSONArray();
    String jsonRows = null;
    int testSize = 200;
    int testBatchSize = 50;
    
    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
      System.out.println("testCreateRowsUsingJSONBulkUpload_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      if (tableDef.containsKey(WinkClient.ORDERED_COLUMNS_DEF)) {
        JSONArray cols = tableDef.getJSONArray(WinkClient.ORDERED_COLUMNS_DEF);
        assertEquals(cols.size(), 1);
        JSONObject col = cols.getJSONObject(cols.size() - 1);
        assertEquals("Date_and_Time", col.getString(WinkClient.ELEM_KEY_JSON));
      }
      
      // Create Rows for the newly created table
      for (int i = 0; i < testSize; i++) {
        
        tempRow = new JSONObject();
        tempRow.put(WinkClient.ID_JSON, Integer.toString(i));
        
        JSONArray ordCols = new JSONArray();
        JSONObject col = new JSONObject();
        col.put("column", "Date_and_Time");
        col.put("value", TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));
        ordCols.add(col);
        
        tempRow.put(WinkClient.ORDERED_COLUMNS_DEF, ordCols);
        System.out.print("testCreateRowsUsingJSONBulkUpload_ExpectPass: tempRow is " + tempRow.toString());
        rowsObj.add(tempRow);
      }
      
      rowsWrapper.put("rows", rowsObj);

      jsonRows = rowsWrapper.toString();
      wc.createRowsUsingJSONBulkUpload(agg_url, appId, testTableId, tableSchemaETag, jsonRows, testBatchSize);

      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), testSize);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingJSONBulkUpload_ExpectPass: expected pass");
    }
  }
  
  /*
   * test getSchemaETagForTable 
   */
  public void testGetSchemaETagForTable_ExpectPass() {

    String testTableId = "test25";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String testTableSchemaETag = "testGetSchemaETagForTable_ExpectPass";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }
      
      String eTag = wc.getSchemaETagForTable(agg_url, appId, testTableId);
      
      assertEquals(tableSchemaETag, eTag);

      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      if (tableDef.containsKey("orderedColumns")) {
        JSONArray cols = tableDef.getJSONArray("orderedColumns");
        for (int i = 0; i < cols.size(); i++) {
          JSONObject col = cols.getJSONObject(i);
          String testElemKey = col.getString("elementKey");
          assertEquals(colKey, testElemKey);
        }
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetSchemaETagForTable_ExpectPass: expected pass");
    }
  }
  
  /*
   * test create row bulk upload with UTF-8
   */
  public void testCreateRowsUsingBulkUploadWithUTF8_ExpectPass() {

    String testTableId = "test26";
    String colName = "utf_test_col";
    String colKey = "utf_test_col";
    String colType = "string";
    String utf_val = "à¤¤à¥�à¤°à¤‚à¤¤ à¤…à¤¸à¥�à¤ªà¤¤à¤¾à¤² à¤°à¥‡à¤«à¤° à¤•à¤°à¥‡à¤‚ à¤µ à¤°à¤¾à¤¸à¥�à¤¤à¥‡ à¤®à¥ˆà¤‚ à¤¶à¤¿à¤¶à¥� à¤•à¥‹ à¤“. à¤†à¤°. à¤�à¤¸ à¤¦à¥‡à¤¤à¥‡à¤°à¤¹à¥‡à¤‚";

    String testTableSchemaETag = "testCreateOrUpdateRowWithUTF8_ExpectPass";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      if (tableDef.containsKey("orderedColumns")) {
        JSONArray cols = tableDef.getJSONArray("orderedColumns");
        for (int i = 0; i < cols.size(); i++) {
          JSONObject col = cols.getJSONObject(i);
          String testElemKey = col.getString("elementKey");
          assertEquals(colKey, testElemKey);
        }
      }
      
      DataKeyValue dkv = new DataKeyValue(colKey, utf_val);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, utf_val, jsonRow));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateOrUpdateRowWithUTF8_ExpectPass: expected pass");
    }
  }
  
  /*
   * test query in time range with last update date 
   */
  public void testQueryRowsInTimeRangeWithLastUpdateDate_ExpectPass() {
    String testTableId = "test27";
    String colName = "seq_num";
    String colKey = "seq_num";
    String colType = "string";

    String testTableSchemaETag = "createRowsForQueryTest";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    Date date = new Date(0);
    String startTime = dateFormat.format(date); 
    
    int sizeOfSeqTable = 50;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

      if (result.containsKey("tableId")) {
        tableSchemaETag = result.getString("schemaETag");
      }

      ArrayList<Row> rowList = new ArrayList<Row>();
      for (int i = 0; i < sizeOfSeqTable; i++) {
        DataKeyValue dkv = new DataKeyValue(colName, Integer.toString(i));
        ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
        dkvl.add(dkv);
        String RowId = "uuid:" + UUID.randomUUID().toString();
        Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
        rowList.add(row);
      }

      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 0);

      JSONObject res = wc.queryRowsInTimeRangeWithLastUpdateDate(agg_url, appId, testTableId, tableSchemaETag, startTime, null, null, null);
      
      if (res.containsKey("rows")) {
        JSONArray rowsObj = res.getJSONArray("rows");
        assertEquals(rowsObj.size(), sizeOfSeqTable);
        
      }

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testQueryRowsInTimeRangeWithLastUpdateDate_ExpectPass: expected pass");
    }
  }
  
  /*
   * test query in time range with savepointTimestamp date 
   */
  public void testQueryRowsInTimeRangeWithSavepointTimestamp_ExpectPass() {
    String testTableId = "test28";
    String colName = "seq_num";
    String colKey = "seq_num";
    String colType = "string";

    String testTableSchemaETag = "createRowsForQueryTest";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");
    Date date = new Date();
    String startTime = dateFormat.format(date); 
    
    int sizeOfSeqTable = 50;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

      if (result.containsKey("tableId")) {
        tableSchemaETag = result.getString("schemaETag");
      }

      ArrayList<Row> rowList = new ArrayList<Row>();
      for (int i = 0; i < sizeOfSeqTable; i++) {
        DataKeyValue dkv = new DataKeyValue(colName, Integer.toString(i));
        ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
        dkvl.add(dkv);
        String RowId = "uuid:" + UUID.randomUUID().toString();
        Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
        rowList.add(row);
      }

      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 0);

      JSONObject res = wc.queryRowsInTimeRangeWithSavepointTimestamp(agg_url, appId, testTableId, tableSchemaETag, startTime, null, null, null);
      
      if (res.containsKey("rows")) {
        JSONArray rowsObj = res.getJSONArray("rows");
        assertEquals(rowsObj.size(), sizeOfSeqTable);
        
      }
      
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testQueryRowsInTimeRangeWithSavepointTimestamp_ExpectPass: expected pass");
    }
  }
  
  public void testUpdateRowWhenRowExists_ExpectPass() {
    String testTableId = "test29";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    String colValue2 = "/whatever/whatever";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, RowFilterScope.EMPTY_ROW_FILTER, dkvl);
      ArrayList<Row> rowArrayList = new ArrayList<Row>();
      rowArrayList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      
      String dataETag = res.getString("dataETag");
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Now update the row
      dkv = new DataKeyValue("scan_output_directory", colValue2);
      dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString(WinkClient.ROW_ETAG_JSON), row.getFormId(), jsonRow.getString(WinkClient.LOCALE_JSON), jsonRow.getString(WinkClient.SAVEPOINT_TYPE_JSON), 
          jsonRow.getString(WinkClient.SAVEPOINT_TIMESTAMP_JSON), jsonRow.getString(WinkClient.SAVEPOINT_CREATOR_JSON), row.getRowFilterScope(), dkvl);
      
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(rowObj);
      wc.updateRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, dataETag, rowList, 0);

      // Check that the row was updated
      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      assertTrue(checkThatRowHasColValue(colValue2, rowRes));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
    }
  }
  
  public void testAlterRowsUsingSingleBatchWithValidData_ExpectPass() {
    String testTableId = "test30";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    String colValue2 = "/whatever/whatever";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, RowFilterScope.EMPTY_ROW_FILTER, dkvl);
      ArrayList<Row> rowArrayList = new ArrayList<Row>();
      rowArrayList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      
      String dataETag = res.getString("dataETag");
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Now update the row
      dkv = new DataKeyValue("scan_output_directory", colValue2);
      dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString(WinkClient.ROW_ETAG_JSON), row.getFormId(), jsonRow.getString(WinkClient.LOCALE_JSON), jsonRow.getString(WinkClient.SAVEPOINT_TYPE_JSON), 
          jsonRow.getString(WinkClient.SAVEPOINT_TIMESTAMP_JSON), jsonRow.getString(WinkClient.SAVEPOINT_CREATOR_JSON), row.getRowFilterScope(), dkvl);
      
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(rowObj);
      RowOutcomeList outcomeList = wc.alterRowsUsingSingleBatch(agg_url, appId, testTableId, tableSchemaETag, dataETag, rowList);
      
      ArrayList<RowOutcome> resultingRowOutcomeList = outcomeList.getRows();
      RowOutcome rowOutcome = resultingRowOutcomeList.get(0);
      
      // Check that the row was updated
      assertEquals(resultingRowOutcomeList.size(), 1);

      assertEquals(rowOutcome.getOutcome(), OutcomeType.SUCCESS);
      
      // Check that the row was updated
      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      assertTrue(checkThatRowExists(RowId, colValue2, rowRes));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
    }
  }
  
  /**
   * Test batchGetFilesForRow
   */
  public void testBatchGetFileForRowWithValidBinaryFiles_ExpectPass() {
	String testTableId = "test31";

    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    boolean foundFile = false;
    String relativePathOnServer = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String wholePathToFile = this.absolutePathOfTestFiles + relativePathOnServer;
    
    String pathToSaveFile = absolutePathOfTestFiles + "downloadedData/downloadBatchInstance";
    String pathToVerify = pathToSaveFile + WinkClient.SEPARATOR_STR + relativePathOnServer;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      
      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      // Put file for row
      wc.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
          relativePathOnServer);

      // Make sure that the file is on the server foundFile =
      foundFile = checkThatInstanceFileExistsOnServer(wc, agg_url, appId, testTableId, tableSchemaETag, RowId, relativePathOnServer);

      assertTrue(foundFile);

      // Get the list of files
      JSONObject filesToGetObj = wc.getManifestForRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      
      // Download files into pathToSaveFile 
      wc.batchGetFilesForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, pathToSaveFile, filesToGetObj, 0);
      
      assertTrue(checkThatTwoFilesAreTheSame(wholePathToFile, pathToVerify));
      
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
	      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testBatchGetFileForRowWithValidBinaryFiles_ExpectPass: expected pass");
    }
  }
  
  public void testAlterRowsUsingSingleBatchWithNoTable_ExpectPass() {
    String testTableId = "test32";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue2 = "/whatever/whatever";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Now update the row
      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue2);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);
      
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");
      Date date = new Date();
      String startTime = dateFormat.format(date);   
        
      Row row = Row.forInsert(RowId, null, null, null, startTime, null, RowFilterScope.EMPTY_ROW_FILTER, dkvl);
      
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      RowOutcomeList outcomeList = wc.alterRowsUsingSingleBatch(agg_url, appId, testTableId, tableSchemaETag, null, rowList);
      
      ArrayList<RowOutcome> resultingRowOutcomeList = outcomeList.getRows();
      RowOutcome rowOutcome = resultingRowOutcomeList.get(0);
      
      // Check that the row was updated
      assertEquals(resultingRowOutcomeList.size(), 1);

      assertEquals(rowOutcome.getOutcome(), OutcomeType.SUCCESS);
      
      // Check that the row was updated
      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      assertTrue(checkThatRowExists(RowId, colValue2, rowRes));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
    }
  }

  public void testCreateRowsUsingCSVBulkUpload_ExpectPass() {
    String testTableId = "test40";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.edited.csv";
    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testCreateRowsUsingCSVBulkUpload_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 7);

      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 3);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVBulkUpload_ExpectPass: expected pass");
    }
  }

  /*public void testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass() {
    String testTableId = "test41";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Temp hack until Aggregate is fixed
      wc.getUsers(agg_url);

      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }
      
      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, batchSize);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass: expected pass");
    }
  }*/
  
  public void testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass() {
	    String testTableId = "test42";
	    String tableSchemaETag = null;

	    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
	    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data_small.csv";

	    try {
	      WinkClient wc = new WinkClient();
	      wc.init(host, userName, password);
	      
	      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
	      System.out.println("testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass: result is " + result);

	      if (result.containsKey("tableId")) {
	        String tableId = result.getString("tableId");
	        assertEquals(tableId, testTableId);
	        tableSchemaETag = result.getString("schemaETag");
	      }
	      
	      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, batchSize);

	      // Now delete the table
	      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
	      
	      wc.close();

	    } catch (Exception e) {
	      e.printStackTrace();
	      TestCase.fail("testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass: expected pass");
	    }
	  }
    
  /*public void testCreateRowsUsingCSVInputStreamBulkUploadWithLotsOfData_ExpectPass() {
    String testTableId = "test43";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      // Temp hack until Aggregate is fixed
      wc.getUsers(agg_url);
      
      File file1 = new File(csvFile);

      InputStream in1 = new FileInputStream(file1);

      JSONObject result = wc.createTableWithCSVInputStream(agg_url, appId, testTableId, null, in1);
      System.out.println("testCreateRowsUsingCSVInputStreamBulkUploadWithLotsOfData_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }
      
      File file2 = new File(csvDataFile);

      InputStream in2 = new FileInputStream(file2);
      
      wc.createRowsUsingCSVInputStreamBulkUpload(agg_url, appId, testTableId, tableSchemaETag, in2, batchSize);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVInputStreamBulkUploadWithLotsOfData_ExpectPass: expected pass");
    }
  }*/
  
  public void testCreateRowsUsingBulkUploadWithValidData_ExpectPass() {
    String testTableId = "test62";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String colValue = "/blah/blah/blah";
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", colValue);
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      ArrayList<Row> rowList = new ArrayList<Row>();
      rowList.add(row);
      wc.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);
      
      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 1);
      
      JSONObject jsonRow = rows.getJSONObject(0);
      // Now check that the row was created with the right rowId
      assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateOrUpdateRowWithValidData_ExpectPass: expected pass");
    }
  }

  public void testPushAllDataToUri_ExpectPass() {
    String dirToGetDataFrom = absolutePathOfTestFiles + "dataToUpload";
    String dirToPushDataFrom = absolutePathOfTestFiles + "downloadedData/pushAllDataToUri";
    String tableSchemaETag = null;
    String testTableId = "geotagger";
    ArrayList<String> filesUploaded;
    ArrayList<String> filesDownloaded;
    ArrayList<String> cleanFilesUploaded = new ArrayList<String>();
    ArrayList<String> cleanFilesDownloaded = new ArrayList<String>();
    String dsStore = ".DS_Store";

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);
      
      wc.pushAllDataToUri(agg_url, appId, dirToGetDataFrom, version);
      File fileDirToGetDataFrom = new File(dirToGetDataFrom);
      filesUploaded = wc.recurseDir(fileDirToGetDataFrom);

      File fileDirToPushDataFrom = new File(dirToPushDataFrom);
      deleteFolder(fileDirToPushDataFrom);
      wc.getAllDataFromUri(agg_url, appId, dirToPushDataFrom, version);
      filesDownloaded = wc.recurseDir(fileDirToPushDataFrom);
      
      // Remove any .DS_Store files
      for (int i = 0; i < filesDownloaded.size(); i++) {
        File tempFile = new File(filesDownloaded.get(i));
        String fileName = tempFile.getAbsolutePath().substring(tempFile.getAbsolutePath().lastIndexOf("/")+1);
        if (!fileName.equals(dsStore)) {
          cleanFilesDownloaded.add(filesDownloaded.get(i));
        }
      }
      
      // Remove any .DS_Store files
      for (int i = 0; i < filesUploaded.size(); i++) {
        File tempFile = new File(filesUploaded.get(i));
        String fileName = tempFile.getAbsolutePath().substring(tempFile.getAbsolutePath().lastIndexOf("/")+1);
        if (!fileName.equals(dsStore)) {
          cleanFilesUploaded.add(filesUploaded.get(i));
        }
      }
      
      assertEquals(cleanFilesDownloaded.size(), cleanFilesUploaded.size());
      
      // Delete the geotagger table defintion
      JSONObject result = wc.getTable(agg_url, appId, testTableId);

      if (result.containsKey("schemaETag")) {
        tableSchemaETag = result.getString("schemaETag");
      }
      
      // Delete geotagger from the server
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      // Make sure the table is gone
      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      // Clean up all the files after 
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId, version);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        
        // After we are done clean up the file
        wc.deleteFile(agg_url, appId, fileName, version);
      }
      
      // Make sure that all app level files are gone 
      obj = wc.getManifestForAppLevelFiles(agg_url, appId, version);
      files = obj.getJSONArray("files");
      
      assertEquals(files.size(), 0);
      
      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testPushAllDataToUri_ExpectPass: expected pass");
    }
  }
  
//  public void testGetUsersWhenUserExists_ExpectPass() {
//
//    try {
//      WinkClient wc = new WinkClient();
//      wc.init(host, userName, password);
//
//      ArrayList<Map<String,Object>> result = wc.getUsers(agg_url);
//     
//      assertNotNull(result);
//      
//      wc.close();
//
//    } catch (Exception e) {
//      e.printStackTrace();
//      TestCase.fail("testGetUsersWhenUserExists_ExpectPass: expected pass for getting users");
//    }
//  }
  
  public void testUploadPermissionCSVWithValidUser_ExpectPass() {
    String relativeTestFilePath = "permissions/perm-file.csv";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    String testUserName = "mailto:testerodk@gmail.com";
    String userIdStr = "user_id";
    boolean foundUser = false;

    try {
      WinkClient wc = new WinkClient();
      wc.init(host, userName, password);

      int rspCode = wc.uploadPermissionCSV(agg_url, appId, testFile);
      
      System.out.println("rspCode = " + rspCode);
      
      ArrayList<Map<String,Object>> result = wc.getUsers(agg_url);
      
      if (result != null) {
        
        assertEquals(rspCode, 200);
        
        for(int i = 0; i < result.size(); i++) {
          Map<String,Object> userMap = result.get(i);
          if(userMap.containsKey(userIdStr) && testUserName.equals(userMap.get(userIdStr))) {
            foundUser = true;
            break;
          }
        }
        
        assertTrue(foundUser);
      }

      wc.close();

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testUploadPermissionCSVWithValidUser_ExpectPass: expected pass uploading user permissions");
    }
  }
}
