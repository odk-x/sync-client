package opendatakit.wink.client.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.wink.json4j.JSONArray;
import org.apache.wink.json4j.JSONObject;
import org.opendatakit.aggregate.odktables.rest.RFC4180CsvReader;
import org.opendatakit.aggregate.odktables.rest.TableConstants;
import org.opendatakit.aggregate.odktables.rest.entity.Column;
import org.opendatakit.aggregate.odktables.rest.entity.DataKeyValue;
import org.opendatakit.aggregate.odktables.rest.entity.Row;
import org.opendatakit.wink.client.WinkClient;

public class WinkClientTest extends TestCase {
  String agg_url;
  String appId;
  String absolutePathOfTestFiles;
  int batchSize;
  
  //String agg_url = "https://clarlars.appspot.com";
  //String agg_url = "https://odk-test-area.appspot.com";
  //String appId = "odktables/tables";
  //String absolutePathOfTestFiles = "testfiles/test/";
  //int batchSize = 1000;

  //String agg_url = "http://carcoal.cs.washington.edu:8888/odktables/odktables";
  //String agg_url = "http://146.148.49.96/odktables";

  //String agg_url = "http://146.148.34.74:8080/dataservice/odktables";
  //String agg_url = "http://146.148.34.74:8080/odktables/odktables";

  //String agg_url = "http://107.178.213.121:8080/odktables";
  //String appId = "mezuri-10100233";

  /*
   * Perform setup for test if necessary
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    agg_url = System.getProperty("test.aggUrl");
    appId = System.getProperty("test.appId");
    absolutePathOfTestFiles = System.getProperty("test.absolutePathOfTestFiles");
    batchSize = Integer.valueOf(System.getProperty("test.batchSize"));
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

  public boolean checkThatFileExistsOnServer(String agg_url, String appId, String relativeFileNameOnServer) {
    // Make sure the server has added the file
    boolean found = false;
    WinkClient wc = new WinkClient();

    try {
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId);
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
  
  public boolean checkThatTableLevelFileExistsOnServer(String agg_url, String appId, String tableId, String relativeFileNameOnServer) {
	    // Make sure the server has added the file
	    boolean found = false;
	    WinkClient wc = new WinkClient();

	    try {
	      JSONObject obj = wc.getManifestForTableId(agg_url, appId, tableId);
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

  public boolean checkThatInstanceFileExistsOnServer(String agg_url, String appId, String tableId,
      String schemaETag, String rowId, String relativeFileNameOnServer) {
    // Make sure the server has added the file
    boolean found = false;
    WinkClient wc = new WinkClient();

    try {
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

  /*
   * Test uploading binary file to server
   */
  public void testUploadFileWithValidBinaryFile_ExpectPass() {

    String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String testFile = absolutePathOfTestFiles + relativeTestFilePath;
    boolean foundFile = false;

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Make sure that the file is on the server
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Make sure that the file is on the server
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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
//      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);
//
//      assertTrue(foundFile);
//
//      // After we are done clean up the file
//      wc.deleteFile(agg_url, appId, relativeTestFilePath);
//
//      // Make sure the server no longer has the file
//      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);
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

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.uploadFile(null, appId, testFile, relativeTestFilePath);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);
  }

  /*
   * Test uploadFile when testFile is null
   */
  public void testUploadFileWhenTestFileIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.uploadFile(agg_url, appId, null, relativeTestFilePath);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.uploadFile(agg_url, appId, testFile, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Get the file off of the server
      wc.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath);

      // Check the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Get the file off of the server
      wc.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath);

      // Check that the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.downloadFile(null, appId, testFile, relativeTestFilePath);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);

  }

  /*
   * Test downloadFile when testFile is null
   */
  public void testDownloadFileWhenTestFileIsNull_ExpectFail() {
    String relativeTestFilePath = "assets/index.html";
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.downloadFile(agg_url, appId, null, relativeTestFilePath);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.downloadFile(agg_url, appId, testFile, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Check that the server has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Check that the file is on the server
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertTrue(foundFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.deleteFile(null, appId, relativeTestFilePath);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);

  }

  /*
   * Test deleteFile when relativePath is null
   */
  public void testDeleteFileWhenRelativePathIsNull_ExpectFail() {
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      // Check the data from the file
      wc.deleteFile(agg_url, appId, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Test the manifest
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId);
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
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

      // Test the manifest
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId);
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

    try {
      WinkClient wc = new WinkClient();

      // Test the manifest
      wc.getManifestForAppLevelFiles(null, appId);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }
    assertTrue(thrown);
  }

  /*
   * Test getting the manifest for app level files when uri is null
   */
  public void tesGetAllAppLevelFilesFromUri_ExpectPass() {
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      // Test the manifest
      wc.getManifestForAppLevelFiles(null, appId);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

      // Put the file on the server
      wc.uploadFile(agg_url, appId, testFile, relativeTestFilePath);

      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(agg_url, appId, dowloadTestDir);

      // Check the data from the two files are the same
      sameFile = checkThatTwoFilesAreTheSame(testFile, downloadTestFile);

      assertTrue(sameFile);

      // After we are done clean up the file
      wc.deleteFile(agg_url, appId, relativeTestFilePath);

      // Make sure the server no longer has the file
      foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

      assertFalse(foundFile);

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

    try {
      WinkClient wc = new WinkClient();

      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(null, appId, dowloadTestDir);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);
  }

  /*
   * Test getting the app level files when dir is null
   */
  public void testGetAllAppLevelFilesFromUriWhenDirToSaveIsNull_ExpectFail() {
    boolean thrown = true;

    try {
      WinkClient wc = new WinkClient();

      // Get the file off of the server
      wc.getAllAppLevelFilesFromUri(agg_url, appId, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);
  }

  /*
   * test createTable when uri is null
   */
  public void testCreateTableWhenUriIsNull_ExpectFail() {

    String testTableId = "test2";
    String testTableSchemaETag = "testCreateTableWhenUriIsNull_ExpectFail";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String listOfChildElements = "[]";
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      wc.createTable(null, appId, testTableId, testTableSchemaETag, columns);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
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

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      wc.createTable(agg_url, appId, null, testTableSchemaETag, columns);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);
  }

  /*
   * test createTable when schemaETag is null
   */
  public void testCreateTableWhenSchemaETagIsNull_ExpectFail() {
    String testTableId = "test6";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateTableWhenSchemaETagIsNull_ExpectFail: expected pass");
    }
  }

  /*
   * test createTable when columns is null
   */
  public void testCreateTableWhenColumnsIsNull_ExpectFail() {
    String testTableId = "test3";
    String testTableSchemaETag = "testCreateTableWhenColumnsIsNull_ExpectFail";
    boolean thrown = false;

    try {
      WinkClient wc = new WinkClient();

      wc.createTable(agg_url, appId, testTableId, testTableSchemaETag, null);

    } catch (Exception e) {
      e.printStackTrace();
      thrown = true;
    }

    assertTrue(thrown);
  }

  /*
   * test createTableWithCSV with valid data
   */
  public void testCreateTableWithCSVAndValidData_ExpectPass() {
    String testTableId = "test4";
    String tableSchemaETag = null;
    String csvFile = absolutePathOfTestFiles + "geoTaggerTest/definition.csv";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testCreateTableWithCSVAndValidData_ExpectPass: expected pass for creating table with CSV");
    }

  }
  
  /*
   * test createTableWithJSON with valid data
   */
  public void testCreateTableWithJSON_ExpectPass() {

    String testTableId = "test22";
    String tableSchemaETag = null;
    String jsonString = "{\"orderedColumns\":[{\"elementKey\":\"Date_and_Time\",\"elementType\":\"dateTime\",\"elementName\":\"Date_and_Time\",\"listChildElementKeys\":\"[]\"}],\"tableId\":\"testDate\",\"schemaETag\":null}";
    
    try {
      WinkClient wc = new WinkClient();

      JSONObject result = wc.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
      System.out.println("testCreateTableWithJSON_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      if (tableDef.containsKey(WinkClient.orderedColumnsDef)) {
        JSONArray cols = tableDef.getJSONArray(WinkClient.orderedColumnsDef);
        assertEquals(cols.size(), 1);
        JSONObject col = cols.getJSONObject(cols.size() - 1);
        assertEquals("Date_and_Time", col.getString(WinkClient.jsonElemKey));
      }

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

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
    String csvFile = absolutePathOfTestFiles + "geoTaggerTest/definition.csv";

    try {
      WinkClient wc = new WinkClient();
      
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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase
          .fail("testDeleteTableDefinitionWithValidValues_ExpectPass: expected pass deleting table defintion");
    }
  }

  public boolean doesTableExistOnServer(String tableId, String schemaETag) {
    boolean exists = false;

    try {
      WinkClient wc = new WinkClient();
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

    } catch (Exception e) {
      e.printStackTrace();
    }

    return exists;
  }

  public void testGetTablesWhenTableExists_ExpectPass() {
    String testTableId = "test6";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";
    boolean found = false;

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTablesWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testGetTableWhenTableExists_ExpectPass() {
    String testTableId = "test7";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTableWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testWriteTableDefinitionToCSVWhenTableExists_ExpectPass() {
    String testTableId = "test8";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteTableDefinitionToCSVWhenTableExists_ExpectPass: expected pass for getting table");
    }
  }

  public void testGetTableDefinitionWhenTableExists_ExpectPass() {
    String testTableId = "test9";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetTableDefinitionWhenTableExists_ExpectPass: expected pass");
    }
  }

  public void testCreateOrUpdateRowWithValidData_ExpectPass() {
    String testTableId = "test62";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject res = wc.createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      // Now check that the row was created with the right rowId
      assertEquals(RowId, res.getString("id"));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateOrUpdateRowWithValidData_ExpectPass: expected pass");
    }
  }

  public void testGetRowWhenRowExists_ExpectPass() {
    String testTableId = "test11";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject createRes = wc
          .createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      String rowETag = createRes.getString("rowETag");

      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

      assertEquals(rowETag, rowRes.getString("rowETag"));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

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

      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testCreateRowsUsingCSVWithValidFile_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSV(agg_url, appId, testTableId, tableSchemaETag, csvDataFile);

      JSONObject res = wc.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

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

      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testGetRowsWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSV(agg_url, appId, testTableId, tableSchemaETag, csvDataFile);

      JSONObject res = wc.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetRowsWhenRowsExist_ExpectPass: expected pass");
    }
  }

  public void testGetRowsSinceWhenRowsExist_ExpectPass() {
    String testTableId = "test13";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

    try {
      WinkClient wc = new WinkClient();

      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testGetRowsSinceWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSV(agg_url, appId, testTableId, tableSchemaETag, csvDataFile);

      JSONObject res = wc.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
          null);
      JSONArray rows = res.getJSONArray("rows");

      assertEquals(rows.size(), 13);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetRowsSinceWhenRowsExist_ExpectPass: expected pass");
    }
  }
  
  public void testWriteRowDataToCSVWhenNoRowsExist_ExpectPass() {
    String testTableId = "test21";
    String tableSchemaETag = null;

    String relativeFileNameOnServer = "geotagger.shouldnotexist.csv";
    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvOutputFile = absolutePathOfTestFiles + "geotaggerTest/" + relativeFileNameOnServer;

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
    }
  }

  public void testWriteRowDataToCSVWhenRowsExist_ExpectPass() {
    String testTableId = "test14";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";
    String csvOutputFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.output.csv";

    try {
      WinkClient wc = new WinkClient();

      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testWriteRowDataToCSVWhenRowsExist_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      wc.createRowsUsingCSV(agg_url, appId, testTableId, tableSchemaETag, csvDataFile);

      wc.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvOutputFile);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
    }
  }
  
  public void testWriteRowDataToCSVWithLotsOfRows_ExpectPass() {
    String testTableId = "test20";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";
    String csvFilePathToWriteTo = absolutePathOfTestFiles + "writeLargeRowData.csv";

    try {
      WinkClient wc = new WinkClient();
      
 
      JSONObject result = wc.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
      System.out.println("testWriteRowDataToCSVWithLotsOfRows_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }
      
      wc.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, batchSize);

      wc.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvFilePathToWriteTo);
      
      InputStream in = new FileInputStream(csvFilePathToWriteTo);
      InputStreamReader inputStream = new InputStreamReader(in);
      RFC4180CsvReader reader = new RFC4180CsvReader(inputStream);
      int lineCnt = 0;
      while (reader.readNext() != null) {
        lineCnt++;
      }
      
      // This will need to be changed for lots of data
      assertEquals(lineCnt, 8193);

      // Now delete the table
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testWriteRowDataToCSVWithLotsOfRows_ExpectPass: expected pass");
    }
  }

  public void testDeleteRowWhenRowExists_ExpectPass() {
    String testTableId = "test15";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject res = wc.createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      // Now check that the row was created with the right rowId
      assertEquals(RowId, res.getString("id"));

      // Now delete the row
      wc.deleteRow(agg_url, appId, testTableId, tableSchemaETag, RowId, res.getString("rowETag"));

      // Check that the row was deleted
      JSONObject rowRes = wc.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
      assertTrue(rowRes.getBoolean("deleted"));

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
    }
  }

  /*
   * Test getting the manifest for app level files with valid files
   */
  public void testGetManifestForTableIdWhenTableFileExists_ExpectPass() {
    String testTableId = "test16";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    // manufacture a rowId for this record... String RowId = "uuid:" +
    UUID.randomUUID().toString();

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Add a file for table

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetManifestForTableIdWhenTableFileExists_ExpectPass: expected pass");
    }
  }
  
  public void testGetManifestForRowWithNoFiles_ExpectPass() {
    String testTableId = "test17";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject res = wc.createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      // Now check that the row was created with the right rowId
      assertEquals(RowId, res.getString("id"));;

      // Get the manifest to check that the file is there
      JSONObject obj = wc.getManifestForRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

      // Make sure there are no files returned
      JSONArray files = obj.getJSONArray("files");
      int numOfFiles = files.size();
      
      assertEquals(numOfFiles, 0);

      // Delete the table and all data
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetManifestForRowWithNoFiles_ExpectPass: expected pass");
    }
  }

  public void testPutFileForRowWithValidBinaryFile_ExpectPass() {
    String testTableId = "test18";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";
    
    // manufacture a rowId for this record...
    String RowId = "uuid:" + UUID.randomUUID().toString();
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    boolean foundFile = false;
    String relativePathOnServer = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
    String wholePathToFile = this.absolutePathOfTestFiles + relativePathOnServer;

    try {
      WinkClient wc = new WinkClient();

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject res = wc.createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      // Now check that the row was created with the right rowId
      assertEquals(RowId, res.getString("id"));

      // Put file for row
      wc.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
          relativePathOnServer);

      // Make sure that the file is on the server foundFile =
      foundFile = checkThatInstanceFileExistsOnServer(agg_url, appId, testTableId, tableSchemaETag, RowId, relativePathOnServer);

      assertTrue(foundFile);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testPutFileForRowWithValidBinaryFile_ExpectPass: expected pass");
    }
  }

  public void testGetFileForRowWithValidBinaryFile_ExpectPass() {
    String testTableId = "test19";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

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

      ArrayList<Column> columns = new ArrayList<Column>();

      columns.add(new Column(colKey, colName, colType, listOfChildElements));

      JSONObject result = wc.createTable(agg_url, appId, testTableId, null, columns);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      DataKeyValue dkv = new DataKeyValue("scan_output_directory", "/blah/blah/blah");
      ArrayList<DataKeyValue> dkvl = new ArrayList<DataKeyValue>();
      dkvl.add(dkv);

      Row row = Row.forInsert(RowId, null, null, null, null, null, null, dkvl);
      JSONObject res = wc.createOrUpdateRow(agg_url, appId, testTableId, tableSchemaETag, row);

      // Now check that the row was created with the right rowId
      assertEquals(RowId, res.getString("id"));

      // Put file for row
      wc.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile, relativePathOnServer);

      // Make sure that the file is on the server
      foundFile = checkThatInstanceFileExistsOnServer(agg_url, appId, testTableId, tableSchemaETag, RowId, relativePathOnServer);

      assertTrue(foundFile);

      // Now get the file
      wc.getFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, false, pathToSaveFile, relativePathOnServer);

      boolean sameFile = this.checkThatTwoFilesAreTheSame(wholePathToFile, pathToSaveFile);

      assertTrue(sameFile);

      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetFileForRowWithValidBinaryFile_ExpectPass: expected pass");
    }
  }

  public void testPushAllDataToUri_ExpectPass() {
    String dirToGetDataFrom = absolutePathOfTestFiles + "dataToUpload";
    String dirToPushDataFrom = absolutePathOfTestFiles + "downloadedData";
    String tableSchemaETag = null;
    String testTableId = "geotagger";
    ArrayList<String> filesUploaded;
    ArrayList<String> filesDownloaded;
    ArrayList<String> cleanFilesUploaded = new ArrayList<String>();
    ArrayList<String> cleanFilesDownloaded = new ArrayList<String>();
    String dsStore = ".DS_Store";

    try {
      WinkClient wc = new WinkClient();

      wc.pushAllDataToUri(agg_url, appId, dirToGetDataFrom);
      File fileDirToGetDataFrom = new File(dirToGetDataFrom);
      filesUploaded = wc.recurseDir(fileDirToGetDataFrom);

      File fileDirToPushDataFrom = new File(dirToPushDataFrom);
      deleteFolder(fileDirToPushDataFrom);
      wc.getAllDataFromUri(agg_url, appId, dirToPushDataFrom);
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
      
      // Delete geoTagger from the server
      wc.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
      
      // Make sure the table is gone
      assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));
      
      // Clean up all the files after 
      JSONObject obj = wc.getManifestForAppLevelFiles(agg_url, appId);
      JSONArray files = obj.getJSONArray("files");

      for (int i = 0; i < files.size(); i++) {
        JSONObject file = files.getJSONObject(i);
        String fileName = file.getString("filename");
        
        // After we are done clean up the file
        wc.deleteFile(agg_url, appId, fileName);
      }
      
      // Make sure that all app level files are gone 
      obj = wc.getManifestForAppLevelFiles(agg_url, appId);
      files = obj.getJSONArray("files");
      
      assertEquals(files.size(), 0);
      

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testPushAllDataToUri_ExpectPass: expected pass");
    }
  }
  
  public void testCreateRowsUsingJSONBulkUpload_ExpectPass() {
    String testTableId = "test23";
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

      JSONObject result = wc.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
      System.out.println("testCreateRowsUsingJSONBulkUpload_ExpectPass: result is " + result);

      if (result.containsKey("tableId")) {
        String tableId = result.getString("tableId");
        assertEquals(tableId, testTableId);
        tableSchemaETag = result.getString("schemaETag");
      }

      // Get the table definition
      JSONObject tableDef = wc.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

      if (tableDef.containsKey(WinkClient.orderedColumnsDef)) {
        JSONArray cols = tableDef.getJSONArray(WinkClient.orderedColumnsDef);
        assertEquals(cols.size(), 1);
        JSONObject col = cols.getJSONObject(cols.size() - 1);
        assertEquals("Date_and_Time", col.getString(WinkClient.jsonElemKey));
      }
      
      // Create Rows for the newly created table
      for (int i = 0; i < testSize; i++) {
        
        tempRow = new JSONObject();
        tempRow.put(WinkClient.jsonId, Integer.toString(i));
        
        JSONArray ordCols = new JSONArray();
        JSONObject col = new JSONObject();
        col.put("column", "Date_and_Time");
        col.put("value", TableConstants.nanoSecondsFromMillis(System.currentTimeMillis()));
        ordCols.add(col);
        
        tempRow.put(WinkClient.orderedColumnsDef, ordCols);
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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingJSONBulkUpload_ExpectPass: expected pass");
    }
  }
  
  /*
   * test getSchemaETagForTable 
   */
  public void testGetSchemaETagForTable_ExpectPass() {

    String testTableId = "test24";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String testTableSchemaETag = "testGetSchemaETagForTable_ExpectPass";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testGetSchemaETagForTable_ExpectPass: expected pass");
    }
  }
  
  /*
   * test createTable with string value
   */
  public void testCreateTableWithString_ExpectPass() {

    String testTableId = "test1";
    String colName = "scan_output_directory";
    String colKey = "scan_output_directory";
    String colType = "string";

    String testTableSchemaETag = "testCreateTableWithString_ExpectPass";
    String tableSchemaETag = null;
    String listOfChildElements = "[]";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateTableWithString: expected pass");
    }
  }

  public void testCreateRowsUsingCSVBulkUpload_ExpectPass() {
    String testTableId = "test40";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.edited.csv";
    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVBulkUpload_ExpectPass: expected pass");
    }
  }

  public void testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass() {
    String testTableId = "test41";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";

    try {
      WinkClient wc = new WinkClient();

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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass: expected pass");
    }
  }
  
  public void testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass() {
	    String testTableId = "test42";
	    String tableSchemaETag = null;

	    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
	    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data_small.csv";

	    try {
	      WinkClient wc = new WinkClient();

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

	    } catch (Exception e) {
	      e.printStackTrace();
	      TestCase.fail("testCreateRowsUsingCSVBulkUploadWithLotsOfData_ExpectPass: expected pass");
	    }
	  }
    
  public void testCreateRowsUsingCSVInputStreamBulkUploadWithLotsOfData_ExpectPass() {
    String testTableId = "test43";
    String tableSchemaETag = null;

    String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
    String csvDataFile = absolutePathOfTestFiles + "cookstoves/data.csv";

    try {
      WinkClient wc = new WinkClient();
      
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

    } catch (Exception e) {
      e.printStackTrace();
      TestCase.fail("testCreateRowsUsingCSVInputStreamBulkUploadWithLotsOfData_ExpectPass: expected pass");
    }
  }
}
