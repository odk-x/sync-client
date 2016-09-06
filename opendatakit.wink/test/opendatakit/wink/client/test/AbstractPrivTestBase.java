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
import org.opendatakit.wink.client.WinkClient;

import junit.framework.TestCase;

public abstract class AbstractPrivTestBase extends TestCase {
	String agg_url;
	String appId;
	String absolutePathOfTestFiles;
	String host;
	int batchSize;
	String version;
	
	abstract WinkClient createNewSyncPrivClient();


	abstract WinkClient createNewAdminPrivClient();

	/*
	 * Perform setup for test if necessary
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		// agg_url = System.getProperty("test.aggUrl");
		// appId = System.getProperty("test.appId");
		// absolutePathOfTestFiles =
		// System.getProperty("test.absolutePathOfTestFiles");
		// batchSize = Integer.valueOf(System.getProperty("test.batchSize"));

		agg_url = "";
		appId = "odktables/default";
		absolutePathOfTestFiles = "testfiles/test/";
		batchSize = 1000;
		URL url = new URL(agg_url);
		host = url.getHost();
		version = "2";
	}

	

	/*
	 * Check preConditions if necessary
	 */
	public void testPreConditions() {

	}

	
	private boolean checkThatFileExistsOnServer(String agg_url, String appId, String relativeFileNameOnServer) {
		// Make sure the server has added the file
		boolean found = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			JSONObject obj = syncPrivClient.getManifestForAppLevelFiles(agg_url, appId, version);
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
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		return found;
	}

	public boolean checkThatTableLevelFileExistsOnServer(String agg_url, String appId, String tableId,
			String relativeFileNameOnServer) {
		// Make sure the server has added the file
		boolean found = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			JSONObject obj = syncPrivClient.getManifestForTableId(agg_url, appId, tableId, version);
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
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		return found;
	}

	public boolean checkThatInstanceFileExistsOnServer(String agg_url, String appId, String tableId, String schemaETag,
			String rowId, String relativeFileNameOnServer) {
		// Make sure the server has added the file
		boolean found = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			JSONObject obj = syncPrivClient.getManifestForRow(agg_url, appId, tableId, schemaETag, rowId);
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
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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
		if (files != null) {
			for (File f : files) {
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

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			JSONObject obj = syncPrivClient.getTables(agg_url, appId);

			JSONArray tables = obj.getJSONArray("tables");

			for (int i = 0; i < tables.size(); i++) {
				JSONObject table = tables.getJSONObject(i);
				if (tableId.equals(table.getString("tableId"))) {
					if (schemaETag.equals(table.getString("schemaETag"))) {
						exists = true;
					}
				}
			}
			syncPrivClient.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		return exists;
	}

	/*
	 * Test uploading binary file to server
	 */
	public void testUploadFileWithValidBinaryFile_ExpectFail() {

		String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean foundFile = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Make sure that the file is on the server
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testUploadFileWithValidBinaryFile_ExpectFail: " + testFile);
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test uploading ASCII file to server
	 */
	public void testUploadFileWithValidAsciiFile_ExpectFail() {

		String relativeTestFilePath = "assets/index.html";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean foundFile = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Make sure that the file is on the server
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testUploadFileWithValidAsciiFile_ExpectPass: expected pass for " + testFile);
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test uploadFile when uri is null
	 */
	public void testUploadFileWhenUriIsNull_ExpectFail() {

		String relativeTestFilePath = "assets/index.html";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean thrown = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.uploadFile(null, appId, testFile, relativeTestFilePath, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * Test downloading binary file to server
	 */
	public void testDownloadFileWithValidBinaryFile_ExpectFail() {
		String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;

		String dowloadTestFile = absolutePathOfTestFiles + "testDownload/" + relativeTestFilePath;
		boolean sameFile = false;
		boolean foundFile = false;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// Put the file on the server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Get the file off of the server
			syncPrivClient.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath, version);

			// Check the data from the two files are the same
			sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

			assertTrue(sameFile);

			// After we are done clean up the file
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail(
					"testDownloadFileWithValidBinaryFile_ExpectPass: expected pass for downloading file " + testFile);
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// Put the file on the server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Get the file off of the server
			syncPrivClient.downloadFile(agg_url, appId, dowloadTestFile, relativeTestFilePath, version);

			// Check that the data from the two files are the same
			sameFile = checkThatTwoFilesAreTheSame(testFile, dowloadTestFile);

			assertTrue(sameFile);

			// After we are done clean up the file
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail(
					"testDownloadFileWithValidAsciiFile_ExpectPass: expected pass for downloading file " + testFile);
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test downloadFile when uri is null
	 */
	public void testDownloadFileWhenUriIsNull_ExpectFail() {

		String relativeTestFilePath = "assets/index.html";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean thrown = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.downloadFile(null, appId, testFile, relativeTestFilePath, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
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

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.downloadFile(agg_url, appId, null, relativeTestFilePath, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
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

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Check the data from the file
			syncPrivClient.downloadFile(agg_url, appId, testFile, null, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * Test delete binary file from server
	 */
	public void testDeleteFileWithValidBinaryFile_ExpectFail() {
		String relativeTestFilePath = "assets/img/spaceNeedle_CCLicense_goCardUSA.jpg";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean foundFile = false;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// Put the file on the server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Check that the server has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertTrue(foundFile);

			// Check what happens when anonyous tries to delete
			syncPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertTrue(foundFile);

			// After we are done clean up the file
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteFileWithValidBinaryFile_ExpectFail: expected fail for deleting file " + testFile);
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test delete ASCII file from server
	 */
	public void testDeleteFileWithValidAsciiFile_ExpectFail() {
		String relativeTestFilePath = "assets/index.html";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean foundFile = false;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// Put the file on the server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Check that the server has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertTrue(foundFile);

			// Check what happens when anonyous tries to delete
			syncPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertTrue(foundFile);

			// After we are done clean up the file
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteFileWithValidBinaryFile_ExpectFail: expected fail for deleting file " + testFile);
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test getting the manifest for app level files with valid files
	 */
	public void testGetManifestForAppLevelFilesWithValidFile_ExpectPass() {
		String relativeTestFilePath = "assets/index.html";
		String testFile = absolutePathOfTestFiles + relativeTestFilePath;
		boolean foundFile = false;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// put file on server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Test the manifest
			JSONObject obj = syncPrivClient.getManifestForAppLevelFiles(agg_url, appId, version);
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
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail(
					"testGetManifestForAppLevelFilesWithValidFile_ExpectPass: expected pass for manifest with file "
							+ testFile);
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test getting the manifest for app level files when there are no files
	 */
	public void testGetManifestForAppLevelFilesWithNoFiles_ExpectPass() {
		String relativeTestFilePath = "assets/index.html";
		boolean foundFile = false;

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			// Test the manifest
			JSONObject obj = syncPrivClient.getManifestForAppLevelFiles(agg_url, appId, version);
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
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test getting the manifest for app level files when uri is null
	 */
	public void testGetManifestForAppLevelFilesWhenUriIsNull_ExpectFail() {
		boolean thrown = false;

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			// Test the manifest
			syncPrivClient.getManifestForAppLevelFiles(null, appId, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
		assertTrue(thrown);
	}

	/*
	 * Test getting the manifest for app level files when uri is null
	 */
	public void tesGetAllAppLevelFilesFromUri_ExpectFail() {
		boolean thrown = false;

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			// Test the manifest
			syncPrivClient.getManifestForAppLevelFiles(null, appId, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			// Put the file on the server
			adminPrivClient.uploadFile(agg_url, appId, testFile, relativeTestFilePath, version);

			// Get the file off of the server
			syncPrivClient.getAllAppLevelFilesFromUri(agg_url, appId, dowloadTestDir, version);

			// Check the data from the two files are the same
			sameFile = checkThatTwoFilesAreTheSame(testFile, downloadTestFile);

			assertTrue(sameFile);

			// After we are done clean up the file
			adminPrivClient.deleteFile(agg_url, appId, relativeTestFilePath, version);

			// Make sure the server no longer has the file
			foundFile = checkThatFileExistsOnServer(agg_url, appId, relativeTestFilePath);

			assertFalse(foundFile);

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("tesGetAllAppLevelFilesFromUri_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	/*
	 * Test getting the app level files when uri is null
	 */
	public void testGetAllAppLevelFilesFromUriWhenUriIsNull_ExpectFail() {
		boolean thrown = true;
		String dowloadTestDir = absolutePathOfTestFiles + "testAppLevelFiles/";

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			// Get the file off of the server
			syncPrivClient.getAllAppLevelFilesFromUri(null, appId, dowloadTestDir, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * Test getting the app level files when dir is null
	 */
	public void testGetAllAppLevelFilesFromUriWhenDirToSaveIsNull_ExpectFail() {
		boolean thrown = true;

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			// Get the file off of the server
			syncPrivClient.getAllAppLevelFilesFromUri(agg_url, appId, null, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * test createTable with string value
	 */
	public void testCreateTableWithString_ExpectFail() {
		boolean thrown = false;
		String testTableId = "test0";
		String colName = "scan_output_directory";
		String colKey = "scan_output_directory";
		String colType = "string";

		String testTableSchemaETag = "testCreateTableWithString_ExpectPass";
		String listOfChildElements = "[]";

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			syncPrivClient.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * test createTableWithCSV with valid data
	 */
	public void testCreateTableWithCSVAndValidData_ExpectFail() {
		boolean thrown = false;
		String testTableId = "test3";
		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			syncPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * test createTableWithCSV with valid data
	 */
	public void testCreateTableWithCSVInputStream_ExpectFail() {
		boolean thrown = false;
		String testTableId = "test4";
		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			File file = new File(csvFile);

			InputStream in = new FileInputStream(file);

			syncPrivClient.createTableWithCSVInputStream(agg_url, appId, testTableId, null, in);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
	}

	/*
	 * test deleteTableDefintion when the table is valid
	 */
	public void testDeleteTableDefinitionWithValidValues_ExpectFail() {
		String testTableId = "test5";
		String colName = "scan_output_directory";
		String colKey = "scan_output_directory";
		String colType = "string";

		String tableSchemaETag = null;
		String listOfChildElements = "[]";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			int responsecode = syncPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertTrue(responsecode == 401);

			assertTrue(doesTableExistOnServer(testTableId, tableSchemaETag));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail(
					"testDeleteTableDefinitionWithValidValues_ExpectPass: expected pass deleting table defintion");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			JSONObject obj = syncPrivClient.getTables(agg_url, appId);
			JSONArray tables = obj.getJSONArray("tables");

			for (int i = 0; i < tables.size(); i++) {
				JSONObject table = tables.getJSONObject(i);
				if (testTableId.equals(table.getString("tableId"))) {
					found = true;
					break;
				}
			}

			assertTrue(found);

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetTablesWhenTableExists_ExpectPass: expected pass for getting table");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testGetTableWhenTableExists_ExpectPass() {
		String testTableId = "test8";
		String colName = "scan_output_directory";
		String colKey = "scan_output_directory";
		String colType = "string";

		String tableSchemaETag = null;
		String listOfChildElements = "[]";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			JSONObject obj = syncPrivClient.getTable(agg_url, appId, testTableId);
			assertEquals(testTableId, obj.getString("tableId"));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetTableWhenTableExists_ExpectPass: expected pass for getting table");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testGetTableWhenTableDoesNotExists_ExpectPass() {
		String testTableId = "test81";

		WinkClient syncPrivClient = null;
		try {
			syncPrivClient = createNewSyncPrivClient();

			JSONObject obj = syncPrivClient.getTable(agg_url, appId, testTableId);

			assertNull(obj);
		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetTableWhenTableExists_ExpectPass: expected pass for getting table");
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testWriteTableDefinitionToCSVWhenTableExists_ExpectPass() {
		String testTableId = "test9";
		String colName = "scan_output_directory";
		String colKey = "scan_output_directory";
		String colType = "string";

		String tableSchemaETag = null;
		String listOfChildElements = "[]";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			String testCSVFile = absolutePathOfTestFiles + "writeTest.csv";
			syncPrivClient.writeTableDefinitionToCSV(agg_url, appId, testTableId, tableSchemaETag, testCSVFile);

			checkThatTableDefAndCSVDefAreEqual(testCSVFile, result);

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testWriteTableDefinitionToCSVWhenTableExists_ExpectPass: expected pass for getting table");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testGetTableDefinitionWhenTableExists_ExpectPass() {
		String testTableId = "test10";
		String colName = "scan_output_directory";
		String colKey = "scan_output_directory";
		String colType = "string";

		String tableSchemaETag = null;
		String listOfChildElements = "[]";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			JSONObject obj = syncPrivClient.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			// Check that the table definition has the right table id
			if (obj.containsKey("tableId")) {
				String tableId = obj.getString("tableId");
				assertEquals(tableId, testTableId);
			}

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetTableDefinitionWhenTableExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject rowRes = syncPrivClient.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, rowRes));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetRowWhenRowExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testCreateRowsUsingCSVWithValidFile_ExpectPass() {
		String testTableId = "test12";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testCreateRowsUsingCSVWithValidFile_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			syncPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

			JSONObject res = syncPrivClient.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 13);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testCreateRowsUsingCSVWithValidFile_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testGetRowsWhenRowsExist_ExpectPass() {
		String testTableId = "test13";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testGetRowsWhenRowsExist_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			adminPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

			JSONObject res = syncPrivClient.getRows(agg_url, appId, testTableId, tableSchemaETag, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 13);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetRowsWhenRowsExist_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testGetRowsSinceWhenRowsExist_ExpectPass() {
		String testTableId = "test14";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testGetRowsSinceWhenRowsExist_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			adminPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
					null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 13);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetRowsSinceWhenRowsExist_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testWriteRowDataToCSVWhenRowsExist_ExpectPass() {
		String testTableId = "test15";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.updated.csv";
		String csvOutputFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.output.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testWriteRowDataToCSVWhenRowsExist_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			adminPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 0);

			syncPrivClient.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvOutputFile);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			adminPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);

			JSONObject res = adminPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);

			String dataETag = res.getString("dataETag");
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			// Now delete the row
			Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString("rowETag"), row.getFormId(), row.getLocale(),
					row.getSavepointType(), row.getSavepointTimestamp(), row.getSavepointCreator(),
					row.getRowFilterScope(), row.getValues());

			// Make sure that all of these rows are marked for deletion
			rowObj.setDeleted(true);
			ArrayList<Row> rowList = new ArrayList<Row>();
			rowList.add(rowObj);
			syncPrivClient.deleteRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, dataETag, rowList,
					0);

			// Check that the row was deleted
			JSONObject rowRes = syncPrivClient.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
			assertTrue(rowRes.getBoolean("deleted"));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		// manufacture a rowId for this record... String RowId = "uuid:" +
		UUID.randomUUID().toString();

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			// Add a file for table

			// TODO add file code

//			syncPrivClient.getManifestForTableId(agg_url, tableSchemaETag, testTableId, version);

			// TODO add check code

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetManifestForTableIdWhenTableFileExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			adminPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
					null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			// Get the manifest to check that the file is there
			JSONObject obj = syncPrivClient.getManifestForRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

			// Make sure there are no files returned
			JSONArray files = obj.getJSONArray("files");
			int numOfFiles = files.size();

			assertEquals(numOfFiles, 0);

			// Delete the table and all data
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetManifestForRowWithNoFiles_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null,
					null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			// Put file for row
			syncPrivClient.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
					relativePathOnServer);

			// Make sure that the file is on the server foundFile =
			foundFile = checkThatInstanceFileExistsOnServer(agg_url, appId, testTableId, tableSchemaETag, RowId,
					relativePathOnServer);

			assertTrue(foundFile);

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

			adminPrivClient.close();

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testPutFileForRowWithValidBinaryFile_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			adminPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = adminPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			// Put file for row
			adminPrivClient.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
					relativePathOnServer);

			// Make sure that the file is on the server
			foundFile = checkThatInstanceFileExistsOnServer(agg_url, appId, testTableId, tableSchemaETag, RowId,
					relativePathOnServer);

			assertTrue(foundFile);

			// Now get the file
			syncPrivClient.getFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, false, pathToSaveFile,
					relativePathOnServer);

			boolean sameFile = checkThatTwoFilesAreTheSame(wholePathToFile, pathToSaveFile);

			assertTrue(sameFile);

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetFileForRowWithValidBinaryFile_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}
	}

	public void testWriteRowDataToCSVWhenNoRowsExist_ExpectPass() {
		String testTableId = "test22";
		String tableSchemaETag = null;

		String relativeFileNameOnServer = "geotagger.shouldnotexist.csv";
		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvOutputFile = absolutePathOfTestFiles + "geotaggerTest/" + relativeFileNameOnServer;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testWriteRowDataToCSVWhenRowsExist_ExpectPass: result of create table is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			syncPrivClient.writeRowDataToCSV(agg_url, appId, testTableId, tableSchemaETag, csvOutputFile);

			File csvOutFile = new File(csvOutputFile);

			assertFalse(csvOutFile.exists());

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testWriteRowDataToCSVWhenRowsExist_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

	}

	/*
	 * test createTableWithJSON with valid data
	 */
	public void testCreateTableWithJSON_ExpectFail() {
		boolean thrown = false;
		String testTableId = "test23";
		String jsonString = "{\"orderedColumns\":[{\"elementKey\":\"Date_and_Time\",\"elementType\":\"dateTime\",\"elementName\":\"Date_and_Time\",\"listChildElementKeys\":\"[]\"}],\"tableId\":\"testDate\",\"schemaETag\":null}";

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			syncPrivClient.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithJSON(agg_url, appId, testTableId, null, jsonString);
			System.out.println("testCreateRowsUsingJSONBulkUpload_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			// Get the table definition
			JSONObject tableDef = syncPrivClient.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

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
			syncPrivClient.createRowsUsingJSONBulkUpload(agg_url, appId, testTableId, tableSchemaETag, jsonRows, testBatchSize);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), testSize);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testCreateRowsUsingJSONBulkUpload_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			String eTag = syncPrivClient.getSchemaETagForTable(agg_url, appId, testTableId);

			assertEquals(tableSchemaETag, eTag);

			JSONObject tableDef = syncPrivClient.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
			if (tableDef.containsKey("orderedColumns")) {
				JSONArray cols = tableDef.getJSONArray("orderedColumns");
				for (int i = 0; i < cols.size(); i++) {
					JSONObject col = cols.getJSONObject(i);
					String testElemKey = col.getString("elementKey");
					assertEquals(colKey, testElemKey);
				}
			}

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
			
		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testGetSchemaETagForTable_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			JSONObject tableDef = syncPrivClient.getTableDefinition(agg_url, appId, testTableId, tableSchemaETag);
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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);
			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, utf_val, jsonRow));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testCreateOrUpdateRowWithUTF8_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

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

			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 0);

			JSONObject res = syncPrivClient.queryRowsInTimeRangeWithLastUpdateDate(agg_url, appId, testTableId, tableSchemaETag,
					startTime, null, null, null);

			if (res.containsKey("rows")) {
				JSONArray rowsObj = res.getJSONArray("rows");
				assertEquals(rowsObj.size(), sizeOfSeqTable);

			}

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testQueryRowsInTimeRangeWithLastUpdateDate_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, testTableSchemaETag, columns);

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

			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 0);

			JSONObject res = syncPrivClient.queryRowsInTimeRangeWithSavepointTimestamp(agg_url, appId, testTableId, tableSchemaETag,
					startTime, null, null, null);

			if (res.containsKey("rows")) {
				JSONArray rowsObj = res.getJSONArray("rows");
				assertEquals(rowsObj.size(), sizeOfSeqTable);

			}

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testQueryRowsInTimeRangeWithSavepointTimestamp_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);

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
			Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString(WinkClient.ROW_ETAG_JSON), row.getFormId(),
					jsonRow.getString(WinkClient.LOCALE_JSON), jsonRow.getString(WinkClient.SAVEPOINT_TYPE_JSON),
					jsonRow.getString(WinkClient.SAVEPOINT_TIMESTAMP_JSON),
					jsonRow.getString(WinkClient.SAVEPOINT_CREATOR_JSON), row.getRowFilterScope(), dkvl);

			ArrayList<Row> rowList = new ArrayList<Row>();
			rowList.add(rowObj);
			syncPrivClient.updateRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, dataETag, rowList, 0);

			// Check that the row was updated
			JSONObject rowRes = syncPrivClient.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
			assertTrue(checkThatRowHasColValue(colValue2, rowRes));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowArrayList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);

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
			Row rowObj = Row.forUpdate(row.getRowId(), jsonRow.getString(WinkClient.ROW_ETAG_JSON), row.getFormId(),
					jsonRow.getString(WinkClient.LOCALE_JSON), jsonRow.getString(WinkClient.SAVEPOINT_TYPE_JSON),
					jsonRow.getString(WinkClient.SAVEPOINT_TIMESTAMP_JSON),
					jsonRow.getString(WinkClient.SAVEPOINT_CREATOR_JSON), row.getRowFilterScope(), dkvl);

			ArrayList<Row> rowList = new ArrayList<Row>();
			rowList.add(rowObj);
			RowOutcomeList outcomeList = syncPrivClient.alterRowsUsingSingleBatch(agg_url, appId, testTableId, tableSchemaETag,
					dataETag, rowList);

			ArrayList<RowOutcome> resultingRowOutcomeList = outcomeList.getRows();
			RowOutcome rowOutcome = resultingRowOutcomeList.get(0);

			// Check that the row was updated
			assertEquals(resultingRowOutcomeList.size(), 1);

			assertEquals(rowOutcome.getOutcome(), OutcomeType.SUCCESS);

			// Check that the row was updated
			JSONObject rowRes = syncPrivClient.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
			assertTrue(checkThatRowExists(RowId, colValue2, rowRes));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		String pathToSaveFile = absolutePathOfTestFiles + "downloadBatchInstance";
		String pathToVerify = pathToSaveFile + WinkClient.SEPARATOR_STR + relativePathOnServer;

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);

			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			// Put file for row
			syncPrivClient.putFileForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, wholePathToFile,
					relativePathOnServer);

			// Make sure that the file is on the server foundFile =
			foundFile = checkThatInstanceFileExistsOnServer(agg_url, appId, testTableId, tableSchemaETag, RowId,
					relativePathOnServer);

			assertTrue(foundFile);

			// Get the list of files
			JSONObject filesToGetObj = syncPrivClient.getManifestForRow(agg_url, appId, testTableId, tableSchemaETag, RowId);

			// Download files into pathToSaveFile
			syncPrivClient.batchGetFilesForRow(agg_url, appId, testTableId, tableSchemaETag, RowId, pathToSaveFile, filesToGetObj,
					0);

			assertTrue(checkThatTwoFilesAreTheSame(wholePathToFile, pathToVerify));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testBatchGetFileForRowWithValidBinaryFiles_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			RowOutcomeList outcomeList = syncPrivClient.alterRowsUsingSingleBatch(agg_url, appId, testTableId,
					tableSchemaETag, null, rowList);

			ArrayList<RowOutcome> resultingRowOutcomeList = outcomeList.getRows();
			RowOutcome rowOutcome = resultingRowOutcomeList.get(0);

			// Check that the row was updated
			assertEquals(resultingRowOutcomeList.size(), 1);

			assertEquals(rowOutcome.getOutcome(), OutcomeType.SUCCESS);

			// Check that the row was updated
			JSONObject rowRes = syncPrivClient.getRow(agg_url, appId, testTableId, tableSchemaETag, RowId);
			assertTrue(checkThatRowExists(RowId, colValue2, rowRes));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testDeleteRowWhenRowExists_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}	
	}

	public void testCreateRowsUsingCSVBulkUpload_ExpectPass() {
		String testTableId = "test40";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "geotaggerTest/definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "geotaggerTest/geotagger.edited.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();

			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out.println("testCreateRowsUsingCSVBulkUpload_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			syncPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, 7);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 3);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testCreateRowsUsingCSVBulkUpload_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}	
	}

	public void testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass() {
		String testTableId = "test42";
		String tableSchemaETag = null;

		String csvFile = absolutePathOfTestFiles + "cookstoves/data_definition.csv";
		String csvDataFile = absolutePathOfTestFiles + "cookstoves/data_small.csv";

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();
			syncPrivClient = createNewSyncPrivClient();

			JSONObject result = adminPrivClient.createTableWithCSV(agg_url, appId, testTableId, null, csvFile);
			System.out
					.println("testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass: result is " + result);

			if (result.containsKey("tableId")) {
				String tableId = result.getString("tableId");
				assertEquals(tableId, testTableId);
				tableSchemaETag = result.getString("schemaETag");
			}

			syncPrivClient.createRowsUsingCSVBulkUpload(agg_url, appId, testTableId, tableSchemaETag, csvDataFile, batchSize);

			// Now delete the table
			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

		} catch (Exception e) {
			System.out.println("WAYLON IS AWESOME!!!");
			e.printStackTrace();
			TestCase.fail("testCreateRowsUsingCSVBulkUploadWithAMediumAmountOfData_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}	
	}


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

		WinkClient adminPrivClient = null;
		WinkClient syncPrivClient = null;

		try {
			adminPrivClient = createNewAdminPrivClient();
			syncPrivClient = createNewSyncPrivClient();

			ArrayList<Column> columns = new ArrayList<Column>();

			columns.add(new Column(colKey, colName, colType, listOfChildElements));

			JSONObject result = adminPrivClient.createTable(agg_url, appId, testTableId, null, columns);

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
			syncPrivClient.createRowsUsingBulkUpload(agg_url, appId, testTableId, tableSchemaETag, rowList, 1);

			JSONObject res = syncPrivClient.getRowsSince(agg_url, appId, testTableId, tableSchemaETag, null, null, null);
			JSONArray rows = res.getJSONArray("rows");

			assertEquals(rows.size(), 1);

			JSONObject jsonRow = rows.getJSONObject(0);
			// Now check that the row was created with the right rowId
			assertTrue(checkThatRowExists(RowId, colValue, jsonRow));

			adminPrivClient.deleteTableDefinition(agg_url, appId, testTableId, tableSchemaETag);

			assertFalse(doesTableExistOnServer(testTableId, tableSchemaETag));

		} catch (Exception e) {
			e.printStackTrace();
			TestCase.fail("testCreateOrUpdateRowWithValidData_ExpectPass: expected pass");
		} finally {
			if (adminPrivClient != null) {
				adminPrivClient.close();
			}
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}	
	}

	public void testPushAllDataToUri_ExpectFail() {
		boolean thrown = false;
		String dirToGetDataFrom = absolutePathOfTestFiles + "dataToUpload";

		WinkClient syncPrivClient = null;

		try {
			syncPrivClient = createNewSyncPrivClient();

			syncPrivClient.pushAllDataToUri(agg_url, appId, dirToGetDataFrom, version);

		} catch (Exception e) {
			e.printStackTrace();
			thrown = true;
		} finally {
			if (syncPrivClient != null) {
				syncPrivClient.close();
			}
		}

		assertTrue(thrown);	}

}
