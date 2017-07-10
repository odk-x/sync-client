package org.opendatakit.sync.client.test;

import org.opendatakit.sync.client.SyncClient;

public class SynchronizeUserTest extends AbstractPrivTestBase {
	
	private String adminUserName;
	private String adminPassword;
	
	private String syncUserName;
	private String syncUserPassword;
	
	SyncClient createNewSyncPrivClient() {
		SyncClient syncPrivClient = new SyncClient();
		syncPrivClient.init(host, syncUserName, syncUserPassword);
		return syncPrivClient;
	}


	SyncClient createNewAdminPrivClient() {
		SyncClient adminClient = new SyncClient();
		adminClient.init(host, adminUserName, adminPassword);
		return adminClient;
	}

	/*
	 * Perform setup for test if necessary
	 */
	@Override
	protected void setUp() throws Exception {
	  
		syncUserName = "syncpriv@mezuricloud.com";
		syncUserPassword = "testingTesting0123";
		
		adminUserName = "tester@mezuricloud.com";
		adminPassword = "testingTesting0123";
		
		super.setUp();
	}

	/*
	 * Perform tear down for tests if necessary
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}


}
