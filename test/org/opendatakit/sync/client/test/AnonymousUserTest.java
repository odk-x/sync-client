package org.opendatakit.sync.client.test;

import org.opendatakit.sync.client.SyncClient;

public class AnonymousUserTest extends AbstractPrivTestBase {
	
	private String adminUserName;
	private String adminPassword;
	
	SyncClient createNewSyncPrivClient() {
		SyncClient anonymousClient = new SyncClient();
		anonymousClient.initAnonymous(host);
		return anonymousClient;
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
	
		adminUserName = "tester";
		adminPassword = "test1234";
		
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
