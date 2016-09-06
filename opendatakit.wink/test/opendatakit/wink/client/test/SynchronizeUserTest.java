package opendatakit.wink.client.test;

import org.opendatakit.wink.client.WinkClient;

public class SynchronizeUserTest extends AbstractPrivTestBase {
	
	private String adminUserName;
	private String adminPassword;
	
	private String syncUserName;
	private String syncUserPassword;
	
	WinkClient createNewSyncPrivClient() {
		WinkClient syncPrivClient = new WinkClient();
		syncPrivClient.init(host, syncUserName, syncUserPassword);
		return syncPrivClient;
	}


	WinkClient createNewAdminPrivClient() {
		WinkClient adminClient = new WinkClient();
		adminClient.init(host, adminUserName, adminPassword);
		return adminClient;
	}

	/*
	 * Perform setup for test if necessary
	 */
	@Override
	protected void setUp() throws Exception {
		super.setUp();

		syncUserName = "syncpriv";
		syncUserPassword = "test1234";
		
		adminUserName = "tester";
		adminPassword = "test1234";
	}

	/*
	 * Perform tear down for tests if necessary
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
	}


}
