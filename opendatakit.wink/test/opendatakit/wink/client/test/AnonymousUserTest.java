package opendatakit.wink.client.test;

import org.opendatakit.wink.client.WinkClient;

public class AnonymousUserTest extends AbstractPrivTestBase {
	
	private String adminUserName;
	private String adminPassword;
	
	WinkClient createNewSyncPrivClient() {
		WinkClient anonymousClient = new WinkClient();
		anonymousClient.initAnonymous(host);
		return anonymousClient;
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
