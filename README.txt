-------------
Build Instructions
-------------
1.  run ant in the dependencies directory
2.  mvn clean in the directory where the pom.xml file resides
3.  mvn install in the directory where the pom.xml file resides

By default, this pom.xml file skips the tests and builds the javadocs.  If you would like to run the tests, you will need to add the following properties to your settings.xml in your .m2 directory which is usually under your home directory.  

<properties>
	<aggUrl>https://odk-test.appspot.com/odktables</aggUrl>
	<appId>default</appId>
	<absolutePathOfTestFiles>testfiles/test/</absolutePathOfTestFiles>
	<batchSize>1000</batchSize>
	<skipTests>false</skipTests>
</properties>

Alternatively, the project is also set up to pass these parameters during the mvn install.  In the test case above, the command line and arguments would look like this:

  mvn install -DskipTests=false -DaggUrl=https://odk-test.appspot.com/odktables -DappId=default \
              -DabsolutePathOfTestFiles=testfiles/test  -DbatchSize=1000

The aggUrl and appId properties will be dependent upon the server that you want to 
run tests against.

NOTE: IN ORDER FOR THE TESTS TO PASS, you MUST have the following users (and permission levels)
set on the aggregate:

    Admin user: tester        pw:test1234
    Super user: superpriv     pw:test1234
    Synch user: syncpriv      pw:test1234

Additionally, your anonymous user MUST be set up to be able to synchronize tables.

Failure to set these users properly will result in a ton of 401:Unauthorized responses to the tests.

-------------
Documentation 
-------------
After running mvn install, the index for documentation will be found under target/apidocs/index.html. 

-------------
JAR files
-------------
The jar file that will be generated from running mvn install is target/sync-client-<version>-SNAPSHOT-jar-with-dependencies.jar.  The target/sync-client-<version>-SNAPSHOT-javadoc.jar contains all of the documentation that can also be found under target/apidocs  
