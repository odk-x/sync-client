-------------
Build Instructions
-------------
1.  run ant in the opendatakit.wink/dependencies directory
2.  mvn clean in the directory where the pom.xml file resides
3.  mvn install in the directory where the pom.xml file resides

By default, this pom.xml file skips the tests and builds the javadocs.  If you would like to run the tests, you will need to add the following properties to your settings.xml in your .m2 directory which is usually under your home directory.  

<properties>
	<aggUrl>https://odk-test.appspot.com/odktables</aggUrl>
	<appId>tables</appId>
	<absolutePathOfTestFiles>testfiles/test/</absolutePathOfTestFiles>
	<batchSize>1000</batchSize>
	<skipTests>false</skipTests>
</properties>

The aggUrl and appId properties will be dependent upon the server that you want to 
run tests against.  

-------------
Documentation 
-------------
After running mvn install, the index for documentation will be found under opendatakit.wink/target/apidocs/index.html. 

-------------
JAR files
-------------
The jar file that will be generated from running mvn install is opendatakit.wink/target/wink-0.3-SNAPSHOT.jar.  The opendatakit.wink/target/wink-0.3-SNAPSHOT-javadoc.jar contains all of the documentation that can also be found under opendatakit.wink/target/apidocs  