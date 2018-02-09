## Development 

### Environment

The development environment requires access to our development MongoDB instance running
on dev-mongodb in order to run the integration tests. If you are already inside Janelia 
that is not a problem - since you automatically have access to it, otherwise you may have 
to be on VPN in order to access it. Another option is to use any other MongoDB instance and 
write a java properties file that overrides the MongoDB settings. Then set up the environment
variables - JACSSTORAGE_CONFIG for production and JACSSTORAGE_CONFIG_TEST for build to point
to it, e.g.

```
mkdir local
cat <<EOF > local/myConfig.properties
MongoDB.ConnectionURL=mongodb://myusername:mypassword@localhost:27017/?authSource=db1
EOF
export JACSSTORAGE_CONFIG=$PWD/local/myConfig.properties
export JACSSTORAGE_CONFIG_TEST=$PWD/local/myConfig.properties
```

As a note if you only want to compile and create the distribution then you don't need the
MongoDB setup because the integration tests run only as part of the build task, installDist 
only compiles and creates the zip and tar distributions.

##### Setup local MongoDB instance

To install MongoDB on MacOS:

With Homebrew:
`brew install mongodb`

With macports:
`sudo port install mongodb`

On Centos based Linux distributions (Centos, Scientific Linux) you can use:
`yum install mongodb-org-server`

On Debian based Linux distributions (Debian, Ubuntu) you can use:
`sudo apt-get install mongodb-org`

Once MongoDB is installed on your machine you really don't have to do anything else because the tests or the application
will create the needed databases and the collections as long as the configured mongo user has prvileges to do so.

### Build the application

To full build the application, which includes running all unit tests and integration tests, and create the distribution simply run:
```
./gradlew clean build installDist
```
To only compile the application and create the distribution run (this will not run any tests and therefore it will 
not require any Mongo database setup):
```
./gradlew clean installDist
```
To run only the integration tests:
```
./gradlew integrationTest
```

If you want to use a different test database than the development MongoDB instance you can create a configuration file,
as explained above, in which you override the database connection settings and then use JACSSTORAGE_CONFIG_TEST 
environment variable to point to it, eg.,
```
JACSSTORAGE_CONFIG_TEST=/my/prefered/location/for/dev/my-config-test.properties ./gradlew clean build installDist
```

Note:

When using the environment variable to reference the configuration use the full path in order to guarantee that the right properties are being used.

### Package and install the application

To generate an RPM package create a gradle.properties:

```
cat > local/gradle.properties <<EOF
jacs.runtime.env.apiKey=JacsStorageAuthorizedAPI.Dev
jacs.runtime.env.jwtSecret=<put the secret key here>
jacs.runtime.env.agentHttpPort=9881
jacs.runtime.env.masterHttpPort=9880
jacs.runtime.env.logsRootDir=/data/jacsstorage/prod-logs
EOF
```

`./gradlew --gradle-user-home=local packageRpm`

Then on centos use yum to install the generated packages

`sudo yum install jacsstorage-masterweb/build/distributions/jacsstorage-masterweb-1.0.0-1.i386.rpm`
`sudo yum install jacsstorage-agentweb/build/distributions/jacsstorage-agentweb-1.0.0-1.i386.rpm`


Note that 'ospackage' task just like 'installDist' will not run any unit tests or integration tests so you don't need 
access to any MongoDB instance.

### Run the application

To run the async services with the default settings which assume a Mongo database instance running on the same machine where the web server is running:

`jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

If you want to debug the application you can start the application with the debug agent as below:

`JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

The default production settings could be overwritten with your own settings in a java properties file similar to the test settings
the only difference is the name of the environment variable - for production settings use JACSSTORAGE_CONFIG environment variable.

`JACSSTORAGE_CONFIG=/usr/local/etc/myjacsstorage-config.properties jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

If the master and the agent are installed as system services:

`
sudo systemctl daemon-reload
sudo systemctl start jacsstorage-masterweb
sudo systemctl start jacsstorage-agentweb
`

## User guide

Most storage service invocation required authenticated access. The authentication is verified 
using a Json Web Token (JWT) passed in with every request in the 'Authorization' header as a
bearer token.

You can obtain a JWT from the authorization service: 

development environment: 'https://jacs-dev.int.janelia.org/SCSW/AuthenticationService/v1/authenticate'

or

production environment: 'http://api.int.janelia.org:8030/authenticate'

```
cat > local/auth.sh <<EOF
#!/bin/sh

AUTH_ENDPOINT="https://jacs-dev.int.janelia.org/SCSW/AuthenticationService/v1/authenticate"

username=$1
password=$2

curl -X POST "" \
-H  "accept: application/json" \
-H  "content-type: application/json" \
-d "{  \"username\": \"${username}\",  \"password\": \"${password}\"}"
EOF
```

### Put data onto the storage servers

The copy of the data onto the storage server(s) it's a two step process:
1. Ask the master (see [documentation](http://jade1:9880/docs/#/Master_storage_API./createBundleInfo)) on which server I can copy the data.
```
curl -i -X POST 'http://localhost:8880/jacsstorage/master_api/v1/storage' \
-H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs' \
-H 'accept: application/json' \
-H 'Content-Type: application/json' \
-d "{ \"name\": \"d1\", \"ownerKey\": \"user:goinac\", \"storageTags\": [ \"d1\" ], \"storageFormat\": \"DATA_DIRECTORY\", \"metadata\": { \"additionalProp1\": {}, \"additionalProp2\": {}, \"additionalProp3\": {} }}"
```

########JSON Fields description:

- name: represents the name of the storage entry and this must be unique within a user context.
- ownerKey: is the name of the owner formatted like a JACS subject, for example: 'user:username' or 'group:groupname'
- storageTags: can be used to select the storage device on which to store the data. This is useful if you want some
data files to be stored on nrs for example so that they could be accessed by processes running on the grid.
- storageFormat: specifies how the data should be stored on the storage server: as a directory, as a tar archive or it's a single file
Valid values for the storage format are: 
    - DATA_DIRECTORY - store data as expanded directory 
    - ARCHIVE_DATA_FILE - store data in a tar archive
    - SINGLE_DATA_FILE - the storage is a single file

2. Use the connectionURL and the ID returned in the JSON result in the first step to send to <connectionURL value>/agent_storage/<ID value> (see [documentation](http://jade1:9881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./persistStream)) the data files.
```
curl -X POST "http://0.0.0.0:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/file/f1" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "Content-Type: application/octet-stream" \
-d "this is the content"
```

If you want to send more than one file at a time, you need to bundle the files in a TAR archive and send the entire tar. On the
storage server the tar will be unbundled and added to the selected storage.

### Retrieve storage info

If the storage ID is known you can find information about the storage, i.e., where it resides, path info using 
[getBundleInfo](http://jade1:9880/docs/#/Master_storage_API./getBundleInfo)

```
curl -X GET "http://localhost:8880/jacsstorage/master_api/v1/storage/2501203311319875608" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs"
```

### Retrieve storage content
The content retrieval is also a two step process:
1. Retrieve storage info or search storage entries to find out where the content is stored
2. Use the returned contentURL in combination with the ID and the entry name to retrieve the actual content 
(see [documentation](http://jade1:9881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./getEntryContent))
```
curl -X GET "http://0.0.0.0:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/entry_content/f1" \
-H "accept: application/octet-stream" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs"
```

### Search storage entries

Storage entries can be searched by owner, volume name, tags - see [documentation](http://jade1:9880/docs/#/Master_storage_API./listBundleInfo)
If you are not an admin you can only search your own entries.

```
curl -X GET "http://localhost:8880/jacsstorage/master_api/v1/storage" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs"
```

### List files that are part of a storage bundle.

If the storage bundle ID and the its server location is known, then the API call is (see [documentation](http://jade1:9881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./listContent)):
```
curl -X GET "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/list" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs"
```

### Accessing the storage service from your application

##### Java

For Java examples please take a look at the jacsstorage-clients module, packages 'org.janelia.jacsstorage.client'
and 'org.janelia.jacsstorage.clientutils'. There you will find examples of how to access the storage
using jersey client library.

##### Python

To access the storage service from python please take a look at the `examples/python` directory.
