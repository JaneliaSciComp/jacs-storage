# JADE - The JACS Storage Engine

[![CircleCI](https://circleci.com/gh/JaneliaSciComp/jacs-storage.svg?style=svg)](https://circleci.com/gh/JaneliaSciComp/jacs-storage)

## General Notes

JADE is a distributed storage system that can manage a set of configured volumes local to the machine or shared 
- NFS-mounted from a remote host. Typically the shared volumes are read only with the exception of 
the special OVERFLOW_VOLUME which is selected when a node runs out of available space on the local volumes.
The system consists of a master node (service) that manages the resource allocation and one or more workers or 
agents that are responsible with reading and writing the data onto/from the managed volumes.

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

`./gradlew --gradle-user-home=local ospackage`

Then on centos use yum to install the generated packages

`sudo yum install jacsstorage-masterweb/build/distributions/jacsstorage-masterweb-${jadeVersion}-1.i386.rpm`
`sudo yum install jacsstorage-agentweb/build/distributions/jacsstorage-agentweb-${jadeVersion}-1.i386.rpm`

where jadeVersion is the version from the main build.gradle file.

Note that 'ospackage' task just like 'installDist' will not run any unit tests or integration tests so you don't need 
access to any MongoDB instance.

### Build the application containers

```
docker build jacsstorage-masterweb --build-arg SSH_PRIVATE_KEY="`cat ~/.ssh/id_rsa`" -t jacsstorage-masterweb
docker build jacsstorage-agentweb --build-arg SSH_PRIVATE_KEY="`cat ~/.ssh/id_rsa`" -t jacsstorage-agentweb
```

```
docker-compose build --build-arg SSH_PRIVATE_KEY="`cat ~/.ssh/id_rsa`"
```

The above command builds master by default, so if you need to containerize a different branch (such as dev in the example below) one can use:

```
docker-compose build --build-arg SSH_PRIVATE_KEY="`cat ~/.ssh/id_rsa`" --build-arg BUILD_TAG=dev
```

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

The storage service persists data in "data bundles". A data bundle is a group of files
that are all persisted on the same data node under the same GUID based on the locality
of reference, i.e., data files that often need to be accessed together by some other service
or application. These could be for example data files associated with a particular sample, 
or they can be data files that resulted from a certain processing pipeline. It is up to the user 
of the storage service to group the files that need to be persisted "together". The storage service
may also associate certain properties with the data bundle that could be used later for searching
the persisted bundles. The data files that are part of a bundle can also be organized in a
directory hierarchy and the user can control whether these data files should be persisted 
in an expanded directory structure or in a TAR archive.

Most storage service invocation required authenticated access. The authentication is verified 
using a Json Web Token (JWT) passed in with every request in the 'Authorization' header as a
bearer token. This is very similar to the SCP command where the user is prompted for username
and password for each invocation.

One can obtain a JWT from the authorization service: 

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

sh local/auth.sh myusername mypassword

```

The above call will return a JSON blob that looks as below. The value of the 'token' attribute
is the one that will need to be passed with all the  invcations that require authentication as
a bearer token in the 'Authorization' header - 'Authorization: Bearer <tokenvalue>'.

```
{"token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg2MjY5NDAsInVzZXJfbmFtZSI6ImphY3MifQ.Yoku2rQfRn4GzoLYCfFc4Sag0jjrnYI_-A5W1W4I-o4","user_name":"jacs"}
```

### Bootstrapping storage volumes

To setup the storage volumes managed by an agent - the config must have a property `StorageAgent.BootstrappedVolumes`
which contains a comma delimited list of volumes to be bootstrapped. Then for each volume from the list
there must be a set of properties that defines the corresponding root path, whether the volume is local to the host
or it is a shared volume, the volume tags. The format of this properties is:

`StorageVolume.<volumeName>.<volumeProperty>`

The current supported properties are:

`RootDir` - defines the volume's root directory<br/>
`Shared` - specifies whether this volume is on a shared mount point<br/>
`PathPrefix` - virtual root directory<br/>
`Tags` - list of features or labels attached to the volume<br/>

Once the configuration is prepared for bootstrapping the you only need to start the agent with `-bootstrapStorageVolumes`
flag 

### Allocate a storage bundle

The method documentation is available [here](http://jade1:8880/docs/#/Master_storage_API./createBundleInfo)

This would be similar to creating a subdirectory in the user's home directory:
- `md /users/home/myusername/aWorkingSubdirForProject1`

The equivalent storage service curl invocation is:
```
curl -i -X POST 'http://localhost:8880/jacsstorage/master_api/v1/storage' \
-H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs' \
-H 'accept: application/json' \
-H 'Content-Type: application/json' \
-d "{ \"name\": \"aWorkingSubdirForProject1\", \"storageFormat\": \"DATA_DIRECTORY\"}"
```

The method returns a JSON block that contains information about the new created data bundle:
```
{
id=2503313663696306217, 
name=workspace1, 
ownerKey=user:jacs, 
path=306/217/2503313663696306217, 
readersKeys=[], 
writersKeys=[], 
storageRootPrefixDir=/localhost/d1, 
storageRootRealDir=/var/tmp/d1, 
storageHost=localhost, 
storageTags=[jade, d1], 
connectionURL=http://localhost:8881/jacsstorage/agent_api/v1, 
storageFormat=DATA_DIRECTORY, 
requestedSpaceInBytes=null, 
checksum=null, 
metadata={}}
```

### Get storage bundle info

The method documentation is available [here](http://jade1:8880/docs/#/Master_storage_API./getBundleInfo)

```
curl -i 'http://localhost:8880/jacsstorage/master_api/v1/storage/2503313663696306217' \
-H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs' \
-H 'accept: application/json'
```
The method returns a JSON block identical to the one returned by the allocate operation.

### Search storage bundles

The method documentation is available [here](http://jade1:8880/docs/#/Master_storage_API./listBundleInfo)

```
curl -i 'http://localhost:8880/jacsstorage/master_api/v1/storage?name=workspace1' \
-H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs' \
-H 'accept: application/json'
```

If the user does not have admin privileges search automatically uses the current user as the owner and
only searches entries created by the current user.

### Create a directory entry

The method documentation is available [here](http://jade1:8881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./postCreateDirectory)

This command creates a subdirectory in the data bundle's workspace. The corresponding 
shell commands are change directory to the workspace followed by create subdirectory
in the current directory, i.e.,
- `cd /users/home/myusername/aWorkingSubdirForProject1`
- `md myDir1`

The equivalent storage service curl invocation is and the command must use the base URL returned 
in the 'connectionURL' field of the allocate result or get storage info result:
```
curl -X POST "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/directory/myDir1" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "Content-Type: application/octet-stream"
```

The directory entry can be a path hierarchy but the constraint is that all parent directories must 
already exist in the storage bundle. For example if the command is:
```
curl -X POST "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/directory/myDir1/myDir1.1//myDir1.1.3" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "Content-Type: application/octet-stream"
```

The entries 'myDir1' and 'myDir1/myDir1.1' must already exist in the bundle '2501203311319875608' and they
must be directory entries.
 
The method returns the a JSON block for the new entry as well as the access URL in the 
header's 'location' attribute
 

### Add a file to the storage bundle  

The method documentation is available [here](http://jade1:8881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./postCreateFile)

```
curl -X POST "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/file/myDir1/myFile1.1" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "Content-Type: application/octet-stream" \
-d @"aLocalFile"
```

Similar to the 'create directory' call the base URL must be the actual storage URL, returned by allocate or get info 
methods in the 'connectionURL' field and if the file entry name denotes a hierarchical structure then all
its parents must exist and be directory entries

The method returns the a JSON block for the new entry as well as the access URL in the 
header's 'location' attribute


### List the content of a storage bundle

The method documentation is available [here](http://jade1:8881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./listContent)

The shell equivalent commands would be:
- `cd /users/home/myusername/aWorkingSubdirForProject1`
- `ls -l`

The curl command must use the actual storage URL returned in 'connectionURL' field.

```
curl "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/list?entry=myDir1" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
```

### Retrieve the content of a specific entry from a data storage bundle

The method documentation is available [here](http://jade1:8881/docs/#/Agent_storage_API._This_API_requires_an_authenticated_subject./getEntryContent)

The curl command must use the actual storage URL returned in 'connectionURL' field.

```
curl "http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/2501203311319875608/data_content/myDir1/myDir1.1" \
-H "accept: application/json" \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
```

If the entry name (the path after 'data_content') denotes a folder the method returns all the sub-entries from
from specified entry packaged in a tar archive.

If no entry is specified then the method returns the content of the entire bundle as a tar archive


### Retrieve content from shared storage

The storage service allows users to download content that resides on shared storage such: dm11, nrs or nearline. The
retrieval can be done by going directly to an agent node and using "storage_content/storage_path" endpoint or using a 
more reliable mechanism that requires two steps - first get an agent that can serve the content from the master and
then use the agent's "storage_content/storage_path" endpoint to actually retrieve the content.
For example to retrieve the data file '/nrs/jacs/jacsData/flylight/pipelineResult/data1.png' one can use the following
sequence:

```
curl -i -X PROPFIND http://localhost:8880/jacsstorage/master_api/v1/webdav/data_storage_path/nrs/jacs/jacsData/flylight/pipelineResult/data1.png \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "accept: application/xml"
```

then get the agent url from the response' HREF field and (assuming HREF field is http://localhost:8881/jacsstorage/agent_api/v1/agent_storage) use:
```
curl http://localhost:8881/jacsstorage/agent_api/v1/agent_storage/storage_content/storage_path/nrs/jacs/jacsData/flylight/pipelineResult/data1.png \
-H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTgyMDE0MjUsInVzZXJfbmFtZSI6ImphY3MifQ.El8GcDhswj-mNmBK2uMaAXHqBPDN_AGgNm_oyU3McQs" \
-H "accept: application/octet-stream"
```
 
### Accessing the storage service from your application

#### CLI

See the [jacsstorage-clients](jacsstorage-clients/README.md) module for more information about the CLI.

#### Java

See the [jacsstorage-clients](jacsstorage-clients/README.md) module for more information about the Java API.

For Java examples please take a look at the 'examples/java' directory.

#### Python

To access the storage service from python please take a look at the `examples/python` directory.
