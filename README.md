`# Setting up the development environment

To build the application you can either install gradle 4.x locally on your machine using the appropriate package manager for your OS
(brew or macports for OSX, yum for Centos based Linux distros, apt-get for Debian based Linux distros) or use gradle scripts packaged
with the project.

Because the build is configured to run the integration tests as part of the full build you also need to have access to a Mongo
database server. The default configuration expects the database server to be running on the development machine but it doesn't have to.

## Setup MongoDB

To install MongoDB on MacOS:

With Homebrew:
`brew install mongodb`

With macports:
`sudo port install mongodb`

On Centos based Linux distributions (Centos, Scientific Linux) you can use:
`yum install mongodb-org-server`

On Debian based Linux distributions (Debian, Ubuntu) you can use:
`sudo apt-get install mongodb-org`

Once MongoDB is installed on your machine you really don't have to do anything else because the tests will create the needed databases and
the collections as long as the user has prvileges to do so.

## Building and running the application

### Building the application

If you already have gradle installed on your system you can simply run:

`gradle build`
or you can always run:
`./gradlew build`

### Running only the integration tests

`./gradlew integrationTest`

If you want to use a different test database than the one running locally on your development machine you can create a configuration file
in which you override the database connection settings and then use JACSSTORAGE_CONFIG_TEST environment variable to point to it, eg.,
`JACSSTORAGE_CONFIG_TEST=/my/prefered/location/for/dev/my-config-test.properties ./gradlew integrationTest`

Keep in mind that since the integrationTests are configured to run as part of the build check stage you also need to have the environment variable
set you you run the build:
`JACSSTORAGE_CONFIG_TEST=/my/prefered/location/for/dev/my-config-test.properties ./gradlew build`

For example my-config-test.properties could look as below if you want to use the dev mongo database. I recommend to prefix the database name with your
user name so that your tests will not clash with other users' tests in case the build runs simultaneously.
`
MongoDB.ConnectionURL=mongodb://dev-mongodb:27017
MongoDB.Database=myusername_jacs_test
`

and to build the application you simply run:

`JACSSTORAGE_CONFIG_TEST=$PWD/my-config-test.properties ./gradlew clean build installDist`

Note:

When using the environment variable to reference the configuration use the full path in order to guarantee that the right properties are being used.

### Package the application

`./gradlew installDist`

To generate an RPM package create a gradle.properties:

```
cat > local/gradle.properties <<EOF
jacs.runtime.env.apiKey=JacsStorageAuthorizedAPI.Dev
jacs.runtime.env.jwtSecret=SYhZwP9ZbSpjIbvRkn6GfBUACWzPb-53zbD9Ps3jVswg85WvJp4DlLrlMdhkaNlP8Zq0V63r5er_w7qlGeGxSD8CH1nsDgRhC0umwGeLDvEj4TbCicJSc3Klz2el-3iv-jiMp69h27YBmrUYf_fwOagi1x9To-hpQ-1x78G7bd8dRVO0wDOeAILNgvhy22hxvoX_PrRZkXbn_7aLeNziw-48vL0idYkxxJIEqnLqnOyEy_TCEvo4w_n14vseDn7ZzulNR97nNL9ZFnD7GXXr5ZQqeVIO-HoNbKSP3f1YCACqT2QC-89FdefaKlR-SFp_EBSfvEiQN8E0WrUObXfHKg
jacs.runtime.env.agentHttpPort=9881
jacs.runtime.env.agentTcpPort=11000
jacs.runtime.env.masterHttpPort=9880
jacs.runtime.env.logsRootDir=/data/jacsstorage/prod-logs
EOF
```

`./gradlew --gradle-user-home=local packageRpm`

Then on centos use yum to install the generated packages

`sudo yum install jacsstorage-masterweb/build/distributions/jacsstorage-masterweb-1.0.0-1.i386.rpm`
`sudo yum install jacsstorage-agentweb/build/distributions/jacsstorage-agentweb-1.0.0-1.i386.rpm`


Note that 'installDist' target will not run any unit tests or integration tests.

### Run the application

To run the async services with the default settings which assume a Mongo database instance running on the same machine where the web server is running:

`jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

If you want to debug the application you can start the application with the debug agent as below:

`JAVA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

The default settings could be overwritten with your own settings in a java properties file that contains only the updated properties
and then use JACSSTORAGE_CONFIG environment variable to reference the settings file, e.g.

`JACSSTORAGE_CONFIG=/usr/local/etc/myjacsstorage-config.properties jacsstorage-web/build/install/jacsstorage-web/bin/jacsstorage-web`

If the master and the agent are installed as system services:

`
sudo systemctl daemon-reload
sudo systemctl start jacsstorage-masterweb
sudo systemctl start jacsstorage-agentweb
`
