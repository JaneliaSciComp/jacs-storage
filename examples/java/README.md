This directory contains an example of how to interact with the storage service from a Java application

#### Build the application

```
gradle clean build installDist
```

#### Allocate storage

Creates a databundle with the given name.

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword allocate \
-name workspace1
```

The method returns a JSON blob that contains the information about the new created data bundle, e.g.
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

#### Create a directory entry

Create a subdirectory in the specified data bundle. As a note the directory entry name is 
specified just like a file system directory relative to the data bundle location, e.g. 'a/b/c/d', and
all the part paths must already exist in the data bundle. So for the above example 'a', 'a/b', 'a/b/c'
must all already be present in the bundle

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword mkdir \
-bundleId 2503313663696306217 \
-entry a
```

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword mkdir \
-bundleId 2503313663696306217 \
-entry a/b
```

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword mkdir \
-bundleId 2503313663696306217 \
-entry a/b/c
```

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword mkdir \
-bundleId 2503313663696306217 \
-entry a/b/c/d
```

#### Add a file entry

Pushes the content of a local file to the specified data bundle. If the bundle entry name for the 
file contains a directory structure all its parent folders must be present. 

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-username myUserName -password myPassword mkdir \
-bundleId 2503313663696306217 \
-localPath pathToALocalFile
-entry a/b/c/newFile
```


#### List the content of a data storage bundle

List the tree content of the data bundle 
```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-token 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg3MTE4NTksInVzZXJfbmFtZSI6ImphY3MifQ.YlG-o14IUYFaLXncKrBH_ZziJmCoBGvoB-LNvmT-VgI' 
list -bundleId 2503313663696306217
```

or the subtree of a specific entry from the data bundle
```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-token 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg3MTE4NTksInVzZXJfbmFtZSI6ImphY3MifQ.YlG-o14IUYFaLXncKrBH_ZziJmCoBGvoB-LNvmT-VgI' 
list -bundleId 2503313663696306217 -entry a/b/c/d
```

If the entry name is not valid it returns an empty list

#### Retrieve the content of a specific entry from a data storage bundle

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-token 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg3MTE4NTksInVzZXJfbmFtZSI6ImphY3MifQ.YlG-o14IUYFaLXncKrBH_ZziJmCoBGvoB-LNvmT-VgI' \
get -bundleId 2503313663696306217 -entry a/b/c/newFile -localPath myLocalFileName
```

If the specified entry name is a directory entry then it retrieves all its files content and subdirectories packaged
in a tar file.

```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-token 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg3MTE4NTksInVzZXJfbmFtZSI6ImphY3MifQ.YlG-o14IUYFaLXncKrBH_ZziJmCoBGvoB-LNvmT-VgI' \
get -bundleId 2503313663696306217 -entry a/b -localPath myLocalTar
```

#### Search the data bundles
```
build/install/jacsStorageExampleApp/bin/jacsStorageExampleApp \
-token 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1MTg3MTE4NTksInVzZXJfbmFtZSI6ImphY3MifQ.YlG-o14IUYFaLXncKrBH_ZziJmCoBGvoB-LNvmT-VgI' \
search -name workspace1
```

This returns a paginated response, e.g.:
```
{
pageOffset=0, 
pageNumber=0, 
pageSize=1, 
sortCriteria=null, 
resultList=[
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
    requestedSpaceInBytes=4023, 
    checksum=null, 
    metadata={}
    }
]
}
```
