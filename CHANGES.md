# 1.13.1.RELEASE
* use host and port to lookup local storage
# 1.12.1.RELEASE
# 1.10.0.RELEASE

* added /authenticated and /unauthenticated mappings for the REST servlet
* quota endpoint for volume and mandatory subject

# 1.9.0.RELEASE

* Cache local host (attempt to fix UnknownHostException thrown in NetUtils.getCurrentHostName)
* Close the data nodes stream when reading a content directory (bug fix)

# 1.8.0.RELEASE

* Added an endpoint to get quota report(s) for all registered volumes

# 1.7.4.SNAPSHOT

* When creating a storage bundle allow to link it to an existing data path by specifying the link
* Changed the retrieve content endpoints "/storage_path/data_content/{dataPath:.+}"
* Added an endpoint to retrieve content information by examining the entry content "/storage_path/data_info/{dataPath:.+}"
* Added a filtering mechanism to be able to retrieve slices from a TIFF for example

# 1.6.2.SNAPSHOT

# 1.6.1.RELEASE

* This was a fix build in which we had to roll back the addFields stage
from search volume because that is not yet supported by the production
version which still runs 3.2.4

# 1.6.0.RELEASE

* support for context variables in the volume path, such as ${username}, ${name}, ${createdDate}

# 1.5.1.RELEASE

* fix for unique volume path prefix

# 1.5.0.RELEASE

* support for "jade://" URI scheme - the volume virtual root prefix now is returned as a URI
* added volume permissions
* entries can be created even if the parent does not exist; if the parent is missing it simply creates it. 
* REST endpoint for deleting path based storage items

# 1.4.0.RELEASE

* REST endpoint for creating path based storage items stored only on the filesystem
* REST endpoint for storage volume management
* fixed the content stream endpoints to set content-length attribute
