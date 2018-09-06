package org.janelia.jacsstorage.webdav.propfind.customprops;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class StorageVolumeProp {

//!!!!    @JacksonXmlProperty(localName="jade:bind")
    private String bindName;
    @JacksonXmlProperty(localName="jade:rootdir")
    private String rootDir;

    public String getBindName() {
        return bindName;
    }

    public void setBindName(String bindName) {
        this.bindName = bindName;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }
}
