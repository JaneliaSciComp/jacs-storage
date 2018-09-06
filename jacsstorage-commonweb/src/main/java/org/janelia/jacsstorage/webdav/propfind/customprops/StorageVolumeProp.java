package org.janelia.jacsstorage.webdav.propfind.customprops;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(namespace = "jade", localName = "storageVolume")
public class StorageVolumeProp {

    @JacksonXmlProperty(namespace = "jade", localName="bindName")
    private String bindName;
    @JacksonXmlProperty(namespace = "jade", localName="rootDir")
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
