package org.janelia.jacsstorage.clients.api;

import java.util.List;

public class JadeResults<T> {
    private List<T> resultList;

    public List<T> getResultList() {
        return resultList;
    }

    public void setResultList(List<T> resultList) {
        this.resultList = resultList;
    }
}
