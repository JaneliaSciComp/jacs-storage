package org.janelia.jacsstorage.datarequest;

import java.util.List;

public class PageRequestBuilder {
    private PageRequest pageRequest = new PageRequest();

    public PageRequest build() {
        PageRequest toReturn = pageRequest;
        pageRequest = new PageRequest();
        return toReturn;
    }

    public PageRequestBuilder firstPageOffset(Long value) {
        if (value != null) pageRequest.setFirstPageOffset(value);
        return this;
    }

    public PageRequestBuilder pageNumber(Long value) {
        if (value != null) pageRequest.setPageNumber(value);
        return this;
    }

    public PageRequestBuilder pageSize(Integer value) {
        if (value != null) pageRequest.setPageSize(value);
        return this;
    }

    private List<SortCriteria> sortCriteria;


    public List<SortCriteria> getSortCriteria() {
        return sortCriteria;
    }

    public void setSortCriteria(List<SortCriteria> sortCriteria) {
        this.sortCriteria = sortCriteria;
    }

}
