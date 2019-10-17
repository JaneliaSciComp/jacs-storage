package org.janelia.jacsstorage.model.jacsstorage;

import java.util.Date;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

/**
 * Entity for a JACS Agent.
 */
@PersistenceInfo(storeName ="jacsStorageAgent", label="JacsStorageAgent")
public class JacsStorageAgent extends AbstractEntity {
    private String agentHost; // storage agent host and port on which this agent is running
    private String agentAccessURL; // agent access URL
    private Set<String> servedVolumes; // list of volumes served by this agent
    private Set<String> unavailableVolumeIds; // list of volume ids that cannot be accessed because of various reasons even though they are configured as servable
    private String status;
    private Date lastStatusCheck = new Date();

    public String getAgentHost() {
        return agentHost;
    }

    public void setAgentHost(String agentHost) {
        this.agentHost = agentHost;
    }

    public String getAgentAccessURL() {
        return agentAccessURL;
    }

    public void setAgentAccessURL(String agentAccessURL) {
        this.agentAccessURL = agentAccessURL;
    }

    public Set<String> getServedVolumes() {
        return servedVolumes;
    }

    public void setServedVolumes(Set<String> servedVolumes) {
        this.servedVolumes = servedVolumes;
    }

    public Set<String> getUnavailableVolumeIds() {
        return unavailableVolumeIds;
    }

    public void setUnavailableVolumeIds(Set<String> unavailableVolumeIds) {
        this.unavailableVolumeIds = unavailableVolumeIds;
    }

    public boolean canServe(JacsStorageVolume storageVolume) {
        return servedVolumes != null && (servedVolumes.contains("*") || servedVolumes.contains(storageVolume.getName())) &&
                (CollectionUtils.isEmpty(unavailableVolumeIds) || !storageVolume.hasId() || !unavailableVolumeIds.contains(storageVolume.getId().toString()))
                ;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getLastStatusCheck() {
        return lastStatusCheck;
    }

    public void setLastStatusCheck(Date lastStatusCheck) {
        this.lastStatusCheck = lastStatusCheck;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentHost", agentHost)
                .append("agentAccessURL", agentAccessURL)
                .append("servedVolumes", servedVolumes)
                .append("status", status)
                .append("lastStatusCheck", lastStatusCheck)
                .toString();
    }
}


