package org.janelia.jacsstorage.service.distributedservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.inject.Inject;
import java.util.List;

@RemoteInstance
public class DistributedStorageLookupService implements StorageLookupService {

    private final JacsStorageVolumeDao storageVolumeDao;
    private final JacsBundleDao bundleDao;
    private final StorageAgentManager agentManager;

    @Inject
    public DistributedStorageLookupService(JacsStorageVolumeDao storageVolumeDao,
                                           JacsBundleDao bundleDao,
                                           StorageAgentManager agentManager) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
        this.agentManager = agentManager;
    }

    @Override
    public JacsBundle findDataBundleByOwnerAndName(String owner, String name) {
        JacsBundle bundle = bundleDao.findByOwnerAndName(owner, name);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        PageResult<JacsBundle> matchingBundles = bundleDao.findMatchingDataBundles(pattern, pageRequest);
        matchingBundles.getResultList().stream()
                .filter(db -> !db.hasStorageHost())
                .forEach(db -> updateStorageVolume(db))
        ;
        return matchingBundles;
    }

    @Override
    public JacsBundle getDataBundleById(Number id) {
        JacsBundle bundle = bundleDao.findById(id);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    private void updateStorageVolume(JacsBundle bundle) {
        JacsStorageVolume bundleVol = bundle.getStorageVolume()
                .orElseGet(() -> {
                    JacsStorageVolume sv = storageVolumeDao.findById(bundle.getStorageVolumeId());
                    bundle.setStorageVolume(sv);
                    return sv;
                });
        if (bundleVol.isShared() && StringUtils.isBlank(bundleVol.getStorageHost())) {
            agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected())
                    .ifPresent(ai -> {
                        bundleVol.setStorageServiceURL(ai.getAgentHttpURL());
                        bundleVol.setStorageServiceTCPPortNo(ai.getTcpPortNo());
                    });
        }
    }
}
