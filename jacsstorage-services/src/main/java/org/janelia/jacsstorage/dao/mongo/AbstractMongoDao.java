package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.AbstractDao;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.ReadWriteDao;
import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.SortCriteria;
import org.janelia.jacsstorage.datarequest.SortDirection;
import org.janelia.jacsstorage.model.support.AppendFieldValueHandler;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * Abstract Mongo DAO.
 *
 * @param <T> entity type
 */
public abstract class AbstractMongoDao<T extends BaseEntity> extends AbstractDao<T> implements ReadWriteDao<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMongoDao.class);

    protected final IdGenerator idGenerator;
    protected final MongoCollection<T> mongoCollection;

    protected AbstractMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        mongoCollection = mongoDatabase.getCollection(getEntityCollectionName(), getEntityType());
    }

    private String getEntityCollectionName() {
        Class<T> entityClass = getEntityType();
        PersistenceInfo persistenceInfo = EntityUtils.getMongoMapping(entityClass);
        Preconditions.checkArgument(persistenceInfo != null, "Entity class " + entityClass.getName() + " is not annotated with MongoMapping");
        return persistenceInfo.storeName();
    }

    @Override
    public T findById(Number id) {
        List<T> entityDocs = find(eq("_id", id), null, 0, 2, getEntityType());
        return CollectionUtils.isEmpty(entityDocs) ? null : entityDocs.get(0);
    }

    @Override
    public List<T> findByIds(Collection<Number> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        } else {
            return find(Filters.in("_id", ids), null, 0, 0, getEntityType());
        }
    }

    @Override
    public PageResult<T> findAll(PageRequest pageRequest) {
        List<T> results = find(null,
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        return new PageResult<>(pageRequest, results);
    }

    protected Bson createBsonSortCriteria(List<SortCriteria> sortCriteria) {
        Bson bsonSortCriteria = null;
        if (CollectionUtils.isNotEmpty(sortCriteria)) {
            Map<String, Object> sortCriteriaAsMap = sortCriteria.stream()
                .filter(sc -> StringUtils.isNotBlank(sc.getField()))
                .collect(Collectors.toMap(
                        SortCriteria::getField,
                        sc -> sc.getDirection() == SortDirection.DESC ? -1 : 1,
                        (sc1, sc2) -> sc2,
                        LinkedHashMap::new));
            bsonSortCriteria = new Document(sortCriteriaAsMap);
        }
        return bsonSortCriteria;
    }

    @Override
    public long countAll() {
        return mongoCollection.count();
    }

    protected <R> List<R> find(Bson queryFilter, Bson sortCriteria, long offset, int length, Class<R> resultType) {
        List<R> entityDocs = new ArrayList<>();
        FindIterable<R> results = mongoCollection.find(resultType);
        if (queryFilter != null) {
            results = results.filter(queryFilter);
        }
        if (offset > 0) {
            results = results.skip((int) offset);
        }
        if (length > 0) {
            results = results.limit(length);
        }
        return results
                .sort(sortCriteria)
                .into(entityDocs);
    }

    protected <R> List<R> aggregate(Bson queryFilter, List<Bson> aggregationOperators, Bson sortCriteria, int offset, int length, Class<R> resultType) {
        List<R> entityDocs = new ArrayList<>();
        ImmutableList.Builder<Bson> aggregatePipelineBuilder = ImmutableList.builder();
        if (queryFilter != null) {
            aggregatePipelineBuilder.add(Aggregates.match(queryFilter));
        }
        if (CollectionUtils.isNotEmpty(aggregationOperators)) {
            aggregatePipelineBuilder.addAll(aggregationOperators);
        }
        if (sortCriteria != null) {
            aggregatePipelineBuilder.add(Aggregates.sort(sortCriteria));
        }
        if (offset > 0) {
            aggregatePipelineBuilder.add(Aggregates.skip(offset));
        }
        if (length > 0) {
            aggregatePipelineBuilder.add(Aggregates.limit(length));
        }
        AggregateIterable<R> results = mongoCollection.aggregate(aggregatePipelineBuilder.build(), resultType);
        return results.into(entityDocs);
    }

    @Override
    public void save(T entity) {
        if (entity.getId() == null) {
            entity.setId(idGenerator.generateId());
            mongoCollection.insertOne(entity);
        }
    }

    public void saveAll(List<T> entities) {
        Iterator<Number> idIterator = idGenerator.generateIdList(entities.size()).iterator();
        List<T> toInsert = new ArrayList<>();
        entities.forEach(e -> {
            if (e.getId() == null) {
                e.setId(idIterator.next());
                toInsert.add(e);
            }
        });
        if (!toInsert.isEmpty()) {
            mongoCollection.insertMany(toInsert);
        }
    }

    @Override
    public void update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(false);
        update(entity, fieldsToUpdate, updateOptions);
    }

    protected long update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate, UpdateOptions updateOptions) {
        if (fieldsToUpdate.isEmpty())
            return 0;
        else
            return update(getUpdateMatchCriteria(entity), getUpdates(fieldsToUpdate), updateOptions);
    }

    protected long update(Bson query, Bson toUpdate, UpdateOptions updateOptions) {
        LOG.trace("Update: {} -> {}", query, toUpdate);
        UpdateResult result = mongoCollection.updateOne(query, toUpdate, updateOptions);
        return result.getMatchedCount();
    }

    protected Bson getUpdates(Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        List<Bson> fieldUpdates = fieldsToUpdate.entrySet().stream()
                .map(e -> getFieldUpdate(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return Updates.combine(fieldUpdates);
    }

    protected Bson getUpdateMatchCriteria(T entity) {
        return eq("_id", entity.getId());
    }

    @Override
    public void delete(T entity) {
        mongoCollection.deleteOne(eq("_id", entity.getId()));
    }

    @SuppressWarnings("unchecked")
    private Bson getFieldUpdate(String fieldName, EntityFieldValueHandler<?> valueHandler) {
        if (valueHandler == null || valueHandler.getFieldValue() == null) {
            return Updates.unset(fieldName);
        } else if (valueHandler instanceof AppendFieldValueHandler) {
            Object value = valueHandler.getFieldValue();
            if (value instanceof Iterable) {
                if (Set.class.isAssignableFrom(value.getClass())) {
                    return Updates.addEachToSet(fieldName, ImmutableList.copyOf((Iterable) value));
                } else {
                    return Updates.pushEach(fieldName, ImmutableList.copyOf((Iterable) value));
                }
            } else {
                return Updates.push(fieldName, value);
            }
        } else {
            return Updates.set(fieldName, valueHandler.getFieldValue());
        }
    }
}
