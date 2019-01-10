package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
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
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.SortCriteria;
import org.janelia.jacsstorage.datarequest.SortDirection;
import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;
import org.janelia.jacsstorage.model.support.AppendFieldValueHandler;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.IncFieldValueHandler;
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
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;

/**
 * Abstract Mongo DAO.
 *
 * @param <T> entity type
 */
public abstract class AbstractMongoDao<T extends BaseEntity> extends AbstractDao<T> implements ReadWriteDao<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMongoDao.class);
    private static final String RECORDS_COUNT_FIELD = "recordsCount";

    protected final IdGenerator idGenerator;
    protected final MongoCollection<T> mongoCollection;

    protected AbstractMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        mongoCollection = mongoDatabase.getCollection(getEntityCollectionName(), getEntityType());
    }

    private String getEntityCollectionName() {
        Class<T> entityClass = getEntityType();
        PersistenceInfo persistenceInfo = EntityUtils.getPersistenceInfo(entityClass);
        Preconditions.checkArgument(persistenceInfo != null, "Entity class " + entityClass.getName() + " is not annotated with MongoMapping");
        return persistenceInfo.storeName();
    }

    /**
     * This is a placeholder for creating the collection indexes. For now nobody invokes this method.
     */
    abstract protected void createDocumentIndexes();

    @Override
    public T findById(Number id) {
        Iterator<T> entityDocsItr = findIterable(eq("_id", id), null, 0, 2, getEntityType()).iterator();
        return entityDocsItr.hasNext() ? entityDocsItr.next() : null;
    }

    @Override
    public List<T> findByIds(Collection<Number> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        } else {
            return findAsList(Filters.in("_id", ids), null, 0, 0, getEntityType());
        }
    }

    @Override
    public PageResult<T> findAll(PageRequest pageRequest) {
        return new PageResult<>(pageRequest,
                findAsList(null,
                        createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize(),
                        getEntityType()));
    }

    Bson createBsonSortCriteria(List<SortCriteria> sortCriteria) {
        return createBsonSortCriteria(sortCriteria, ImmutableList.of());
    }

    Bson createBsonSortCriteria(List<SortCriteria> sortCriteria, List<SortCriteria> additionalCriteria) {
        Bson bsonSortCriteria = null;
        Map<String, Object> sortCriteriaAsMap = Stream.of(sortCriteria)
                .filter(lsc -> CollectionUtils.isNotEmpty(lsc))
                .flatMap(lsc -> lsc.stream())
                .filter(sc -> StringUtils.isNotBlank(sc.getField()))
                .collect(Collectors.toMap(
                        SortCriteria::getField,
                        sc -> sc.getDirection() == SortDirection.DESC ? -1 : 1,
                        (sc1, sc2) -> sc2,
                        LinkedHashMap::new));
        if (!sortCriteriaAsMap.isEmpty()) {
            bsonSortCriteria = new Document(sortCriteriaAsMap);
        }
        return bsonSortCriteria;
    }

    @Override
    public long countAll() {
        return mongoCollection.countDocuments();
    }

    <R> List<R> findAsList(Bson queryFilter, Bson sortCriteria, long offset, int length, Class<R> resultType) {
        List<R> results = new ArrayList<>();
        findIterable(queryFilter, sortCriteria, offset, length, resultType).forEach(results::add);
        return results;
    }

    <R> Iterable<R> findIterable(Bson queryFilter, Bson sortCriteria, long offset, int length, Class<R> resultType) {
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
                .sort(sortCriteria);
    }

    <R> List<R> aggregateAsList(List<Bson> aggregationOperators, Bson sortCriteria, long offset, int length, Class<R> resultType) {
        List<R> results = new ArrayList<>();
        Iterable<R> resultsItr = aggregateIterable(aggregationOperators, sortCriteria, offset, length, resultType);
        resultsItr.forEach(results::add);
        return results;
    }

    <R> Iterable<R> aggregateIterable(List<Bson> aggregationOperators, Bson sortCriteria, long offset, int length, Class<R> resultType) {
        ImmutableList.Builder<Bson> aggregatePipelineBuilder = ImmutableList.builder();
        if (CollectionUtils.isNotEmpty(aggregationOperators)) {
            aggregatePipelineBuilder.addAll(aggregationOperators);
        }
        if (sortCriteria != null) {
            aggregatePipelineBuilder.add(Aggregates.sort(sortCriteria));
        }
        if (offset > 0) {
            aggregatePipelineBuilder.add(Aggregates.skip((int) offset));
        }
        if (length > 0) {
            aggregatePipelineBuilder.add(Aggregates.limit(length));
        }
        return mongoCollection.aggregate(aggregatePipelineBuilder.build(), resultType);
    }

    Long countAggregate(List<Bson> aggregationOperators) {
        ImmutableList.Builder<Bson> aggregatePipelineBuilder = ImmutableList.builder();
        if (CollectionUtils.isNotEmpty(aggregationOperators)) {
            aggregatePipelineBuilder.addAll(aggregationOperators);
        }
        aggregatePipelineBuilder.add(Aggregates.count(RECORDS_COUNT_FIELD));
        Document recordsCountDoc = mongoCollection.aggregate(aggregatePipelineBuilder.build(), Document.class).first();
        if (recordsCountDoc == null) {
            return 0L;
        } else if (recordsCountDoc.get(RECORDS_COUNT_FIELD) instanceof Integer) {
            return recordsCountDoc.getInteger(RECORDS_COUNT_FIELD).longValue();
        } else if (recordsCountDoc.get(RECORDS_COUNT_FIELD) instanceof Long) {
            return recordsCountDoc.getLong(RECORDS_COUNT_FIELD);
        } else {
            LOG.error("Unknown records count field type: {}", recordsCountDoc);
            throw new IllegalStateException("Unknown RECORDS COUNT FIELD TYPE " + recordsCountDoc);
        }
    }

    @Override
    public void save(T entity) {
        if (entity.getId() == null) {
            entity.setId(idGenerator.generateId());
            try {
                mongoCollection.insertOne(entity);
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    throw new IllegalArgumentException(e);
                }
            }
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
            try {
                mongoCollection.insertMany(toInsert);
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    throw new IllegalArgumentException(e);
                }
            }
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
        } else if (valueHandler instanceof IncFieldValueHandler) {
            return Updates.inc(fieldName, (Number) valueHandler.getFieldValue());
        } else {
            return Updates.set(fieldName, valueHandler.getFieldValue());
        }
    }
}
