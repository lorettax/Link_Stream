package com.lorettax.StreamExtractionEngine.core.functions;


import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.lorettax.StreamExtractionEngine.core.Field;
import com.lorettax.StreamExtractionEngine.core.LinkStreamFQL;
import com.lorettax.StreamExtractionEngine.tools.JsonTool;
import com.lorettax.StreamExtractionEngine.tools.MD5Tool;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class O_FlatSet extends AbstractFunction {
	
    private static final Logger logger = LoggerFactory.getLogger(O_FlatSet.class);

    public O_FlatSet() {
        super("FLAT_SET");
    }

    private static class FlatSetTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private String value;

        public FlatSetTable(String name, long timestamp, String value) {
            this.name = name;
            this.timestamp = timestamp;
            this.value = value;
        }
    }
	

    /**
     * "FLAT_SET(60d, transaction, phone, SET(60d, transaction, card_no, userid), ip)"
     * <p>
     * FLAT_COUNT must has a SET target, it use the same target with SET.
     *
     * @param fql
     * @param event
     * @param helper
     * @param mode
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> doExecute(LinkStreamFQL fql,
                                         JSONObject event,
                                         Map<String, Object> helper,
                                         String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();

        // cache name
        String cacheName = genCacheName(fql, event);

        // time
        long atTime = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"), Long.class,
                System.currentTimeMillis());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());

		if(isUpdateMode(mode)) {
			String target = getStringFromEvent(event, fql.getTarget());
			
			if (isNotNull(target)) {
				if (CollectionUtils.isEmpty(fql.getOn())) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_SET invalid on[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(fql.getOn()),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
				
				Object firstOnResult = getValueFromEvent(event, fql.getOn().get(0));
                if (!LinkStreamFQLResult.class.isInstance(firstOnResult)) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_SET invalid firstOnResult[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(firstOnResult),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
                LinkStreamFQLResult firstOnlinkStreamFQLResult = (LinkStreamFQLResult) firstOnResult;
				
				
				final SortedMap<String, Field> onFields = Maps.newTreeMap();
                List<Field> onList = new ArrayList<>(fql.getOn());
                onList.set(0, firstOnlinkStreamFQLResult.getFql().getTarget());
                onList.forEach(x -> onFields.put(x.getField(), x));

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, FlatSetTable> cache = openIgniteCache(
                        cacheName, String.class, FlatSetTable.class, ttlSeconds(fql.getWindow()));
						
				String id = MD5Tool.md5ID(String.format("%s_%s", name, target));
                FlatSetTable record = cache.get(id);
                if (record == null) {
                    record = new FlatSetTable(name, atTime, target);
                } else {
                    record.timestamp = atTime;
                }
                cache.put(id, record);
			}
			
			LinkStreamFQLResult linkStreamFQLResult = new LinkStreamFQLResult();
            linkStreamFQLResult.setFql(fql);
            linkStreamFQLResult.setResult(Void.create());
            result.put(VALUE_FIELD, linkStreamFQLResult);
		}
		
		
		if (isGetMode(mode)) {
			if (CollectionUtils.isEmpty(fql.getOn())) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_SET invalid on[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(fql.getOn()),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }

            Object firstOnResult = getValueFromEvent(event, fql.getOn().get(0));
            if (!LinkStreamFQLResult.class.isInstance(firstOnResult)) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_SET invalid firstOnResult[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(firstOnResult),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }
            LinkStreamFQLResult firstOnlinkStreamFQLResult = (LinkStreamFQLResult) firstOnResult;

            Object firstOnComplexResultValue = firstOnlinkStreamFQLResult.getResult();
            if (!Collection.class.isInstance(firstOnComplexResultValue)) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_SET invalid firstOnComplexResultValue[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(firstOnComplexResultValue),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }

            final SortedMap<String, Field> onFields = Maps.newTreeMap();
            List<Field> onList = new ArrayList<>(fql.getOn());
            onList.set(0, firstOnlinkStreamFQLResult.getFql().getTarget());
            onList.forEach(x -> onFields.put(x.getField(), x));
			
			
			List<String> names = new ArrayList<>();
            Collection firstOnValues = (Collection) firstOnComplexResultValue;
            for (Object firstOnValue : firstOnValues) {
                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    if (on.getKey().equals(firstOnlinkStreamFQLResult.getFql().getTarget().getField())) {
                        nameSplits.add(String.valueOf(firstOnValue));
                    } else {
                        nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
                    }
                }
                names.add(Joiner.on(SPLIT_SIGN).join(nameSplits));
            }

            // sum query
            IgniteCache<String, FlatSetTable> cache = openIgniteCache(
                    cacheName, String.class, FlatSetTable.class, ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT value FROM FlatSetTable t1 join table(name varchar = ?) t2 " +
                            "ON t1.name = t2.name " +
                            "WHERE t1.timestamp > ? and t1.timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(
                    names.toArray(new String[0]), startTime, atTime)).getAll();

            Set<String> set = new HashSet<>();
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    set.add(String.valueOf(row.get(0)));
                }
            }

            LinkStreamFQLResult linkStreamFQLResult = new LinkStreamFQLResult();
            linkStreamFQLResult.setFql(fql);
            linkStreamFQLResult.setResult(set);
            result.put(VALUE_FIELD, linkStreamFQLResult);
			
		}
		
		return result;
		
	}
	
	    @Override
    protected Map<String, Object> defaultValue(LinkStreamFQL fql, JSONObject event,
                                               Map<String, Object> helper, String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();
        LinkStreamFQLResult linkStreamFQLResult = new LinkStreamFQLResult();
        linkStreamFQLResult.setFql(fql);
        linkStreamFQLResult.setResult(Void.create());
        result.put(VALUE_FIELD, linkStreamFQLResult);
        return result;
    }
	
	
	
}