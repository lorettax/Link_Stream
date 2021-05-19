package com.lorettax.StreamExtractionEngine.core.functions;


import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.lorettax.StreamExtractionEngine.core.Field;
import com.lorettax.StreamExtractionEngine.core.LinkStreamFQL;
import com.lorettax.StreamExtractionEngine.tools.JsonTool;
import com.lorettax.StreamExtractionEngine.tools.MD5Tool;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

public class O_Max extends AbstractFunction {
	
    private static final Logger logger = LoggerFactory.getLogger(O_Max.class);

    public O_Max() {
        super("MAX");
    }

    private static class MaxTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;

        public MaxTable(String name, long timestamp, double amount) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MaxTable maxTable = (MaxTable) o;

            if (timestamp != maxTable.timestamp){
                return false;
            }

            if (Double.compare(maxTable.amount, amount) != 0) {
                return false;
            }
            return name != null ? name.equals(maxTable.name) : maxTable.name == null;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            temp = Double.doubleToLongBits(amount);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }
    }


    @Override
    public Map<String, Object> doExecute(LinkStreamFQL fql,
                                         JSONObject event,
                                         Map<String, Object> helper,
                                         String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();

        // on
        final SortedMap<String, Field> onFields = sortedOn(fql);

        // cache name
        String cacheName = genCacheName(fql, event);

        // time
        long atTime = ceilTimestamp(JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"),
                Long.class, System.currentTimeMillis()), fql.getWindow());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());
        String windowSegmentId = getWindowSegmentId(atTime, fql.getWindow());
		
		
		
        if (isUpdateMode(mode)) {
            String target = getStringFromEvent(event, fql.getTarget());
            if (isValidNumber(target)) {
                double amount = new BigDecimal(target).doubleValue();

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, MaxTable> cache = openIgniteCache(cacheName, String.class, MaxTable.class,
                        ttlSeconds(fql.getWindow()));

                String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);
                MaxTable newRecord = new MaxTable(name, atTime, amount);
                int retryTimes = RETRY_TIMES;
                boolean succeed;
                do {
                    MaxTable oldRecord = cache.get(id);
                    if (oldRecord != null) {
                        newRecord.amount = Math.max(oldRecord.amount, newRecord.amount);
                    } else {
                        oldRecord = newRecord;
                        cache.putIfAbsent(id, oldRecord);
                    }
                    succeed = cache.replace(id, oldRecord, newRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalStateException(String.format(
                            "MAX failed to update index[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(name),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
            }

            result.put(VALUE_FIELD, Void.create());
        }
		
        if (isGetMode(mode)) {
            List<String> nameSplits = new ArrayList<>();
            for (Map.Entry<String, Field> on : onFields.entrySet()) {
                nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
            }
            String name = Joiner.on(SPLIT_SIGN).join(nameSplits);
            IgniteCache<String, MaxTable> cache = openIgniteCache(cacheName, String.class, MaxTable.class,
                    ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT max(amount) FROM MaxTable " +
                            "WHERE name = ? and timestamp > ? and timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
            double sum = 0.0;
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    sum += (Double) row.get(0);
                }
            }
            result.put(VALUE_FIELD, sum);
        }
        return result;
		
	}
	
	
}