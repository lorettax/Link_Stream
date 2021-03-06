package com.lorettax.StreamExtractionEngine.core.functions;


import com.alibaba.fastjson.JSONObject;
import com.lorettax.StreamExtractionEngine.core.Field;
import com.lorettax.StreamExtractionEngine.core.LinkStreamFQL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class F_Sqrt extends AbstractFunction {
	
	public F_Sqrt() {
		super("F_SQRT");
	}
	
	@Override
	protected Map<String, Object> doExecute(LinkStreamFQL fql, 
											JSONObject event,
											Map<String, Object> helper,
											String mode) throws Exception {
		final List<Field> onFields = fql.getOn();
		if(onFields.size() != 1) {
			throw new IllegalArgumentException(String.format(
					"op[%s] invalid on[%s], featureDSL[%s], event[%s]",
					fql.getOp(),
					JSONObject.toJSONString(fql.getOn()),
					JSONObject.toJSONString(fql),
					JSONObject.toJSONString(event)));
		}
		
		double on1Value = getDoubleFromConditionOrEvent(event, onFields.get(0));
		double value = Math.sqrt(on1Value);
		
		Map<String, Object> result = new HashMap<>();
		result.put(VALUE_FIELD, value);
		return result;
	}
	
}