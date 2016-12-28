package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ListenableActionFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JMElastricsearchUtil {

	@Getter
	@Setter
	private static long timeoutMillis = 5000;

	private static final ObjectMapper JsonMapper = new ObjectMapper()
			.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
			.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);

	static <R, T> T logExcuteAndReturn(String method, R requestBuilder,
			ListenableActionFuture<T> responseFunction) {
		log.debug("[{}][timeoutMillis={}] - {}", method, timeoutMillis,
				requestBuilder.toString());
		try {
			return responseFunction.actionGet();
		} catch (Exception e) {
			return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
					method, requestBuilder);
		}
	}

	static <R, T> ListenableActionFuture<T> logExcuteAndReturnAsync(
			String method, R requestBuilder,
			ListenableActionFuture<T> responseFunction) {
		log.debug("[{}][timeoutMillis={}] - {}", method, timeoutMillis,
				requestBuilder.toString());
		return responseFunction;
	}

	static String buildSourceByJsonMapper(Object sourceObject) {
		try {
			return JsonMapper.writeValueAsString(sourceObject);
		} catch (JsonProcessingException e) {
			return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
					"buildSourceByJsonMapper", sourceObject);
		}
	}

}
