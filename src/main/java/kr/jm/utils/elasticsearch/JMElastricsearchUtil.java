package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ListenableActionFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JMElastricsearchUtil {

	private static final ObjectMapper JsonMapper = new ObjectMapper();

	static <R, T> T logExcuteAndReturn(String method, R requestBuilder,
			ListenableActionFuture<T> responseFunction) {
		log.info("[{}] - {}", method, requestBuilder.toString());
		try {
			return responseFunction
					.actionGet(JMElasticsearchClient.timeoutMillis);
		} catch (Exception e) {
			return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
					method, requestBuilder);
		}
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
