package kr.jm.utils.elasticsearch;

import org.elasticsearch.action.ListenableActionFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kr.jm.utils.exception.JMExceptionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JMElastricsearchUtil {

	/**
	 * Gets the timeout millis.
	 *
	 * @return the timeout millis
	 */

	/**
	 * Gets the timeout millis.
	 *
	 * @return the timeout millis
	 */
	@Getter

	/**
	 * Sets the timeout millis.
	 *
	 * @param timeoutMillis
	 *            the new timeout millis
	 */

	/**
	 * Sets the timeout millis.
	 *
	 * @param timeoutMillis
	 *            the new timeout millis
	 */
	@Setter
	private static long timeoutMillis = 5000;

	private static final ObjectMapper JsonMapper = new ObjectMapper();

	static <R, T> T logExcuteAndReturn(String method, R requestBuilder,
			ListenableActionFuture<T> responseFunction) {
		log.info("[{}][timeoutMillis={}] - {}", method, timeoutMillis,
				requestBuilder.toString());
		try {
			return responseFunction.actionGet();
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
