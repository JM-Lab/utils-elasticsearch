package kr.jm.utils.elasticsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestBuilder;

import java.util.Arrays;
import java.util.Map;

/**
 * The type Jm elasticsearch util.
 */
@Slf4j
public class JMElasticsearchUtil {

    private static final ObjectMapper JsonMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
    private static final TypeReference<Map<String, ?>>
            MAP_TYPE_REFERENCE = new TypeReference<Map<String, ?>>() {};

    /**
     * Log request query and return t.
     *
     * @param <R>              the type parameter
     * @param <T>              the type parameter
     * @param method           the method
     * @param requestBuilder   the request builder
     * @param responseFunction the response custom
     * @return the t
     */
    public static <R extends ActionRequestBuilder, T> T logRequestQueryAndReturn(
            String method, R requestBuilder, ActionFuture<T> responseFunction) {
        return logRequestQueryAndReturn(method, requestBuilder,
                responseFunction, null);
    }

    /**
     * Log request query and return t.
     *
     * @param <R>              the type parameter
     * @param <T>              the type parameter
     * @param method           the method
     * @param requestBuilder   the request builder
     * @param responseFunction the response custom
     * @param timeoutMillis    the timeout millis
     * @return the t
     */
    public static <R extends ActionRequestBuilder, T> T logRequestQueryAndReturn(
            String method, R requestBuilder,
            ActionFuture<T> responseFunction, Long timeoutMillis) {
        try {
            logRequestQuery(method, requestBuilder, timeoutMillis);
            return timeoutMillis == null || timeoutMillis == 0 ?
                    responseFunction.actionGet() : responseFunction
                    .actionGet(timeoutMillis);
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
                    method, requestBuilder);
        }
    }

    /**
     * Log request query r.
     *
     * @param <R>            the type parameter
     * @param method         the method
     * @param requestBuilder the request builder
     * @param params         the params
     * @return the r
     */
    public static <R extends ActionRequestBuilder> R logRequestQuery(
            String method,
            R requestBuilder, Object... params) {
        if (params == null)
            JMLog.debug(log, method, requestBuilder);
        else
            JMLog.debug(log, method, Arrays.asList(params), requestBuilder);
        return requestBuilder;
    }

    /**
     * Build source by json mapper map.
     *
     * @param sourceObject the source object
     * @return the map
     */
    static Map<String, ?> buildSourceByJsonMapper(Object sourceObject) {
        try {
            return JsonMapper.convertValue(sourceObject, MAP_TYPE_REFERENCE);
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
                    "buildSourceByJsonMapper", sourceObject);
        }
    }

    /**
     * Build source by json mapper map.
     *
     * @param jsonObjectString the json object string
     * @return the map
     */
    static Map<String, ?> buildSourceByJsonMapper(String jsonObjectString) {
        try {
            return JsonMapper.readValue(jsonObjectString, MAP_TYPE_REFERENCE);
        } catch (Exception e) {
            return JMExceptionManager.handleExceptionAndThrowRuntimeEx(log, e,
                    "buildSourceByJsonMapper", jsonObjectString);
        }
    }

}
