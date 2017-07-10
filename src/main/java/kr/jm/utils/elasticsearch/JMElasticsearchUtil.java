package kr.jm.utils.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.jm.utils.exception.JMExceptionManager;
import kr.jm.utils.helper.JMLog;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ListenableActionFuture;

import java.util.Arrays;

@Slf4j
public class JMElasticsearchUtil {

    private static final ObjectMapper JsonMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);

    public static <R extends ActionRequestBuilder, T> T
    logRequestQueryAndReturn(
            String method, R requestBuilder,
            ListenableActionFuture<T> responseFunction) {
        return logRequestQueryAndReturn(method, requestBuilder,
                responseFunction, null);
    }

    public static <R extends ActionRequestBuilder, T> T logRequestQueryAndReturn(
            String method, R requestBuilder,
            ListenableActionFuture<T> responseFunction, Long timeoutMillis) {
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

    public static <R extends ActionRequestBuilder> R logRequestQuery(
            String method,
            R requestBuilder, Object... params) {
        if (params == null)
            JMLog.debug(log, method, requestBuilder);
        else
            JMLog.debug(log, method, Arrays.asList(params), requestBuilder);
        return requestBuilder;
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
