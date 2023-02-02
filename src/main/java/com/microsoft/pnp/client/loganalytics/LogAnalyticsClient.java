package com.microsoft.pnp.client.loganalytics;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.DefaultServiceUnavailableRetryStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public class LogAnalyticsClient implements Closeable {
    private static final String DEFAULT_URL_SUFFIX = "ods.opinsights.azure.com";
    private static final String DEFAULT_API_VERSION = "2016-04-01";
    private static final String URL_FORMAT = "https://%s.%s/api/logs?api-version=%s";
    private static final String RESOURCE_ID =
            "/subscriptions/%s/resourceGroups/%s/providers/%s/%s/%s";
    private String workspaceId;
    private String workspaceKey;
    private String url;
    private HttpClient httpClient;

    private static final Logger logger = LogManager.getLogger();

    public LogAnalyticsClient(String workspaceId, String workspaceKey) {
        this(workspaceId, workspaceKey, HttpClients.custom()
                .disableAuthCaching()
                .disableContentCompression()
                .disableCookieManagement()
                .setRetryHandler(new DefaultHttpRequestRetryHandler())
                .setServiceUnavailableRetryStrategy(new DefaultServiceUnavailableRetryStrategy(3, 1000))
                .setDefaultRequestConfig(RequestConfig.custom().setConnectTimeout(15000).setSocketTimeout(120000).build())
                .build());
    }
    public LogAnalyticsClient(String workspaceId, String workspaceKey,
                              HttpClient httpClient) {
        this(workspaceId, workspaceKey, httpClient, DEFAULT_URL_SUFFIX);
    }

    public LogAnalyticsClient(String workspaceId, String workspaceKey,
                              HttpClient httpClient, String urlSuffix) {
        this(workspaceId, workspaceKey, httpClient, urlSuffix, DEFAULT_API_VERSION);
    }

    public LogAnalyticsClient(String workspaceId, String workspaceKey,
                              HttpClient httpClient, String urlSuffix, String apiVersion) {
        if (isNullOrWhitespace(workspaceId)) {
            throw new IllegalArgumentException("workspaceId cannot be null, empty, or only whitespace");
        }

        if (isNullOrWhitespace(workspaceKey)) {
            throw new IllegalArgumentException("workspaceKey cannot be null, empty, or only whitespace");
        }

        if (httpClient == null) {
            throw new IllegalArgumentException("httpClient cannot be null");
        }

        if (isNullOrWhitespace(urlSuffix)) {
            throw new IllegalArgumentException("urlSuffix cannot be null, empty, or only whitespace");
        }

        if (isNullOrWhitespace(apiVersion)) {
            throw new IllegalArgumentException("apiVersion cannot be null, empty, or only whitespace");
        }

        this.workspaceId = workspaceId;
        this.workspaceKey = workspaceKey;
        this.httpClient = httpClient;
        this.url = String.format(URL_FORMAT, this.workspaceId, urlSuffix, apiVersion);
    }

    @Override
    public void close() throws IOException {
        if (this.httpClient instanceof Closeable) {
            ((Closeable) this.httpClient).close();
        }
    }

    public boolean ready() {
        return (this.workspaceId != null && this.workspaceKey != null);
    }

    public void send(String body, String logType) throws IOException {
        this.send(body, logType, null);
    }

    protected HttpPost getHttpPost() {
        return new HttpPost(this.url);
    }

    public void send(String body, String logType, String timestampFieldName) throws IOException {

        if (isNullOrWhitespace(body)) {
            throw new IllegalArgumentException("body cannot be null, empty, or only whitespace");
        }

        if (isNullOrWhitespace(logType)) {
            throw new IllegalArgumentException("logType cannot be null, empty, or only whitespace");
        }

        String dateString = getServerTime();
        String httpMethod = "POST";
        String contentType = "application/json";
        String xmsDate = "x-ms-date:" + dateString;
        String resource = "/api/logs";
        String stringToHash = String
                .join("\n", httpMethod, String.valueOf(body.getBytes(StandardCharsets.UTF_8).length), contentType,
                        xmsDate , resource);
        String hashedString = getHMAC256(stringToHash, workspaceKey);
        String signature = "SharedKey " + workspaceId + ":" + hashedString;

        postData(signature, dateString, body, logType, timestampFieldName);
    }

    private static final String RFC_1123_DATE = "EEE, dd MMM yyyy HH:mm:ss z";

    private static String getServerTime() {
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat(RFC_1123_DATE, Locale.US);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(calendar.getTime());
    }

    private void postData(String signature, String dateString, String json, String logName, String timefield) throws IOException {
        HttpPost httpPost = getHttpPost();
        httpPost.setHeader("Authorization", signature);
        httpPost.setHeader("content-type", "application/json");
        httpPost.setHeader("Log-Type", logName);
        httpPost.setHeader("x-ms-date", dateString);
        if (timefield != null) {
            httpPost.setHeader("time-generated-field", timefield);
        }
        String resourceId = azResourceId();
        if (resourceId != null) {
            httpPost.setHeader("x-ms-AzureResourceId", resourceId);
        }
        httpPost.setEntity(new StringEntity(json));
        HttpResponse response = httpClient.execute(httpPost);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200){
            String reason = response.getStatusLine().getReasonPhrase();
            logger.error("Could not send body to Log Analytics  : " + reason);
        }

    }

    private static String getHMAC256(String input, String key) {
        try {
            String hash;
            Mac sha256HMAC = Mac.getInstance("HmacSHA256");
            java.util.Base64.Decoder decoder = java.util.Base64.getDecoder();
            SecretKeySpec secretKey = new SecretKeySpec(decoder.decode(key.getBytes(StandardCharsets.UTF_8)), "HmacSHA256");
            sha256HMAC.init(secretKey);
            java.util.Base64.Encoder encoder = Base64.getEncoder();
            hash = new String(encoder.encode(sha256HMAC.doFinal(input.getBytes(StandardCharsets.UTF_8))));
            return hash;
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isNullOrWhitespace(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }

        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Gets the value of a System.getenv call or null if it is not set or
     * if the length is 0.
     *
     * @param key System environment variable.
     * @return value of System.getenv(key) or null.
     */
    private static String sysEnvOrNull(final String key) {
        String val = System.getenv(key);
        if (val == null || val.length() == 0) {
            return null;
        }
        return val;
    }

    /**
     * Gets Azure Resource Id from System Environment variables.
     *
     * @return ResourceId in the form of:
     * /subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{resourceProviderNamespace}/{resourceType}/{resourceName}
     */
    private static String azResourceId() {
        final String AZ_SUBSCRIPTION_ID = "AZ_SUBSCRIPTION_ID";
        final String AZ_RSRC_GRP_NAME = "AZ_RSRC_GRP_NAME";
        final String AZ_RSRC_PROV_NAMESPACE = "AZ_RSRC_PROV_NAMESPACE";
        final String AZ_RSRC_TYPE = "AZ_RSRC_TYPE";
        final String AZ_RSRC_NAME = "AZ_RSRC_NAME";

        String id = sysEnvOrNull(AZ_SUBSCRIPTION_ID);
        String grpName = sysEnvOrNull(AZ_RSRC_GRP_NAME);
        String provName = sysEnvOrNull(AZ_RSRC_PROV_NAMESPACE);
        String type = sysEnvOrNull(AZ_RSRC_TYPE);
        String name = sysEnvOrNull(AZ_RSRC_NAME);
        if (id == null || grpName == null ||
                provName == null || type == provName ||
                name == null) {
            return null;
        }
        return String.format(RESOURCE_ID, id, grpName, provName, type, name);
    }

}
