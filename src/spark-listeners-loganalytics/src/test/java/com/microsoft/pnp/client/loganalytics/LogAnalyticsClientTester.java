package com.microsoft.pnp.client.loganalytics;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

public class LogAnalyticsClientTester {

    @Mock
    private HttpClient httpClient;


    @Before
    public void setUp() {

        MockitoAnnotations.initMocks(this);
    }

    @Test(expected = IllegalArgumentException.class)
    public void it_should_not_initialize_with_null_or_empty_workspaceID() {

        String workSpaceKey = "someKey";
        LogAnalyticsClient sut = new LogAnalyticsClient("", workSpaceKey);
    }

    @Test(expected = IllegalArgumentException.class)
    public void it_should_not_initialize_with_null_or_empty_workspaceKey() {

        String workSpaceID = "someWorkSpaceId";
        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void it_should_not_initialize_with_null_httpClient() {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void it_should_not_initialize_with_null_or_empty_urlSuffix() {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String apiVersion = "someApiVersion";
        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, "", apiVersion);
    }

    @Test(expected = IllegalArgumentException.class)
    public void it_should_not_initialize_with_null_or_empty_apiVersion() {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, "");
    }

    @Test
    public void it_should_initialize_successfully() {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        // this is static construction in actual class
        String expectedURL = String.format("https://%s.%s/api/logs?api-version=%s"
                , workSpaceID
                , urlSuffix
                , apiVersion);

        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);


        try {
            Field workspaceIdField = LogAnalyticsClient.class.getDeclaredField("workspaceId");
            workspaceIdField.setAccessible(true);
            Field workspaceKeyField = LogAnalyticsClient.class.getDeclaredField("workspaceKey");
            workspaceKeyField.setAccessible(true);
            Field urlField = LogAnalyticsClient.class.getDeclaredField("url");
            urlField.setAccessible(true);
            Field httpClientField = LogAnalyticsClient.class.getDeclaredField("httpClient");
            httpClientField.setAccessible(true);

            String actualWorkSpaceID = (String) workspaceIdField.get(sut);
            assert (actualWorkSpaceID.contentEquals(workSpaceID));

            String actualWorkSpaceKey = (String) workspaceKeyField.get(sut);
            assert (actualWorkSpaceKey.contentEquals(workSpaceKey));

            String actualURL = (String) urlField.get(sut);
            assert (expectedURL.contentEquals(actualURL));


        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    @Test(expected = IOException.class)
    public void it_should_not_send_when_empty_body() throws IOException {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);

        sut.send(null, "someLogType");

    }

    @Test(expected = IOException.class)
    public void it_should_not_send_when_empty_logtype() throws IOException {

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);
        sut.send("someBody", "");

    }

    @Test
    public void it_should_send_to_logAnalytics_when_body_and_log_type_not_empty() throws IOException {

        //arrange
        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(httpClient.execute(Matchers.any(HttpPost.class))).thenReturn(httpResponse);

        //act
        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);
        sut.send("someBody", "someLogType");

        //assert
        verify(httpClient, times(1)).execute(Matchers.any(HttpPost.class));

    }


}
