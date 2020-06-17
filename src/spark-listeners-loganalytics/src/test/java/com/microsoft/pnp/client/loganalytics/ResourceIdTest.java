package com.microsoft.pnp.client.loganalytics;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class ResourceIdTest {

    @Rule
    public final EnvironmentVariables env = new EnvironmentVariables();

    @Mock
    private HttpClient httpClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void it_should_have_x_ms_azure_resource_Id_set() throws IOException {

        env.set("AZ_SUBSCRIPTION_ID", "1234");
        env.set("AZ_RSRC_GRP_NAME", "someGrpName");
        env.set("AZ_RSRC_PROV_NAMESPACE", "someProvNamespace");
        env.set("AZ_RSRC_TYPE", "someResourceType");
        env.set("AZ_RSRC_NAME", "someResourceName");

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(httpClient.execute(Matchers.any(HttpPost.class))).thenReturn(httpResponse);

        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);
        LogAnalyticsClient mockSut = spy(sut);
        HttpPost httpPost = spy(HttpPost.class);
        when(mockSut.getHttpPost()).thenReturn(httpPost);
        mockSut.send("someBody", "someLogType");

        Header header = httpPost.getFirstHeader("x-ms-AzureResourceId");
        assertNotNull(header);
        assertTrue(header.getValue().contains("1234"));
        assertTrue(header.getValue().contains("someGrpName"));
        assertTrue(header.getValue().contains("someProvNamespace"));
        assertTrue(header.getValue().contains("someResourceType"));
        assertTrue(header.getValue().contains("someResourceName"));
    }

    @Test
    public void it_should_have_x_ms_azure_resource_Id_null() throws IOException {

        // Not all the env variables are set.
        env.set("AZ_SUBSCRIPTION_ID", "1234");
        env.set("AZ_RSRC_TYPE", "someResourceType");
        env.set("AZ_RSRC_NAME", "someResourceName");

        String workSpaceID = "someWorkSpaceId";
        String workSpaceKey = "someKey";
        String urlSuffix = "someUrlSuffix";
        String apiVersion = "someApiVersion";

        HttpResponse httpResponse = mock(HttpResponse.class);
        StatusLine statusLine = mock(StatusLine.class);
        when(statusLine.getStatusCode()).thenReturn(200);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(httpClient.execute(Matchers.any(HttpPost.class))).thenReturn(httpResponse);

        LogAnalyticsClient sut = new LogAnalyticsClient(workSpaceID, workSpaceKey, httpClient, urlSuffix, apiVersion);
        LogAnalyticsClient mockSut = spy(sut);
        HttpPost httpPost = spy(HttpPost.class);
        when(mockSut.getHttpPost()).thenReturn(httpPost);
        mockSut.send("someBody", "someLogType");

        Header header = httpPost.getFirstHeader("x-ms-AzureResourceId");
        assertNull(header);
    }
}
