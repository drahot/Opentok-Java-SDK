/**
 * OpenTok Java SDK
 * Copyright (C) 2015 TokBox, Inc.
 * http://www.tokbox.com
 *
 * Licensed under The MIT License (MIT). See LICENSE file for more information.
 */
package com.opentok.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ning.http.client.*;
import com.ning.http.client.filter.FilterContext;
import com.ning.http.client.filter.FilterException;
import com.ning.http.client.filter.RequestFilter;
import com.opentok.ArchiveProperties;
import com.opentok.constants.Version;
import com.opentok.exception.OpenTokException;
import com.opentok.exception.RequestException;

public class HttpClient extends AsyncHttpClient {
    
    private final String apiUrl;
    private final int apiKey;

    private HttpClient(Builder builder) {
        super(builder.config);
        this.apiKey = builder.apiKey;
        this.apiUrl = builder.apiUrl;
    }

    public String createSession(Map<String, List<String>> params) throws RequestException {
        final String message = "Could not create an OpenTok Session.";

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 200) {
                    responseString = response.getResponseBody();
                } else {
                    handleError(response, message);
                }
                return responseString;
            }
        };

        BoundRequestBuilder builder = this.preparePost(this.apiUrl + "/session/create")
                .setFormParams(params);

        return request(builder, handler, message);
    }

    public String getArchive(String archiveId) throws RequestException {
        String url = this.apiUrl + "/v2/partner/" + this.apiKey + "/archive/" + archiveId;

        final String id = archiveId;
        final String message = "Could not get an OpenTok Archive.";

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 200) {
                    responseString = response.getResponseBody();
                } else {
                    Map<Integer, String> errorMessages = new HashMap<Integer, String>();
                    errorMessages.put(400, " The archiveId was invalid. archiveId: " + id);
                    handleError(response, message, errorMessages);
                }
                return responseString;
            }
        };

        return request(this.prepareGet(url), handler, message);
    }

    public String getArchives(int offset, int count) throws RequestException {
        StringBuilder sb = new StringBuilder();
        sb.append(this.apiUrl).append("/v2/partner/").append(this.apiKey).append("/archive");
        if (offset != 0 || count != 1000) {
            sb.append("?");
            if (offset != 0) {
                sb.append("offset=").append(Integer.toString(offset));
            }
            if (count != 1000) {
                if (offset != 0) {
                    sb.append('&');
                }
                sb.append("count=").append(Integer.toString(count));
            }
        }
        String url = sb.toString();
        final String message = "Could not get OpenTok Archives.";

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 200) {
                    responseString = response.getResponseBody();
                } else {
                    handleError(response, message);
                }
                return responseString;
            }
        };

        return request(this.prepareGet(url), handler, message);
    }

    public String startArchive(String sessionId, ArchiveProperties properties)
            throws OpenTokException {
        // TODO: maybe use a StringBuilder?
        String url = this.apiUrl + "/v2/partner/" + this.apiKey + "/archive";

        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        ObjectNode requestJson = nodeFactory.objectNode();
        requestJson.put("sessionId", sessionId);
        requestJson.put("hasVideo", properties.hasVideo());
        requestJson.put("hasAudio", properties.hasAudio());
        requestJson.put("outputMode", properties.outputMode().toString());

        if (properties.name() != null) {
            requestJson.put("name", properties.name());
        }

        String requestBody;
        try {
            requestBody = new ObjectMapper().writeValueAsString(requestJson);
        } catch (JsonProcessingException e) {
            throw new OpenTokException("Could not start an OpenTok Archive. The JSON body encoding failed.", e);
        }

        final String id = sessionId;
        final String message = "Could not start an OpenTok Archive.";

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 200) {
                    responseString = response.getResponseBody();
                } else {
                    Map<Integer, String> errorMessages = new HashMap<Integer, String>();
                    errorMessages.put(404, " The sessionId does not exist. sessionId = " + id);
                    errorMessages.put(409, " The session is either peer-to-peer or already recording. sessionId = " + id);
                    handleError(response, message, errorMessages);
                }
                return responseString;
            }
        };

        BoundRequestBuilder builder = this.preparePost(url)
                                    .setBody(requestBody)
                                    .setHeader("Content-Type", "application/json");

        return request(builder, handler, message);
    }

    public String stopArchive(String archiveId) throws RequestException {
        // TODO: maybe use a StringBuilder?
        String url = this.apiUrl + "/v2/partner/" + this.apiKey + "/archive/" + archiveId + "/stop";

        final String id = archiveId;
        final String message = "Could not stop an OpenTok Archive.";

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 200) {
                    responseString = response.getResponseBody();
                } else {
                    Map<Integer, String> errorMessages = new HashMap<Integer, String>();
                    errorMessages.put(404, " The archiveId does not exist. archiveId = " + id);
                    errorMessages.put(409, " The archive is not being recorded. archiveId = " + id);
                    handleError(response, message, errorMessages);
                }
                return responseString;
            }
        };

        return request(this.preparePost(url), handler, message);
    }

    public String deleteArchive(String archiveId) throws RequestException {
        String url = this.apiUrl + "/v2/partner/" + this.apiKey + "/archive/" + archiveId;
        final String id = archiveId;
        final String message = "Could not delete an OpenTok Archive. archiveId = " + id;

        RequestHandler handler = new RequestHandler() {
            @Override
            public String handle(Response response) throws IOException, RequestException {
                String responseString = null;
                if (response.getStatusCode() == 204) {
                    responseString = response.getResponseBody();
                } else {
                    Map<Integer, String> errorMessages = new HashMap<Integer, String>();
                    errorMessages.put(409, " The status was not \"uploaded\", \"available\"," +
                            " or \"deleted\". archiveId = " + id);
                    handleError(response, "Could not delete an OpenTok Archive.", errorMessages);
                }
                return responseString;
            }
        };

        return request(this.prepareDelete(url), handler, message);
    }

    private String request(BoundRequestBuilder builder, RequestHandler handler, String errorMessage)
            throws RequestException {
        Future<Response> request = builder.execute();
        try {
            Response response = request.get();
            return handler.handle(response);
            // if we only wanted Java 7 and above, we could DRY this into one catch clause
        } catch (InterruptedException e) {
            throw new RequestException(errorMessage, e);
        } catch (ExecutionException e) {
            throw new RequestException(errorMessage, e);
        } catch (IOException e) {
            throw new RequestException(errorMessage, e);
        }
    }

    private interface RequestHandler {
        String handle(Response response) throws IOException, RequestException;
    }

    private void handleError(Response response, String message) throws RequestException {
        handleError(response, message, null);
    }

    private void handleError(Response response, String message, Map<Integer, String> errorMessages) 
            throws RequestException {
        int statusCode = response.getStatusCode();
        if (errorMessages != null && errorMessages.containsKey(statusCode)) {
            throw new RequestException(message + errorMessages.get(statusCode));
        }

        switch (statusCode) {
            case 400:
                throw new RequestException(message);
            case 403:
                throw new RequestException(message + " The request was not authorized.");
            case 500:
                throw new RequestException(message + " A server error occurred.");
            default:
                throw new RequestException(message + " The server response was invalid." +
                        " response code: " + response.getStatusCode());
        }
    }

    public static class Builder {
        private final int apiKey;
        private final String apiSecret;
        private String apiUrl;
        private AsyncHttpClientConfig config;

        public Builder(int apiKey, String apiSecret) {
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
        }

        public Builder apiUrl(String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public HttpClient build() {
            this.config = new AsyncHttpClientConfig.Builder()
                    .setUserAgent("Opentok-Java-SDK/"+Version.VERSION)
                    .addRequestFilter(new PartnerAuthRequestFilter(this.apiKey, this.apiSecret))
                    .build();
            // NOTE: not thread-safe, config could be modified by another thread here?
            HttpClient client = new HttpClient(this);
            return client;
        }
    }

    @SuppressWarnings("unchecked")
    static class PartnerAuthRequestFilter implements RequestFilter {

        private int apiKey;
        private String apiSecret;

        public PartnerAuthRequestFilter(int apiKey, String apiSecret) {
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
        }

        public FilterContext filter(FilterContext ctx) throws FilterException {
            return new FilterContext.FilterContextBuilder(ctx)
                    .request(new RequestBuilder(ctx.getRequest())
                            .addHeader("X-TB-PARTNER-AUTH", this.apiKey+":"+this.apiSecret)
                            .build())
                    .build();
        }
    }

}
