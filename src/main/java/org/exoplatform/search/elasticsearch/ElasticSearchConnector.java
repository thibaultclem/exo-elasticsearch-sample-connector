package org.exoplatform.search.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.commons.api.search.SearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchContext;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringWriter;
import java.util.*;

public class ElasticSearchConnector extends SearchServiceConnector {

    private Map<String, String> sortMapping = new HashMap<String, String>();

    public ElasticSearchConnector(InitParams initParams) {
        super(initParams);

        sortMapping.put("date", "createdDate");
        sortMapping.put("relevancy", "_score");
        sortMapping.put("title", "title");
    }

    @Override
    public Collection<SearchResult> search(SearchContext context, String query, Collection<String> sites, int offset, int limit, String sort, String order) {
        Collection<SearchResult> results = new ArrayList<SearchResult>();

        String esQuery = "{\n" +
                "     \"from\" : " + offset + ", \"size\" : " + limit + ",\n" +
                "     \"sort\" : [\n" +
                "       { \"" + sortMapping.get(sort) + "\" : {\"order\" : \"" + order + "\"}}\n" +
                "     ],\n" +
                "     \"query\": {\n" +
                "        \"filtered\" : {\n" +
                "            \"query\" : {\n" +
                "                \"query_string\" : {\n" +
                "                    \"query\" : \"" + query + "\"\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "     },\n" +
                "     \"highlight\" : {\n" +
                "       \"fields\" : {\n" +
                "         \"text\" : {\"fragment_size\" : 150, \"number_of_fragments\" : 3}\n" +
                "       }\n" +
                "     }\n" +
                "}";


        try {
            HttpClient client = new DefaultHttpClient();
            HttpPost request = new HttpPost("http://localhost:8080/elasticsearch-war/forum/topic/_search");
            StringEntity input = new StringEntity(esQuery);
            request.setEntity(input);

            HttpResponse response = client.execute(request);
            StringWriter writer = new StringWriter();
            IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
            String jsonResponse = writer.toString();

            JSONParser parser = new JSONParser();

            Map json = (Map)parser.parse(jsonResponse);
            JSONObject jsonResult = (JSONObject) json.get("hits");
            JSONArray jsonHits = (JSONArray) jsonResult.get("hits");
            for(Object jsonHit : jsonHits) {
                JSONObject hitSource = (JSONObject) ((JSONObject) jsonHit).get("_source");
                String title = (String) hitSource.get("title");
                String description = (String) hitSource.get("description");
                String url = (String) hitSource.get("url");
                Long createdDate = (Long) hitSource.get("createdDate");
                Double score = (Double) ((JSONObject) jsonHit).get("_score");

                results.add(new SearchResult(
                        url,
                        title,
                        description,
                        description,
                        "http://dc348.4shared.com/img/9yBXxM_E/s3/12d92930df8/pikachu",
                        createdDate,
                        score.longValue()
                ));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }
}
