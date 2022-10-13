import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.sound.midi.SysexMessage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ElasticsearchJava {

    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;

    private static synchronized void makeConnection() {
        // Create the low-level client
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "password"));

        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestClient restClient = builder.build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        client = new ElasticsearchClient(transport);
        asyncClient = new ElasticsearchAsyncClient(transport);
    }

    public static void main(String[] args) throws IOException {
        makeConnection();

        // Index data to an index products
        Product product = new Product("abc", "Bag", 42);
        IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                .index("products")
                .id("abc")
                .document(product)
                .build();
        client.index(indexRequest);

        // Index another document into products
        Product product1 = new Product("efg", "Bag", 42);
        client.index(builder -> builder
                .index("products")
                .id(product1.getId())
                .document(product1)
        );

        // Search for a data
        TermQuery query = QueryBuilders.term()
                .field("name")
                .value("bag")
                .build();

        SearchRequest request = new SearchRequest.Builder()
                .index("products")
                .query(query._toQuery())
                .build();

        SearchResponse<Product> search =
                client.search(
                        request,
                        Product.class
                );

        // Print out the response
        System.out.println("The found results are: ");
        for (Hit<Product> hit: search.hits().hits()) {
            Product pd = hit.source();
            System.out.println(pd);
        }

        System.out.println("=================================================\n");

        // Match search
        String searchText = "bag";
        SearchResponse<Product> response1 = client.search(s -> s
                        .index("products")
                        .query(q -> q
                                .match(t -> t
                                        .field("name")
                                        .query(searchText)
                                )
                        ),
                Product.class
        );

        TotalHits total1 = response1.hits().total();
        boolean isExactResult = total1.relation() == TotalHitsRelation.Eq;

        if (isExactResult) {
            System.out.println("There are " + total1.value() + " results");
        } else {
            System.out.println("There are more than " + total1.value() + " results");
        }

        System.out.println("The scores are:");

        List<Hit<Product>> hits1 = response1.hits().hits();
        for (Hit<Product> hit: hits1) {
            Product pd2 = hit.source();
            System.out.println("Found product " + pd2.getId() + ", score " + hit.score());
        }

        System.out.println("=================================================\n");

        // Term search
        SearchResponse<Product> search1 = client.search(s -> s
                        .index("products")
                        .query(q -> q
                                .term(t -> t
                                        .field("name")
                                        .value(v -> v.stringValue("bag"))
                                )),
                Product.class);

        System.out.println("The term search results are:");

        for (Hit<Product> hit: search1.hits().hits()) {
            Product pd = hit.source();
            System.out.println(pd);
        }

        System.out.println("=================================================\n");

        System.out.println("Splitting complex DSL ");

        // Splitting complex DSL
        TermQuery termQuery = TermQuery.of(t ->t.field("name").value("bag"));

        SearchResponse<Product> search2 = client.search(s -> s
                .index("products")
                .query(termQuery._toQuery()),
                Product.class
        );

        for (Hit<Product> hit: search2.hits().hits()) {
            Product pd = hit.source();
            System.out.println(pd);
        }

        System.out.println("=================================================\n");
        System.out.println("Compound bool search");
        // Search by product name
        Query byName = MatchQuery.of(m -> m
                .field("name")
                .query("bag")
        )._toQuery();

        // Search by max price
        Query byMaxPrice = RangeQuery.of(r -> r
                .field("price")
                .gte(JsonData.of(10))
        )._toQuery();

        // Combine name and price queries to search the product index
        SearchResponse<Product> response = client.search(s -> s
                        .index("products")
                        .query(q -> q
                                .bool(b -> b
                                        .must(byName)
                                        .should(byMaxPrice)
                                )
                        ),
                Product.class
        );


        List<Hit<Product>> hits = response.hits().hits();
        for (Hit<Product> hit: hits) {
            Product product2 = hit.source();
            System.out.println("Found product " + product2.getId() + ", score " + hit.score());
        }

        System.out.println("=================================================\n");
        System.out.println("Creating an aggregations");

        // Creating aggregations
        // Void.class here means no documents in the response
        SearchResponse<Void> search3 = client.search( b-> b
                .index("products")
                .size(0)
                .aggregations("price-histo", a -> a
                        .histogram(h -> h
                                .field("price")
                                .interval(20.0)
                        )
                ),
                Void.class
        );

        long firstBucketCount = search3.aggregations()
                .get("price-histo")
                .histogram()
                .buckets().array()
                .get(0)
                .docCount();

        System.out.println("doc count: " + firstBucketCount);

        System.out.println("=================================================\n");
        System.out.println("Creating an aggregation from JSON");

        String aggstr = "\n" +
           " { \n" +
           "   \"size\": 0, \n" +
           "   \"aggs\": { \n" +
           "     \"price-histo\": {  \n" +
           "       \"histogram\": { \n" +
           "         \"field\": \"price\", \n" +
           "         \"interval\": 20 \n" +
           "       } \n" +
           "     } \n" +
           "   } \n" +
           " } ";

        System.out.println("agg is: " + aggstr  );

        InputStream agg = new ByteArrayInputStream(aggstr.getBytes());
        SearchResponse<Void> searchAgg = client
                .search(b -> b
                        .index("products")
                        .withJson(agg),
                        Void.class
                );

        firstBucketCount = searchAgg.aggregations()
                .get("price-histo")
                .histogram()
                .buckets().array()
                .get(0)
                .docCount();

        System.out.println("doc count: " + firstBucketCount);

        System.out.println("=================================================\n");

    }
}
