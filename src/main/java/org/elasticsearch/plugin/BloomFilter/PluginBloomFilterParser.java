package org.elasticsearch.plugin.BloomFilter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import org.apache.lucene.search.Filter;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.smile.SmileXContentParser;
import org.elasticsearch.index.cache.filter.support.CacheKeyFilter;
import org.elasticsearch.index.query.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PluginBloomFilterParser implements FilterParser {
    public static final String NAME = "bloom";
    private Client client;

    @Inject
    public PluginBloomFilterParser(Client client) {
        this.client = client;
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();
        XContentParser.Token token;

        String currentFieldName = null;
        byte[] bloomFilterBytes = null;
        Filter primaryFilter = null;
        boolean cache = true;
        //String s = Hashing.murmur3_128().hashBytes().toString();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT && currentFieldName.equals("primary")) {
                primaryFilter = parseContext.parseInnerFilter();
            } else if (token.isValue()) {
                if ("bf".equals(currentFieldName)) {
                    bloomFilterBytes = parser.binaryValue();
                } else if ("_cache".equals(currentFieldName)) {
                    cache = parser.booleanValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bloom] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if(null == primaryFilter) {
            throw new QueryParsingException(parseContext.index(), "No primary filter was given");
        }
        else if(null == bloomFilterBytes) {
            throw new QueryParsingException(parseContext.index(), "No bloom filter was given");
        }

        BloomFilter<CharSequence> bloomFilter;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bloomFilterBytes);
            bloomFilter = BloomFilter.readFrom(bais, Funnels.stringFunnel(Charsets.UTF_8));
            bais.close();
        } catch (IOException e) {
            throw new QueryParsingException(parseContext.index(), "Couldn't deserialize the given bloom filter string representation");
        }

        Filter filter = new UnfollowedFilter(bloomFilter, primaryFilter);
        if (cache) {
            ByteBuffer toHashBytes = ByteBuffer
                    .allocate(primaryFilter.toString().length() + bloomFilterBytes.length)
                    .put(primaryFilter.toString().getBytes())
                    .put(bloomFilterBytes);

            String cacheKey = Hashing.murmur3_128().hashBytes(toHashBytes.array()).toString();
            filter = parseContext.cacheFilter(filter, new CacheKeyFilter.Key(cacheKey));
        }

        return filter;
    }

}
