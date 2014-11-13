package org.elasticsearch.plugin.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.apache.lucene.search.Filter;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;

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

        String lookupFieldName = null;
        String lookupId = null;
        byte[] bloomFilterBytes = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    lookupFieldName = parser.text();
                } else if ("id".equals(currentFieldName)) {
                    lookupId = parser.text();
                } else if ("bf".equals(currentFieldName)) {
                    bloomFilterBytes = parser.binaryValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bloom] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if(null == lookupFieldName) {
            throw new QueryParsingException(parseContext.index(), "No lookup field name was given");
        }
        else if(null == lookupId) {
            throw new QueryParsingException(parseContext.index(), "No lookup id was given");
        }
        else if(null == bloomFilterBytes) {
            throw new QueryParsingException(parseContext.index(), "No bloom filter was given");
        }

        BloomFilter bloomFilter = BloomFilter.deserialize(bloomFilterBytes);
        if (null == bloomFilter) {
            throw new QueryParsingException(parseContext.index(), "Couldn't deserialize bloom filter");
        }

        return new UnfollowedFilter(lookupFieldName, lookupId, bloomFilter);
    }

}
