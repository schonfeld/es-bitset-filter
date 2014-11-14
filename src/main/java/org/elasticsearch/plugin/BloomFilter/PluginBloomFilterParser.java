package org.elasticsearch.plugin.BloomFilter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.lucene.search.Filter;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.ByteArrayInputStream;
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

        ByteArrayInputStream bais = new ByteArrayInputStream(bloomFilterBytes);
        BloomFilter<CharSequence> bloomFilter = BloomFilter.readFrom(bais, Funnels.stringFunnel(Charsets.UTF_8));
        bais.close();

        return new UnfollowedFilter(lookupFieldName, lookupId, bloomFilter);
    }

}
