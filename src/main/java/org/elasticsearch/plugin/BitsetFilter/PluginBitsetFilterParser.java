package org.elasticsearch.plugin.BitsetFilter;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.DocIdBitSet;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.BitSet;

public class PluginBitsetFilterParser implements FilterParser {
    public static final String NAME = "bitset";

    @Inject
    public PluginBitsetFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        Integer input = null;

        XContentParser parser = parseContext.parser();
        XContentParser.Token token;

        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("bitset".equals(currentFieldName)) {
                    input = parser.intValue();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bitset] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if(null == input) {
            throw new QueryParsingException(parseContext.index(), "No input was given");
        }

        BitSet bitSet = new BitSet();
        bitSet.set(input);

        if(bitSet.isEmpty()) {
            throw new QueryParsingException(parseContext.index(), "Empty bitset given.");
        }

        Filter filter;
        filter = new PluginBitsetLuceneFilter(bitSet);

        return filter;
    }
}
