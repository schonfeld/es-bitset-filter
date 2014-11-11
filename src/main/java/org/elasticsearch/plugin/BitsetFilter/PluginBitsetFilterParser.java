package org.elasticsearch.plugin.BitsetFilter;

import com.google.common.io.BaseEncoding;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.logging.Logger;

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
        String input = null;

        XContentParser parser = parseContext.parser();
        XContentParser.Token token;

        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("bitset".equals(currentFieldName)) {
                    input = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bitset] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if(null == input) {
            throw new QueryParsingException(parseContext.index(), "No input was given");
        }

        CompressedBitSetTermsFilter filter = new CompressedBitSetTermsFilter(input);

        return filter;
    }
}
