package org.elasticsearch.plugin.BitsetFilter;

import com.google.common.io.BaseEncoding;
import org.apache.lucene.search.Filter;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.FilterParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class PluginBitsetFilterParser implements FilterParser {
    public static final String NAME = "bitset";
    private Client client;

    @Inject
    public PluginBitsetFilterParser(Client client) {
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
        String lookupIndex = null;
        String lookupType = null;
        String lookupId = null;
        String lookupFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    lookupIndex = parser.text();
                } else if ("type".equals(currentFieldName)) {
                    lookupType = parser.text();
                } else if ("id".equals(currentFieldName)) {
                    lookupId = parser.text();
                } else if ("field".equals(currentFieldName)) {
                    lookupFieldName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[bitset] filter does not support [" + currentFieldName + "]");
                }
            }
        }

        if(null == lookupIndex) {
            throw new QueryParsingException(parseContext.index(), "No lookup index was given");
        }
        else if(null == lookupType) {
            throw new QueryParsingException(parseContext.index(), "No lookup type was given");
        }
        else if(null == lookupId) {
            throw new QueryParsingException(parseContext.index(), "No lookup id was given");
        }
        else if(null == lookupFieldName) {
            throw new QueryParsingException(parseContext.index(), "No lookup field name was given");
        }

        ImmutableRoaringBitmap bitmap = getFollowersBitmap(lookupIndex, lookupType, lookupId, lookupFieldName);
        if(null == bitmap) {
            return Queries.MATCH_NO_FILTER;
        }

        CompressedBitSetTermsFilter filter = new CompressedBitSetTermsFilter(parseContext.queryTypes(), bitmap);

        return filter;
    }

    private ImmutableRoaringBitmap getFollowersBitmap(String index, String type, String id, String fieldName) {
        GetRequest getRequest = new GetRequest(index, type, id);
        Map<String, Object> bitmapFieldSource = client.get(getRequest)
                .actionGet()
                .getSource();

        if(bitmapFieldSource.isEmpty() || !bitmapFieldSource.containsKey(fieldName)) {
            return null;
        }

        try {
            ByteBuffer bytesBuffer = ByteBuffer.wrap(
                    Snappy.uncompress(BaseEncoding.base64().decode(bitmapFieldSource.get(fieldName).toString())));
            ImmutableRoaringBitmap followersBitmap = new ImmutableRoaringBitmap(bytesBuffer);

            return followersBitmap;
        } catch (Exception e) {
            return null;
        }
    }
}
