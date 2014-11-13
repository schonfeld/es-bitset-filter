package org.elasticsearch.plugin.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

public class UnfollowedFilter extends Filter {
    private String fieldName;
    private String termId;
    private BloomFilter bf;

    public UnfollowedFilter(String fieldName, String termId, BloomFilter bf) {
        this.fieldName = fieldName;
        this.termId = termId;
        this.bf = bf;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        TermsFilter termsFilter = new TermsFilter(fieldName, BytesRefs.toBytesRef(termId));

        FixedBitSet docIdSet = (FixedBitSet) termsFilter.getDocIdSet(context, acceptDocs);
        if (null == docIdSet) {
            return null;
        }

        FixedBitSet result = new FixedBitSet(docIdSet.cardinality());

        DocIdSetIterator iterator = docIdSet.iterator();
        int docId;
        AtomicReader reader = context.reader();

        while ((docId = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            Document document = reader.document(docId);
            Uid uid = Uid.createUid(document.getField("_uid").stringValue());
            if (!bf.isPresent(uid.id())) {
                result.set(docId);
            }
        }

        return result;
    }
}
