package org.elasticsearch.plugin.BitsetFilter;

import com.google.common.hash.BloomFilter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;

public class UnfollowedFilter extends Filter {
    private String twitterUserId;
    private BloomFilter<CharSequence> bf;

    public UnfollowedFilter(String twitterUserId, BloomFilter<CharSequence> bf) {
        this.twitterUserId = twitterUserId;
        this.bf = bf;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        TermsFilter termsFilter = new TermsFilter(new Term("following_id", BytesRefs.toBytesRef(twitterUserId)));

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
            if (!bf.mightContain(uid.id())) {
                result.set(docId);
            }
        }

        return result;
    }
}
