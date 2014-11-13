package org.elasticsearch.plugin.BitsetFilter;

import org.elasticsearch.client.transport.TransportClient;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class BitsetHelper implements AutoCloseable {
    private TransportClient elasticsearch;

    public BitsetHelper(TransportClient elasticsearch) {
        this.elasticsearch = elasticsearch;
    }

    private Boolean storeListOfFollowers(List<String> ids) {
        try {
            MutableRoaringBitmap b = MutableRoaringBitmap.bitmapOf();
            for(String id : ids) {
                b.add(Integer.valueOf(id));
            }

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            b.serialize(dos);
            dos.close();

            Snappy.compress(bos.toByteArray());
        } catch (IOException e) {

            return false;
        }

        return true;
    }

    @Override
    public void close() throws Exception {
        elasticsearch.close();
    }
}
