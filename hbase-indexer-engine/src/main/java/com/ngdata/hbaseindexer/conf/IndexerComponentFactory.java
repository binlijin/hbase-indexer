package com.ngdata.hbaseindexer.conf;

import java.io.InputStream;
import java.util.Map;


public interface IndexerComponentFactory {
    void configure(InputStream is, Map<String, String> connectionParams) throws IndexerConfException;

    IndexerConf createIndexerConf() throws IndexerConfException;

}
