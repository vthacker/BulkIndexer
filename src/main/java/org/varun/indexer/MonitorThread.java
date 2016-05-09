package org.varun.indexer;


import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;

public class MonitorThread implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String collection;
  private final CloudSolrClient client;

  public MonitorThread(String collection, CloudSolrClient client) {
    this.collection = collection;
    this.client = client;
  }

  @Override
  public void run() {
    while (true) {
      CollectionAdminRequest.ClusterStatus clusterStatus = new CollectionAdminRequest.ClusterStatus();
      clusterStatus.setCollectionName(collection);
      try {
        CollectionAdminResponse response = clusterStatus.process(client);
        if (!isHealthy(response)) {
          log.warn("Replica(s) have gone into recovery");
        } else {
          Thread.sleep(10*1000);
        }
      } catch (Exception e) {
        continue;
      }
    }
  }

  private boolean isHealthy(CollectionAdminResponse response) {
    NamedList<Object> rsp = response.getResponse();
    NamedList<Object> cluster = (NamedList<Object>) rsp.get("cluster");
    SimpleOrderedMap<Object> collections = (SimpleOrderedMap<Object>) cluster.get("collections");
    LinkedHashMap<Object, Object> collectionMap = (LinkedHashMap<Object, Object>) collections.get(collection);
    LinkedHashMap<Object, Object> shardsMap = (LinkedHashMap<Object, Object>) collectionMap.get("shards");
    for (Map.Entry<Object, Object> shard : shardsMap.entrySet()) {
      LinkedHashMap<Object, Object> shardMap = (LinkedHashMap<Object, Object>) shard.getValue();
      LinkedHashMap<Object, Object> replicasMap = (LinkedHashMap<Object, Object>) shardMap.get("replicas");
      for (Map.Entry<Object, Object> replica : replicasMap.entrySet()) {
        String state = (String) ((LinkedHashMap)replica.getValue()).get("state");
        if (!state.equals("active")) {
          return false;
        }
      }
    }
    return true;
  }
}
