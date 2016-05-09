package org.varun.indexer;

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mvn clean package
 * java -jar bulk-indexer-1.0-SNAPSHOT-jar-with-dependencies.jar
 * nohup java -jar bulk-indexer-1.0-SNAPSHOT-jar-with-dependencies.jar &> nohup.out &
 * nohup java -Dcollection=gettingstarted -DzkHost=localhost:9983 -DnumIndexers=10 -DbatchSize=200 -jar bulk-indexer-1.0-SNAPSHOT-jar-with-dependencies.jar &> nohup.out &
 */
public class BulkIndexer {

  private static Random r = new Random();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CloudSolrClient client;
  private final String collection;

  AtomicInteger idValue = new AtomicInteger(0);

  public static void main(String[] args) {

    int numIndexers = Integer.getInteger("numIndexers", 10);
    String collection = System.getProperty("collection", "gettingstarted");
    String zkHost = System.getProperty("zkHost", "localhost:9983");
    int batchSize = Integer.getInteger("batchSize", 200);

    log.info("starting indexing for collection={} zkHost={} numIndexers={} batchSize={}",
            collection, zkHost, numIndexers, batchSize);

    BulkIndexer bulkIndexer = new BulkIndexer(zkHost, collection);
    bulkIndexer.startIndexing(numIndexers, batchSize);
  }

  public BulkIndexer(String zkHost, String collection) {
    client = new CloudSolrClient(zkHost);
    this.collection = collection;
  }

  public void startIndexing(int numIndexers, int batchSize) {
    List<Thread> indexers = new ArrayList<>(numIndexers);
    for (int i=0; i<numIndexers; i++) {
      indexers.add(new Thread(new IndexThread(batchSize)));
    }
    log.info("Starting Indexers");
    for (Thread indexer : indexers) {
      indexer.start();
    }

    log.info("Starting Monitoring Thread");
    new Thread(new MonitorThread(collection, client)).start();
  }

  /**
   * Sends a batch of N docs. 2 long description fields.
   * Pause for 1s
   */
  private class IndexThread implements Runnable {

    private final int batchSize;

    public IndexThread(int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public void run() {
      List<SolrInputDocument> docs = new ArrayList<>(batchSize);
      while (true) {
        docs.clear();
        for (int j=0; j<batchSize; j++) {
          SolrInputDocument document = new SolrInputDocument();
          document.addField("id", idValue.incrementAndGet());
          document.addField("description_txt", createSentence(500));
          document.addField("long_description_txt", createSentence(1000));
          docs.add(document);
        }
        sendBatch(docs, 1, 10);
      }
    }

    protected int sendBatch(List<SolrInputDocument> reuse, int waitBeforeRetry, int maxRetries) {
      int sent;
      try {
        UpdateRequest updateRequest = new UpdateRequest();
        if (reuse.size() == 1) {
          updateRequest.add(reuse.get(0));
        } else {
          updateRequest.add(reuse);
        }
        client.request(updateRequest, collection);
        sent = reuse.size();
      } catch (Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        boolean wasCommError =
                (rootCause instanceof ConnectException ||
                        rootCause instanceof ConnectTimeoutException ||
                        rootCause instanceof NoHttpResponseException ||
                        rootCause instanceof SocketException);

        if (wasCommError) {
          if (--maxRetries > 0) {
            log.warn("ERROR: " + rootCause + " ... Sleeping for "
                    + waitBeforeRetry + " seconds before re-try ...");
            try {
              Thread.sleep(waitBeforeRetry * 1000L);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            sent = sendBatch(reuse, waitBeforeRetry, maxRetries);
          } else {
            throw new RuntimeException("No more retries available!", exc);
          }
        } else {
          throw new RuntimeException(exc);
        }
      }
      return sent;
    }
  }

  private static String createSentence(int numWords) {
    //Sentence with numWords and 3-7 letters in each word
    StringBuilder sb = new StringBuilder(numWords*5);
    for (int i=0; i<numWords; i++) {
      sb.append(TestUtil.randomSimpleString(r, 3, 7) + " ");
    }
    return sb.toString();
  }

}
