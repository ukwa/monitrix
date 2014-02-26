
package at.ac.ait.ubicity.fileloader.aggregation;


import static at.ac.ait.ubicity.fileloader.FileLoader.TWO;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jan  van oort
 */
public final class AggregationJob implements Runnable    {
    
    
    
    private final static Logger logger = Logger.getLogger( AggregationJob.class.getName() );
    
    
    private final String host;
    private final int batchSize;
    private final Keyspace keySpace;
    
    
    public AggregationJob( final Keyspace _keySpace, final String _host, final int _batchSize )  {
        keySpace = _keySpace;
        host = _host;
        batchSize = _batchSize;
    }
    
    
    
    public final void doRun( final Keyspace keySpace, final String _host, final int _batchSize ) throws Exception {
        
    
        final MutationBatch batch = keySpace.prepareMutationBatch();
        
        logger.setLevel( Level.ALL );
        
        
        final ExecutorService exec = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() * 2 );        
        
        final Disruptor< AggregateDelta > downloadVoldisruptor = new Disruptor( AggregateDelta.EVENT_FACTORY, ( int ) Math.pow( TWO, 10 ), exec );
        final EventHandler< AggregateDelta > downloadVolumehandler = new Aggregator( new DownloadVolume() );
        downloadVoldisruptor.handleEventsWith( downloadVolumehandler );
        
        final Disruptor< AggregateDelta > completedHostsdisruptor = new Disruptor( AggregateDelta.EVENT_FACTORY, ( int ) Math.pow( TWO, 10 ), exec );
        final EventHandler< AggregateDelta > completedHostsHandler = new Aggregator( new CompletedHosts() );
        completedHostsdisruptor.handleEventsWith( completedHostsHandler );
        
        final Disruptor< AggregateDelta > numNewHostsCrawleddisruptor = new Disruptor( AggregateDelta.EVENT_FACTORY, ( int ) Math.pow( TWO, 10 ), exec );
        final EventHandler< AggregateDelta > numberOfNewHostsCrawledHandler = new Aggregator( new NumberOfNewHostsCrawled() );
        numNewHostsCrawleddisruptor.handleEventsWith( numberOfNewHostsCrawledHandler );
        
        final Disruptor< AggregateDelta > numURLsCrawleddisruptor = new Disruptor( AggregateDelta.EVENT_FACTORY, ( int ) Math.pow( TWO, 10 ), exec );
        final EventHandler< AggregateDelta > numberOfURLsCrawledHandler = new Aggregator( new NumberOfURLsCrawled() );
        numURLsCrawleddisruptor.handleEventsWith( numberOfURLsCrawledHandler );
        
        
        RingBuffer downloadVolRB = downloadVoldisruptor.start();
        RingBuffer completedHostsRB = completedHostsdisruptor.start();
        RingBuffer numNewHostsRB = numNewHostsCrawleddisruptor.start();
        RingBuffer numURLsCrawledRB = numURLsCrawleddisruptor.start();
        
        logger.info( "[AGGREGATOR] started RingBuffers, beginning aggregate job" );
        
        
        int _lineCount = 0;
        long _start, _lapse;
        _start = System.nanoTime();
        
    
    }
    
    
    
    
    public final static void main( String... args ) {
        
    }

    
    
    
    
    @Override
    public void run() {
        try {
            doRun( keySpace, host, batchSize );
        }
        catch( Exception e )    {
            logger.severe( "[AGGREGATOR] encountered a problem while aggregating : " + e.toString() );
        }
    }
}
