
package at.ac.ait.ubicity.fileloader.aggregation;


import static at.ac.ait.ubicity.fileloader.FileLoader.TWO;
import at.ac.ait.ubicity.fileloader.SingleLogLineAsString;
import at.ac.ait.ubicity.fileloader.TokenizedLogLine;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;

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
    

    
    private ColumnFamily crawl_stats;
    
    private Keyspace keySpace;
    
    private static RingBuffer< TokenizedLogLine > rb; 
    
    public AggregationJob( final Keyspace _keySpace, final ColumnFamily _crawl_stats )  {
        keySpace = _keySpace;
        crawl_stats = _crawl_stats;
    }
    
    
    
    public final void doRun( final Keyspace keySpace ) throws Exception {
        
    
        final MutationBatch batch = keySpace.prepareMutationBatch();
        
        logger.setLevel( Level.ALL );
        
        
        final ExecutorService exec = Executors.newFixedThreadPool( Runtime.getRuntime().availableProcessors() * 2 );        
        
        final Disruptor< TokenizedLogLine > disruptor = new Disruptor( TokenizedLogLine.EVENT_FACTORY, ( int ) Math.pow( TWO, 10 ), exec );
        final EventHandler< TokenizedLogLine > handler = new Aggregator();
        disruptor.handleEventsWith( handler );
        
        rb = disruptor.start();
        
        logger.info( "[AGGREGATOR] started RingBuffers, beginning aggregate job" );
        
        
        int _lineCount = 0;
        long _start, _lapse;
        _start = System.nanoTime();
        
    
    }
    
    
    public final void offer( String[] _tokenizedLogLine )   {
        long _sequence  = rb.next();
        TokenizedLogLine _tokenizedLine = rb.get( _sequence );
        _tokenizedLine.tokens = _tokenizedLogLine;   
        rb.publish( _sequence );
    }
    
    
    
    @Override
    public void run() {
        try {
            doRun( keySpace );
        }
        catch( Exception e )    {
            logger.severe( "[AGGREGATOR] encountered a problem while aggregating : " + e.toString() );
        }
    }
}
