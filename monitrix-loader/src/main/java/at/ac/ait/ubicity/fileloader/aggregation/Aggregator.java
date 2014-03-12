
package at.ac.ait.ubicity.fileloader.aggregation;

import at.ac.ait.ubicity.fileloader.TokenizedLogLine;

import com.lmax.disruptor.EventHandler;
import com.netflix.astyanax.MutationBatch;




/**
 *
 * @author jan van oort
 
 */

public final class Aggregator implements EventHandler< TokenizedLogLine > {

    
    static MutationBatch mutationBatch;
    
    static CrawlStatsEntry currentEntry;
    
    private static boolean firstTimeStampSynced = false;
    
    public final static long FIVE_MINUTES = 300000;
    
    static  {
        
    }
    
    
    public Aggregator(  ) {
        
    }
    
    
    

    @Override
    public void onEvent( TokenizedLogLine event, long sequence, boolean endOfBatch) throws Exception {
        long __ts = Long.parseLong( event.tokens[ 14 ] ) ;
        long _fiveMinsBlockBegin = ( __ts / FIVE_MINUTES ) * FIVE_MINUTES ;
        System.out.println( " ------- > > > " +  _fiveMinsBlockBegin );
    }
}
