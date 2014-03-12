
package at.ac.ait.ubicity.fileloader.aggregation;

import at.ac.ait.ubicity.fileloader.TokenizedLogLine;
import at.ac.ait.ubicity.fileloader.SingleLogLineAsString;
import com.lmax.disruptor.EventHandler;
import java.util.logging.Logger;



/**
 *
 * @author jan van oort
 
 */

public final class Aggregator implements EventHandler< TokenizedLogLine > {

    
    
    
    
    public Aggregator(  ) {
        
    }
    
    
    

    @Override
    public void onEvent( TokenizedLogLine event, long sequence, boolean endOfBatch) throws Exception {
        System.out.println( "-------> event in Aggregator :: "  + event.tokens[ 0 ] );
    }
}
