
package at.ac.ait.ubicity.fileloader.aggregation;

import com.lmax.disruptor.EventHandler;
import java.util.logging.Logger;



/**
 *
 * @author jan van oort
 
 */

public final class Aggregator implements EventHandler<AggregateDelta> {

    
    protected final Aggregate A;
    
    
    public final static int TARGET_UNKNOWN = -1;
    
    public final static int TARGET_COMPLETED_HOSTS = 10;
    
    public final static int TARGET_DOWNLOADVOLUME = 10;
    
    public final static int TARGET_NUMBER_OF_NEW_HOSTS_CRAWLED = 30;
    
    public final static int TARGET_NUMBER_OF_URLS_CRAWLED = 40;
    
    
    
    public Aggregator( Aggregate _A ) {
        A = _A;
    }
    
    
    
    public final void update( final long _delta ) {
        A.accumulate( _delta );
    }
    
    
    
    public Aggregate get()  {
        return A;
    }

    @Override
    public void onEvent(AggregateDelta event, long sequence, boolean endOfBatch) throws Exception {
        A.accumulate( event.delta.get().get() );
    }
}
