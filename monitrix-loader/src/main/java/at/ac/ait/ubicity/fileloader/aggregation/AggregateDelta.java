

package at.ac.ait.ubicity.fileloader.aggregation;

import com.lmax.disruptor.EventFactory;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author jan van oort
 */
public final class AggregateDelta {

    
    
    public int target = Aggregator.TARGET_UNKNOWN ;
    
    
    public WeakReference< AtomicLong > delta;
    
    
    
    public void setDelta( int _target, long _delta )  {
        delta = new WeakReference( new AtomicLong( _delta ) );
    }

    
    private AggregateDelta() {}
    
    
    static EventFactory EVENT_FACTORY = new EventFactory() {

        public Object newInstance() {
            return new AggregateDelta();
        }
    };
    
}
