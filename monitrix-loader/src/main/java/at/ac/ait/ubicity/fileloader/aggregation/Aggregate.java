
package at.ac.ait.ubicity.fileloader.aggregation;

import java.util.concurrent.atomic.AtomicLong;


/**
 *
 * @author jan van oort
 */


public abstract class Aggregate {

    
    volatile AtomicLong value = new AtomicLong( 0 );
    
    
    
    
    
    
    public Aggregate()  {
        
    }
    
    
    
    public void accumulate( final  long _delta )    {
        value.addAndGet( _delta );
    }
    
    
    
    public long get()   {
        return value.get();
    } 
}
