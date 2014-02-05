
package at.ac.ait.ubicity.fileloader.util;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jan van Oort
 */
public enum Delay {
    
    
    SECOND( 1000), MINUTE( 60000), HOUR( 3600000), DAY( 86400000);
    
    public final static List< Delay > knownOptions; 
    
    static  {
        knownOptions = new ArrayList();
        knownOptions.add( SECOND );
        knownOptions.add( MINUTE );
        knownOptions.add( HOUR );
        knownOptions.add( DAY );
    }
    
    
    private long milliSecondsDelay;
    
    
    
    private Delay( long _milliSeconds )  {
        milliSecondsDelay = _milliSeconds;
    }
    
    
    public final long getMilliSeconds() {
        return milliSecondsDelay;
    }
}
