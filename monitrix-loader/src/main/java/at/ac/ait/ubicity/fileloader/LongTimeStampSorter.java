
package at.ac.ait.ubicity.fileloader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author vanOortJ
 */
public final class LongTimeStampSorter implements Runnable {

    
    
    final Set< Long > timeStamps = Collections.newSetFromMap( new ConcurrentHashMap() );
    
    @Override
    public void run() {
        
        while( true )    {
            try {
                Thread.sleep(0 , 1000 );
            }
            catch( InterruptedException _interrupt )    {
                Thread.interrupted();
            }
        }
    }
    
}
