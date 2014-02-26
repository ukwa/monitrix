
package at.ac.ait.ubicity.fileloader.util;

import at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jan  van oort
 */
public class CleanUp {
   
    
    
    public final static void main( String[] args ) throws Exception  {
        Logger logger = Logger.getLogger( CheckDB.class.getName() );
        logger.setLevel(Level.ALL);

        Rows< Long, String > rows = null;
        Keyspace keySpace = AstyanaxInitializer.doInit( "Test Cluster", "localhost", "crawl_uris" );
        ColumnFamily< String, String > cf = null;
        
        
        try {
            
               cf = AstyanaxInitializer.log;
               keySpace.truncateColumnFamily( cf );
        }
        catch( Throwable t )    {
            t.printStackTrace();
        }
        
    }
}
