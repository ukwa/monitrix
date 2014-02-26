
package at.ac.ait.ubicity.fileloader.util;

import at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Rows;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author vanOortJ
 */
public final class StatsTableActualizer {

     static Logger logger = Logger.getLogger( CheckDB.class.getName() );
     
     static {
        logger.setLevel(Level.ALL);
     }
     
     
     
     
     public final static boolean update( Long start_ts, Long end_ts ) throws Exception  {
//        Rows< String, String > rows = null;
//        Keyspace keySpace = AstyanaxInitializer.doInit( "Test Cluster", "localhost", "crawl_uris" );
//        ColumnFamily< String, String > crawls = null;
//       
           throw new Exception( "Not implemented yet" );      
     }
}
