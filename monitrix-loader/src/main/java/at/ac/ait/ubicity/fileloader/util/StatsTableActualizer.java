
package at.ac.ait.ubicity.fileloader.util;

import at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.util.RangeBuilder;
import java.util.Iterator;
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
     
     
     
     
     public final static boolean update( String _key, Long start_ts, Long end_ts ) throws Exception  {
        Rows< String, Long > rows = null;
        ColumnList<  Long > cl = null;
        Keyspace keySpace = AstyanaxInitializer.doInit( "Test Cluster", "localhost", "crawl_uris" );
        ColumnFamily< String, Long > crawls = AstyanaxInitializer.crawls;
//       
        
try {
            

            cl = keySpace.prepareQuery( crawls ).getKey( "_key").execute().getResult();
                    
        }
        catch( BadRequestException bre )    {
            logger.log( Level.INFO,  "column space " + crawls.getName() + " exists, everything OK, proceeding... " ) ;
            return false;
        }    
        catch( ConnectionException noCassandra )    {
            logger.log( Level.SEVERE, noCassandra.toString() );
            noCassandra.printStackTrace();
            return false;
        }
        System.out.println( "!!!! cols == null ? " + ( cl == null ) );
        Iterator<Column< Long>> onCols = cl.iterator();
        int i = 0;
        System.out.println( "------------------------------------ > executed ** crawls ** query ::: " );
        while( onCols.hasNext() )   {        
            System.out.println( "-------------> col:: " + onCols.next().getStringValue() );
        }
        return true;
     }
}
