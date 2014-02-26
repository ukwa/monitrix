
package at.ac.ait.ubicity.fileloader.util;

import at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
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
     
    static Keyspace keySpace;
    static ColumnFamily< String, String > crawls = AstyanaxInitializer.crawls;
     
     static {
        logger.setLevel(Level.ALL);
        try {
        keySpace = AstyanaxInitializer.doInit( "Test Cluster", "localhost", "crawl_uris" );        
        }
        catch( Exception e )    {
            logger.severe( e.toString() );
        }
     }
     
     
     
     
     public final static boolean update( String _key, Long start_ts, Long end_ts ) throws Exception  {

        ColumnList<  String > cl = null;
        
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
        Iterator<Column< String>> onCols = cl.iterator();
        int i = 0;
        System.out.println( "------------------------------------ > executed ** crawls ** query ::: " );
        while( onCols.hasNext() )   {        
            System.out.println( "-------------> col:: " + onCols.next().getStringValue() );
        }
        //simple case: simply write into the table what we have, and leave
        doUpdate( _key, start_ts, end_ts );
        return true;
     }

    private static void doUpdate(String _key, Long start_ts, Long end_ts) throws Exception {
        MutationBatch mb = keySpace.prepareMutationBatch();
        mb.withRow( crawls, _key ).putColumn( "start_ts", Long.toString( start_ts) ).putColumn( "end_ts", Long.toString( end_ts ) );
        mb.execute();
    }
}
