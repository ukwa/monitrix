
package at.ac.ait.ubicity.fileloader.cassandra;


import static at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer.logger;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import java.util.logging.Level;

/**
    Copyright (C) 2013  AIT / Austrian Institute of Technology
    http://www.ait.ac.at

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see http://www.gnu.org/licenses/agpl-3.0.html
 */
/**
 *
 * @author Jan van Oort
 * 
 * Helper class to populate the desired keyspace with the correct row family 
 * ( or "table" ). 
 * 
 * 
 */
public final class CassandraCrawlLogSchema {
    
    
    public final static ColumnFamily< String, String > checkOrBuildMonitrixSchema( final Keyspace _keySpace)   {
        ColumnFamily< String, String > cf = null;
        
            try {
               cf = ColumnFamily.newColumnFamily(
               "log",              // Column Family Name
               StringSerializer.get(),   // Key Serializer
               StringSerializer.get());  // Column Serializer

               _keySpace.createColumnFamily( cf, ImmutableMap.<String, Object>builder()
                   .put("column_metadata", ImmutableMap.<String, Object>builder()
                           .put("Index1", ImmutableMap.<String, Object>builder()
                                   .put("validation_class", "UTF8Type")
                                   .put("index_name",       "Index1")
                                   .put("index_type",       "KEYS")
                                   .build())
                           .put("Index2", ImmutableMap.<String, Object>builder()
                                   .put("validation_class", "UTF8Type")
                                   .put("index_name",       "Index2")
                                   .put("index_type",       "KEYS")
                                   .build())
                            .build())
                        .build());
        }
        catch( BadRequestException bre )    {
            logger.log( Level.INFO,  "column space " + cf.getName() + " exists, everything OK, proceeding... " ) ;
        }    
        catch( ConnectionException noCassandra )    {
            logger.log( Level.SEVERE, noCassandra.toString() );
        }
        return cf;
    }

    static ColumnFamily<String, String> checkOrBuildCrawlsTable(Keyspace keySpace) {
        ColumnFamily< String, String  > cf = null;
        
            try {
               cf = ColumnFamily.newColumnFamily(
               "crawls",              // Column Family Name
               StringSerializer.get(),   // Key Serializer
               StringSerializer.get());  // Column Serializer

               keySpace.createColumnFamily( cf, ImmutableMap.<String, Object>builder()
                   .put("column_metadata", ImmutableMap.<String, Object>builder()
                           .put("Index1", ImmutableMap.<String, Object>builder()
                                   .put("validation_class", "UTF8Type")
                                   .put("index_name",       "Index1")
                                   .put("index_type",       "KEYS")
                                   .build())
                           .put("Index2", ImmutableMap.<String, Object>builder()
                                   .put("validation_class", "UTF8Type")
                                   .put("index_name",       "Index2")
                                   .put("index_type",       "KEYS")
                                   .build())
                            .build())
                        .build());
        }
        catch( BadRequestException bre )    {
            logger.log( Level.INFO,  "column family " + cf.getName() + " exists, everything OK, proceeding... " ) ;
        }    
        catch( ConnectionException noCassandra )    {
            logger.log( Level.SEVERE, noCassandra.toString() );
        }
        return cf;        
    }
}
