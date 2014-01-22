

package at.ac.ait.ubicity.fileloader.cassandra;
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
import com.google.common.collect.ImmutableMap;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author jan van oort
 */
public final class AstyanaxInitializer {


    public static ColumnFamily< Long, String > CF_LOGLINES;
    
    final static Logger logger = Logger.getLogger( "AstyanaxInitializer" );
    
    /**
     * 
     * @param _clusterName
     * @param _server
     * @param _keySpaceName
     * @return KeySpace - the Cassandra KeySpace we are going to work in, and that we give back to the caller
     * ( if everthing goes well ). 
     * @throws java.lang.Exception 
     */
    public final static Keyspace doInit( final String _clusterName, final String _server, final String _keySpaceName ) throws Exception   {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
        .forCluster( _clusterName )
        .forKeyspace( _keySpaceName )
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE )
        .setCqlVersion("2.0.0")
        .setTargetCassandraVersion("2.0.4")
        
    )
    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl( "ubicity file loading pool")
        .setPort( 9160 )
        .setMaxConnsPerHost( 8 )
        .setSeeds( _server + ":9160" )
    )
    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
    .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();

    Keyspace keySpace = context.getClient();
    try {
        keySpace.describeKeyspace();
        logger.log( Level.INFO, "keyspace " + _keySpaceName + " does exist" );
    }
    catch( BadRequestException bre )    {
        logger.log( Level.INFO, "keyspace " + _keySpaceName + " does NOT exist, creating it" );
        keySpace.createKeyspace( ImmutableMap.<String, Object>builder()
        .put("strategy_options", ImmutableMap.<String, Object>builder()
        .put("replication_factor", "1")
        .build())
        .put("strategy_class",     "SimpleStrategy")
        .build()
        );    
    }

    
    try {
    CF_LOGLINES =
    ColumnFamily.newColumnFamily(
    "CF_LOGLINES",              // Column Family Name
    LongSerializer.get(),   // Key Serializer
    StringSerializer.get());  // Column Serializer

    keySpace.createColumnFamily( CF_LOGLINES, ImmutableMap.<String, Object>builder()
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
        logger.log( Level.INFO,  "column space " + CF_LOGLINES.getName() + " exists, everything OK, proceeding... " ) ;
    }
    
    return keySpace;
    }
}
