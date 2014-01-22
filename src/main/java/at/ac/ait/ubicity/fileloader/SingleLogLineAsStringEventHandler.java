package at.ac.ait.ubicity.fileloader;

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

import static at.ac.ait.ubicity.fileloader.LogLineTokenizer._SEPARATION_TOKEN;
import static at.ac.ait.ubicity.fileloader.cassandra.AstyanaxInitializer.CF_LOGLINES;
import at.ac.ait.ubicity.fileloader.cassandra.LogLineColumn;

import com.lmax.disruptor.EventHandler;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Jan van Oort
 */
final class SingleLogLineAsStringEventHandler implements EventHandler<SingleLogLineAsString> {

    
    
    static int batchSize; 
    
    
    final static Logger logger = Logger.getLogger( "EventHandler" );
    
    
    static Keyspace keySpace;
    
    static MutationBatch batch;
    

    /**
     * No-arg constructor, necessary by contract with LMAX Disruptor 
     */
    public SingleLogLineAsStringEventHandler( ) {
    }
    
    
    /**
     * 
     * We will, foreseeably, never need LogLineTokenizer  for some
     * other purpose than for having its implementation handy here; hence, we might
     * as well use a lambda expression, which saves us some maintenance pain. 
     */
    final static LogLineTokenizer _tokenizer = ( _event ) -> {
        final List< String > _list = new ArrayList();
        int pos = 0, end;
        while( ( end = _event.value.indexOf( _SEPARATION_TOKEN, pos ) ) >= 0 ) {
            final String __t = ( _event.value.substring( pos, end ) );
            if( ! __t.startsWith( _SEPARATION_TOKEN, 0 ) ) {
                _list.add( __t );
            }
            pos = end + 1;
        }            
        //the block above is incapable of detecting the last token, so let's do this here,
        //as modifying it would be kludgier than this one-liner:
        _list.add( _event.value.substring( pos, _event.value.length() ) );
        
        //moreover, we have a requirement to ingest the entire log line, too: 
        _list.add( _event.value );
        return _list;
    };        
    
    
    
    
    @Override
    public final void onEvent(final SingleLogLineAsString event, final long sequence, final boolean endOfBatch) throws Exception {
    
        /**
         * Tokenize the payload of our event, and use a database column schema 
         * to attribute the right token to the right column.
         * Take the LMAX ringbuffer sequence number as line_id, always works; 
         * also has the advantage that line_ids in the database *always* mirror the exact order of insertion; 
         * exact order of insertion = exact order of line loading into the ringbuffer = exact order of reading from the log file
         * 
         */
        
        final List< String > __tokens = _tokenizer.process( event );
        final int __n = __tokens.size();
        
        LogLineColumn _col = LogLineColumn.ID; 
        

         for ( int i = 0; i < __n; i++ )   {
            String __tok = __tokens.get( i );
            if (  ( _col = _col.next() ) != LogLineColumn.NONE )  {
                batch.withRow( CF_LOGLINES, sequence ).putColumn( _col.name, __tok );
            }
         }

        try {
            if( sequence % batchSize == 0 ) {
                batch.executeAsync().get();
                System.out.print( " " + sequence );
            }
        }
        catch( ConnectionException e )  {
            logger.log( Level.SEVERE, e.toString() );
        }
    }
}
