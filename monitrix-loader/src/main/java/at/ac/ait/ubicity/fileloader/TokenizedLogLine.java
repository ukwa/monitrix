
package at.ac.ait.ubicity.fileloader;

import com.lmax.disruptor.EventFactory;

/**
 *
 * @author vanoortj
 */
public class TokenizedLogLine {
    
    
    public String[] tokens;
    
    
    public TokenizedLogLine()   {
        
    }
    
    
    
    public final static EventFactory< TokenizedLogLine > EVENT_FACTORY = new EventFactory<TokenizedLogLine>() {

        @Override
        public TokenizedLogLine newInstance() {
            return new TokenizedLogLine();
        }
    };
}
