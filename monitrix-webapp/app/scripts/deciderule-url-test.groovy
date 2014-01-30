//uri = "http://static.guim.co.uk/sys-images/Guardian/Pix/pictures/2013/4/11/1365681260808/Egyptian-women-protest-008.jpg"
//pathFromSeed = "-"

import org.archive.net.UURI;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.CrawlURI;

def printProps(obj){
  ptot =  obj.properties.size()
  pi = 0
  // getProperties is a groovy introspective shortcut. it returns a map
  obj.properties.each{ prop ->
    // prop is a Map.Entry
    rawOut.print(" \""+prop.key+"\": \""+prop.value.toString().replace("\"","\\\"")+"\" ")
    pi++
    if( pi < ptot ) {
      rawOut.print(",")
    }
  }
}

// Construct the CrawlURI, taking care to cope with API changes in recent H3 updates:
try {
  uuri = new UURI(uri,false);
} catch( groovy.lang.GroovyRuntimeException e ) {
  uuri = new UURI(uri,false,"UTF-8");
}
lc = LinkContext.NAVLINK_MISC;
curi = new CrawlURI( uuri, pathFromSeed, null, lc);


// loop through the rules
counter = 0
rawOut.println("{")
rules = appCtx.getBean("scope").rules
rules.each { rule ->
  rawOut.print("\""+rule.class.name + "\":{ \"decision\": \""+rule.decisionFor(curi)+"\", ")
  printProps(rule)
  if( rule == rules.last()) {
    rawOut.println("}")
  } else{
    rawOut.println("},")
  }
}
rawOut.println("}")
