uri = "http://static.guim.co.uk/sys-images/Guardian/Pix/pictures/2013/4/11/1365681260808/Egyptian-women-protest-008.jpg"
pathFromSeed = "LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL"

import org.archive.net.UURI;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.CrawlURI;

def printProps(obj){
  // getProperties is a groovy introspective shortcut. it returns a map
  obj.properties.each{ prop ->
    // prop is a Map.Entry
    rawOut.println("{ "+prop.key+": '"+prop.value.toString().replace("'","\\'")+"' },")
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
appCtx.getBean("scope").rules.each { rule ->
  rawOut.println(rule.class.name + ":{ { decision: '"+rule.decisionFor(curi)+"' }, ")
  printProps(rule)
  rawOut.println("},")
}
rawOut.println("}")
