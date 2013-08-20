// Parameters
sheetName = "noForms"
uriStr = "http://www.greenagesolutions.co.uk"

// Get the sheet manager:
mgr = appCtx.getBean("sheetOverlaysManager")
 
// Test that you got the name right
if ( ! mgr.sheetsByName.containsKey( sheetName ) ) {
 rawOut.println( "sheet $sheetName does not exist. your choices are:" )
 mgr.sheetsByName.keySet().each{ rawOut.println(it) }
 return;
}

seedSurt = org.archive.util.SurtPrefixSet.prefixFromPlainForceHttp("http://"+ new org.apache.commons.httpclient.URI(uriStr).host).replaceAll( /www,$/, "" )

rawOut.println("associating $seedSurt")
try{
  mgr.addSurtAssociation( seedSurt, sheetName)
 } catch (Exception e) {
  println("caught $e on $seedSurt")
 }

//review the change
rawOut.println("SURTs associated with "+sheetName)
mgr.sheetNamesBySurt.each{ k, v -> 
    if( v.contains(sheetName) ) rawOut.println("$k") 
}
