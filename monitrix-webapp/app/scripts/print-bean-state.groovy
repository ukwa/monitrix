// print out available variables from the scripting context
this.binding.getVariables().each{ rawOut.println("${it.key}=\n ${it.value}\n") }

//Groovy
appCtxData = appCtx.getData()
appCtxData.printProps = { rawOut, obj ->
  rawOut.println "#properties"
  // getProperties is a groovy introspective shortcut. it returns a map
  obj.properties.each{ prop ->
    // prop is a Map.Entry
    rawOut.println "\n"+ prop
    try{ // some things don't like you to get their class. ignore those.
      rawOut.println "TYPE: "+ prop.value.class.name
    }catch(Exception e){}
  }
  rawOut.println "\n\n#methods"
  try {
  obj.class.methods.each{ method ->
    rawOut.println "\n${method.name} ${method.parameterTypes}: ${method.returnType}"
  } }catch(Exception e){}
}
 
// above this line need not be included in later script console sessions
def printProps(x) { appCtx.getData().printProps(rawOut, x) }
 
// example: see what can be accessed on the frontier
printProps(job.crawlController.frontier)
