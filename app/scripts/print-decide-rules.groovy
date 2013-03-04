//Groovy
def printProps(obj){
  // getProperties is a groovy introspective shortcut. it returns a map
  obj.properties.each{ prop ->
    // prop is a Map.Entry
    rawOut.println "\n"+ prop
    try{ // some things don't like you to get their class. ignore those.
      rawOut.println "TYPE: "+ prop.value.class.name
    }catch(Exception e){}
  }
}
  
// loop through the rules
counter = 0
appCtx.getBean("scope").rules.each { rule ->
  rawOut.println("\n###############${counter++}\n")
  printProps( rule )
}
