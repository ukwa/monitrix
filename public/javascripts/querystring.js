window.QueryString = {
  
  get: function(name) {
    name = name.replace(/[\[]/, "\\\[").replace(/[\]]/, "\\\]");
	var regexS = "[\\?&]" + name + "=([^&#]*)";
	var regex = new RegExp(regexS);
	var results = regex.exec(window.location.search);
	if(results == null)
	  return undefined;
	else
	  return decodeURIComponent(results[1].replace(/\+/g, " "));
  }

}

