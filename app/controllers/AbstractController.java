package controllers;

import play.mvc.Controller;

public class AbstractController extends Controller {
	
	protected static String getStringParam(String name) {
		String[] value = request().queryString().get(name);
		if (value == null)
			return null;
		
		if (value.length == 0)
			return null;
		
		return value[0];
	}
	
	protected static int getIntParam(String name, int defaultValue) {
		String[] param = request().queryString().get(name);
		if (param == null)
			return defaultValue;
		
		if (param.length < 1)
			return defaultValue;
		
		try {
			return Integer.parseInt(param[0]);
		} catch (Throwable t) {
			return defaultValue;
		}
	}
	
}
