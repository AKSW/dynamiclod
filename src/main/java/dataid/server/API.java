package dataid.server;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import dataid.mongodb.actions.MakeLinksets;

public class API extends HttpServlet{
	
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		manageRequest(request, response);
	}
	
	
	private void manageRequest(HttpServletRequest request, HttpServletResponse response){
		
		if (request.getParameter("makeLinksets")!=null){
			if(request.getParameter("makeLinksets").equals("true")){
				MakeLinksets m = new MakeLinksets();
				m.updateLinksets();
			}
		}
		
	}

}
