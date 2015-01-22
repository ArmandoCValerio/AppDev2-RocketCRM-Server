package de.rfh.rocketcrm.control;

import java.util.ArrayList;
import java.util.List;

import de.rfh.rocketcrm.boundary.KontaktService;
import de.rfh.rocketcrm.entity.ErrorHandle;
import de.rfh.rocketcrm.entity.KontaktDAO;
import de.rfh.rocketcrm.entity.KontaktDAOFactory;

import java.rmi.server.UnicastRemoteObject;

public class KontaktServiceImplementation extends UnicastRemoteObject implements KontaktService {

	private static final long serialVersionUID = 1L;

	ErrorHandle oErrorHandler = new ErrorHandle();
	
	public KontaktDAO dao;	
	
	public KontaktServiceImplementation() throws Exception 
	{
		this.dao = KontaktDAOFactory.getKontakteDAO(1);   // <---  hier H2 (0) oder File (1)
	}
	
	public List<Kontakt> getKontakte() throws Exception {
		
		List<Kontakt> kontaktarray = new ArrayList<Kontakt>();
		
		try {
			kontaktarray = dao.getKontakte();
			
		} 
		catch (Exception e) {				
			// Get Error Message based on Type of Exception/Error
			String errorMessage = exceptionHandler(e);
			// Error Output should happen in Client, not Server -> Message back to Client	
    	    throw new Exception(errorMessage);
		}
		
		return kontaktarray;
	}

	public Kontakt getKontakt(Kontakt k) throws Exception {
		
		try {
		  k = dao.getKontakt(k);
		  		  
		}
		catch (Exception e) {				
			// Get Error Message based on Type of Exception/Error
			String errorMessage = exceptionHandler(e);
			// Error Output should happen in Client, not Server -> Message back to Client	
    	    throw new Exception(errorMessage);
		}
					
		return k;
	}

	public Kontakt createKontakt(Kontakt k) throws Exception {
		// TODO Auto-generated method stub
		k = dao.createKontakt(k);
		
		return k;
	}

	public Kontakt deleteKontakt(Kontakt k) throws Exception {
		// TODO Auto-generated method stub
		k = dao.deleteKontakt(k);
		
		return k;
	}
	
	
	public Kontakt editKontakt(Kontakt k) throws Exception {
		// TODO Auto-generated method stub
		k = dao.editKontakt(k);
		
		return k;
	}
	
	// exceptionHandler: Decides which errorMessage is created, based on the exception Parameter,
	// EITHER: handled Runtime Exception (by errorID), OR: handled Program Error (by errorID), OR generic Exception
	private String exceptionHandler(Exception e) {
		
		String errorMessage = "";
		
		String exceptionString = e.toString();
				
		// Handle known and specific Runtime Exceptions
		if (exceptionString.contains("ConnectException")) {
			errorMessage = oErrorHandler.getErrorMessage(100);
			
		} else if (exceptionString.contains("SQLException")) {	
			errorMessage = oErrorHandler.getErrorMessage(99);
			 
		} else { // Our handled Error's or Generic Runtime-Exceptions: (i.e.: java.lang.Exception: 3)			
		
			// If Integer was set as Return-Value (errorID): Use our Handled Error-Messages
		    try { 
		        int errorID = Integer.parseInt(e.getMessage()); // TRY to parse Return-Value as INTEGER 
		        errorMessage = oErrorHandler.getErrorMessage(errorID);  
		        
		    // If NO Return Integer was set, its not one of our handled Errors: Generic Runtime-Exceptions:
		    } catch(NumberFormatException eNumb) {		    	
		    	errorMessage = oErrorHandler.getErrorMessage(1);	    	
		    }
		
		}
		
		return errorMessage;	
	}
	public String sayHello(String msg) throws Exception 
	{
		System.out.println("Server_Message_Implementation: " + msg);
		msg = dao.sayHello(msg);
		return msg;
	} 
}
	

