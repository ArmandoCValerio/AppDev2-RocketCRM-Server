package de.rfh.rocketcrm.entity;

public class ErrorHandle {

	private String errorMessage;
	
	public String getErrorMessage(int i) {
		
		switch(i){
        case 0:
            errorMessage = "Kein Fehler - 0";
            break;
        case 1:
        	errorMessage = "Allgemeiner Fehler - 1";
            break;
        
        // Weitere Fehlerfälle definieren
        
        case 3:
        	errorMessage = "Fehler - 3 - RecordNotFound.";
        	break;
            
        case 99:
        	errorMessage = "Fehler - 99 - SQL Fehler.";
        	break;
        	
        case 100:
        	errorMessage = "Fehler - 100 - SQL Connect Fehler.";
        	break;
        	
        // Weitere Fehlerfälle definieren
        	
        default:
        	errorMessage = "Allgemeiner Fehler - X";
        }		
		
		return errorMessage;		
		
	}
	
}
