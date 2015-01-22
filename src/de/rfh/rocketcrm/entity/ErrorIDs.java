package de.rfh.rocketcrm.entity;

public final class ErrorIDs {
	
	public static int cOK = 0;
	public static int cNotOK = 1;
	
	public static int  cErrRecordChanged    = 2;
	public static int  cErrRecordNotFound	= 3;
	public static int  cErrWrongData		= 4;
	public static int  cErrTableEmpty		= 5;
	
	public static int  cErrWrongParameter	= 10;
	public static int  cErrPageNotFound 	= 11;
	public static int  cErrInvalidRequest	= 12;
	public static int  cErrServerError		= 13;
	public static int  cErrUndefClass		= 14;		
	
	//Datenbank und SQL Fehler
	public static int  cErrSQL    			= 99;
	public static int  cErrDBConnect    	= 100;
	public static int  cErrDBClose          = 101;
	public static int  cErrDBCharset    	= 102;
	public static int  cErrDBQuery      	= 103;
	public static int  cErrDBFetch      	= 104;
	public static int  cErrDBBFetchRow     	= 105;

	// Fehler in Input
	public static int cErrInputNName		= 200;
	public static int cErrInputVName		= 201;
	public static int cErrInputCompany		= 202;
		
}