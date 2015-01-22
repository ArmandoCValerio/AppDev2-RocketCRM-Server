package de.rfh.rocketcrm.entity;


public class KontaktDAOFactory {

	public static KontaktDAO getKontakteDAO(int type) 
	{
		KontaktDAO conDAO = null;
		
		switch(type)
		{
			case 0: conDAO = new KontaktDAO2db(); 
				    break;
			case 1: conDAO = new KontaktDAO2file();
					break;
			default: ;
			
		}
		return conDAO;
	}
	
}