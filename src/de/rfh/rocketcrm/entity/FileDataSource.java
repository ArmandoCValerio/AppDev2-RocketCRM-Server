package de.rfh.rocketcrm.entity;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.util.Enumeration;
import java.util.Hashtable;

import de.rfh.rocketcrm.control.Kontakt;
import de.rfh.rocketcrm.entity.DataSource;

public class FileDataSource implements DataSource
{
	public static Hashtable<Long, Kontakt> hash = new Hashtable<Long, Kontakt>();
	
	public File dbFile = new File("C:\\git\\AppDev2-RocketCRM-Server\\src\\dbFolder\\dbFile.csv");
	private Connection myConnection;
	
	@Override
	public Connection getConnection() throws Exception 
	{
			FileReader fr = new FileReader(dbFile);
			BufferedReader br = new BufferedReader(fr);	
			
			System.out.println("Hashtable füllen!");	
			
			String dbRec = null;
			while ((dbRec = br.readLine()) != null) 
			{
				Kontakt objK = new Kontakt();
				
				String strId = dbRec.split(";")[0];
				long lId = Long.parseLong(strId);
				objK.setcId(lId);
				objK.setcCrtDate(dbRec.split(";")[1]);
				objK.setcCrtUser(dbRec.split(";")[2]);
				objK.setcUpdtDate(dbRec.split(";")[3]);
				objK.setcUpdtUser(dbRec.split(";")[4]);
				objK.setcVName(dbRec.split(";")[5]);
				objK.setcNName(dbRec.split(";")[6]);
				objK.setcCompany(dbRec.split(";")[7]);
				objK.setcCity(dbRec.split(";")[8]);
				objK.setcBirthDay(dbRec.split(";")[9]);
				objK.setcMail(dbRec.split(";")[10]);
				objK.setcPhone(dbRec.split(";")[11]);
				
				String strVersion = dbRec.split(";")[12];
				long lVersion = Long.parseLong(strVersion);
				objK.setcVersion(lVersion);
					
				hash.put(objK.getcId(),objK);
				System.out.println("Hashtable Eintrag:" + objK.getcId());
				
			}
			br.close();
		
		return myConnection;
	}
	
	@Override
	public void doDisConnect()  throws Exception
	{
		// TODO Auto-generated method stub
		storeHashTable(hash);
	}

	private void storeHashTable(Hashtable<Long, Kontakt> hash) throws Exception
	{	
		FileWriter fw = new FileWriter(dbFile);
		BufferedWriter bw = new BufferedWriter(fw);	
		
		Enumeration<Long> e = hash.keys();
		
	    while (e.hasMoreElements()) 
	    {
	       Long hashId = e.nextElement();
	       System.out.println(hashId + " --> " + hash.get(hashId));
	       
	       Kontakt objK = new Kontakt();
	       
	       objK = hash.get(hashId);
	       if  (objK != null)
	       {
	    	   String str = Long.toString(objK.getcId()) + ";" + 
	    		        objK.getcCrtDate() + ";" + 
	       				objK.getcCrtUser() + ";" + 
	       				objK.getcUpdtDate() + ";" + 
	       				objK.getcUpdtUser() + ";" +
	       				objK.getcNName() + ";" +
	       				objK.getcVName() + ";" + 
	       				objK.getcCompany() + ";" +
	       				objK.getcCity() + ";" +
	       				objK.getcBirthDay() + ";" +
	       				objK.getcMail() + ";" +
	       				objK.getcPhone() + ";" +
	       				objK.getcVersion() + ";" + "\r\n";
	       				
	    	   bw.write(str);
	       }
	    }
		bw.close();
	}	
}
