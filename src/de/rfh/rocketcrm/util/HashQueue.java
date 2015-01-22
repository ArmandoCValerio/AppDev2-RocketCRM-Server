package de.rfh.rocketcrm.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Enumeration;
import java.util.Hashtable;

import de.rfh.rocketcrm.control.Kontakt;

public class HashQueue extends Thread
{
	static long queueEntry = 0;
	
	@SuppressWarnings("rawtypes")
	static Hashtable<Long, Hashtable> hashQueue = new Hashtable<Long, Hashtable>();
	
	@SuppressWarnings("unchecked")
	public void run()
	{
		Hashtable<Long, Kontakt> hashKontakt = new Hashtable<Long, Kontakt>();
		
		boolean retC = true;
			
		while (retC == true)
		{
			try 
			{
				Enumeration<Long> e = hashQueue.keys();
				while (e.hasMoreElements())
				{
					Long hashId = e.nextElement();
					hashKontakt = hashQueue.get(hashId);
					System.out.println(hashId + " --> " + hashQueue.get(hashId));
					storeHashTable(hashKontakt);
					hashQueue.remove(hashId);
					hashKontakt = new Hashtable<Long, Kontakt>();
				}
			} catch (Exception e) 
			{
				// TODO Auto-generated catch block
				retC = false;
				e.printStackTrace();
			}
		}
	}
	
	private void storeHashTable(Hashtable<Long, Kontakt> hash) throws Exception
	{
		File file = new File("C:\\git\\AppDev2-RocketCRM-Server\\src\\dbFolder\\dbFile.csv");
		
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);	
		
		Enumeration<Long> e = hash.keys();
		
	    while (e.hasMoreElements()) 
	    {
	       Long hashId = e.nextElement(); 
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
	
	public static void hash2Queue(Hashtable<Long, Kontakt> hash)
	{
		queueEntry++;
		hashQueue.put(queueEntry,hash);
	}
}
