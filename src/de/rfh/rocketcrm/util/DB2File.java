package de.rfh.rocketcrm.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import de.rfh.rocketcrm.control.Kontakt;
import de.rfh.rocketcrm.entity.DataSource;
import de.rfh.rocketcrm.entity.H2DataSource;

public class DB2File 
{
	private static DataSource myDataSource = new H2DataSource();
	
	public static void main(String[] args)
	{
		Connection myConnection = null;
		
		try {
			
			String dbURL = "jdbc:h2:tcp://localhost/C:/git/AppDev2-RocketCRM-Server/src/dbFolder/RocketCRM";	
			String driver = "org.h2.Driver";
			String user = "sa";
			String pwd = "";

			Class.forName(driver);
			myConnection = DriverManager.getConnection(dbURL, user, pwd);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if  (myConnection != null) 
		{
			Hashtable<Long, Kontakt> hash = new Hashtable<Long, Kontakt>();
			
			getKontakt2Hash(hash);
			
			if  (hash.size() > 0)
			{
				storeHashTable(hash);	
			}
		}
	}
	
	public static void getKontakt2Hash(Hashtable<Long, Kontakt> hash )  
	{
		try {
			Connection myConnection = myDataSource.getConnection();
			if  (myConnection != null) 
			{
				String sql = "SELECT * FROM Kontakt";

				PreparedStatement myStatement = myConnection.prepareStatement(sql);
				ResultSet myResultSet = myStatement.executeQuery();				

				while (myResultSet.next()) 
				{	
					Kontakt objK = new Kontakt();
					objK.setcId(myResultSet.getLong("CID"));
					objK.setcCrtDate(myResultSet.getString("CCRTDATE"));
					objK.setcCrtUser(myResultSet.getString("CCRTUSER"));
					objK.setcUpdtDate(myResultSet.getString("CUPDTDATE"));
					objK.setcUpdtUser(myResultSet.getString("CUPDTUSER"));
					objK.setcVName(myResultSet.getString("CVNAME"));
					objK.setcNName(myResultSet.getString("CNNAME"));
					objK.setcCompany(myResultSet.getString("CCOMPANY"));
					objK.setcCity(myResultSet.getString("CCITY"));
					objK.setcBirthDay(myResultSet.getString("CBIRTHDAY"));
					objK.setcMail(myResultSet.getString("CMAIL"));
					objK.setcPhone(myResultSet.getString("CPHONE"));
					objK.setcVersion(myResultSet.getLong("CVERSION"));
					
					hash.put(objK.getcId(),objK);
					
					
				}					
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void storeHashTable(Hashtable<Long, Kontakt> hash)
	{
		try
		{		
			File file = new File("C:\\git\\AppDev2-RocketCRM-Server\\src\\dbFolder\\dbFile.csv");
			
			FileWriter fw = new FileWriter(file);
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
		catch(IOException ioexcp)
		{
			ioexcp.printStackTrace();
		}
	}
	
}
