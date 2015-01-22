package de.rfh.rocketcrm.entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import de.rfh.rocketcrm.control.Kontakt;

public class KontaktDAO2db implements KontaktDAO {
	
	private DataSource myDataSource = new H2DataSource();

	public List<Kontakt> getKontakte() throws Exception {
		
		// Normales Array ist NICHT dynamisch erweiterbar, daher ArrayList
		List<Kontakt> kontaktarray = new ArrayList<Kontakt>();
	
		Connection myConnection = myDataSource.getConnection();
		if  (myConnection != null) 
		{
			String sql = "SELECT * FROM Kontakt";

			PreparedStatement myStatement = myConnection.prepareStatement(sql);
			ResultSet myResultSet = myStatement.executeQuery();				
			
			int i = 0;
			while (myResultSet.next()) 
			{
				
				kontaktarray.add(new Kontakt());
				
				kontaktarray.get(i).setcId(myResultSet.getLong("CID"));
				kontaktarray.get(i).setcNName(myResultSet.getString("CNNAME"));
				kontaktarray.get(i).setcVName(myResultSet.getString("CVNAME"));
						
				i++;
				
			}	
					
		}
		
		myDataSource.doDisConnect();
		return kontaktarray;	
	}

		
	public Kontakt getKontakt(Kontakt k) throws Exception
	{
		Connection myConnection = myDataSource.getConnection();

		String sql = "SELECT * FROM Kontakt WHERE CID = ?";

		PreparedStatement myStatement = myConnection.prepareStatement(sql);
		myStatement.setLong(1, k.getcId());
						
		ResultSet myResultSet = myStatement.executeQuery();
		// myResultSet.beforeFirst();
		
		// Datensatz vorhanden?
		if (myResultSet.next()) {
		
			k.setcId(myResultSet.getLong("CID"));
			k.setcNName(myResultSet.getString("CNNAME"));
			k.setcVName(myResultSet.getString("CVNAME"));
		
		// Kein Datensatz vorhanden? Error setzen, Exception werfen
		} else {
			int errorID = ErrorIDs.cErrRecordNotFound;
			String errorIDstring = String.valueOf(errorID) ;
			throw new Exception(errorIDstring);					
		}				
		
		myDataSource.doDisConnect();
		return k;
	}

	public Kontakt createKontakt(Kontakt k) throws Exception
	{
		// TODO Auto-generated method stub
		k.setcUpdtDate("2015-01-20");
		k.setcCrtDate("2015-01-20");
		k.setcUpdtUser("admin");
		k.setcCrtUser("admin");
		
		//sql ausführen!
		
		return k;
	}

	public Kontakt deleteKontakt(Kontakt k) throws Exception
	{
		// TODO Auto-generated method stub
		
		//sql ausführen!
		
		return k;
	}
	
	public Kontakt editKontakt(Kontakt k) throws Exception
	{
		// TODO Auto-generated method stub
		k.setcUpdtDate("2015-01-20");
		k.setcUpdtUser("admin");
		
		//sql ausführen!
		
		return k;
	}

	@Override
	public String sayHello(String msg) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("Server_Message_DAOdb: " + msg);
		msg = "Hello Client! Your Message:" + msg; 
		return msg;
	}

}
