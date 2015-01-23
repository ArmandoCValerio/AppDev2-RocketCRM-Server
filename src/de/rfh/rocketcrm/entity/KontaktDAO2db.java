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
				kontaktarray.get(i).setcVersion(myResultSet.getLong("CVERSION"));
				kontaktarray.get(i).setcCompany(myResultSet.getString("CCOMPANY"));
				kontaktarray.get(i).setcCity(myResultSet.getString("CCITY"));
				kontaktarray.get(i).setcMail(myResultSet.getString("CMAIL"));
				kontaktarray.get(i).setcBirthDay(myResultSet.getString("CBIRTHDAY"));
				kontaktarray.get(i).setcPhone(myResultSet.getString("CPHONE"));
				kontaktarray.get(i).setcUpdtDate(myResultSet.getString("CUPDTDATE"));
				kontaktarray.get(i).setcCrtDate(myResultSet.getString("CCRTDATE"));
				kontaktarray.get(i).setcUpdtUser(myResultSet.getString("CUPDTUSER"));
				kontaktarray.get(i).setcCrtUser(myResultSet.getString("CCRTUSER"));

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
			k.setcVersion(myResultSet.getLong("CVERSION"));
			k.setcCompany(myResultSet.getString("CCOMPANY"));
			k.setcCity(myResultSet.getString("CCITY"));
			k.setcMail(myResultSet.getString("CMAIL"));
			k.setcBirthDay(myResultSet.getString("CBIRTHDAY"));
			k.setcPhone(myResultSet.getString("CPHONE"));
			k.setcUpdtDate(myResultSet.getString("CUPDTDATE"));
			k.setcCrtDate(myResultSet.getString("CCRTDATE"));
			k.setcUpdtUser(myResultSet.getString("CUPDTUSER"));
			k.setcCrtUser(myResultSet.getString("CCRTUSER"));
			
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

		k.setcUpdtUser("admin");
		k.setcCrtUser("admin");
		
		Connection myConnection = myDataSource.getConnection();

		String sql = "INSERT INTO Kontakt " + 
				     "(CID, CUPDTDATE, CCRTDATE, CVERSION, CUPDTUSER, CCRTUSER, CVNAME, CNNAME, CCOMPANY, CCITY, CPHONE, CMAIL, CBIRTHDAY) " + 
	                 "VALUES ((SELECT MAX(CID) + 1 FROM Kontakt), CURDATE(), CURDATE(), 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)"; 	                 
		
		PreparedStatement myStatement = myConnection.prepareStatement(sql);
		myStatement.setString(1, k.getcUpdtUser());
		myStatement.setString(2, k.getcCrtUser());
		myStatement.setString(3, k.getcVName());
		myStatement.setString(4, k.getcNName());
		myStatement.setString(5, k.getcCompany());
		myStatement.setString(6, k.getcCity());
		myStatement.setString(7, k.getcPhone());
		myStatement.setString(8, k.getcMail());
		myStatement.setString(9, k.getcBirthDay());
	
		myStatement.executeUpdate();
		
		myDataSource.doDisConnect();
		
		return k;
	}

	public Kontakt deleteKontakt(Kontakt k) throws Exception
	{
		Connection myConnection = myDataSource.getConnection();
		
		String sql = "DELETE FROM Kontakt WHERE CID = ? AND CVERSION = ?";

		PreparedStatement myStatement = myConnection.prepareStatement(sql);
		myStatement.setLong(1, k.getcId());
		myStatement.setLong(2, k.getcVersion());
		myStatement.execute();
		
		myDataSource.doDisConnect();
		
		return k;
	}
	
	public Kontakt editKontakt(Kontakt k) throws Exception
	{
		k.setcUpdtUser("admin");

		Connection myConnection = myDataSource.getConnection();
		
		String sql = "UPDATE Kontakt SET CUPDTDATE = CURDATE(), CUPDTUSER = ? ,CVNAME = ? ,CNNAME = ? ," +
					 "CVERSION = CVERSION + 1 " + 
					 " WHERE CID = ? AND CVERSION = ?";

		PreparedStatement myStatement = myConnection.prepareStatement(sql);
		myStatement.setString(1, k.getcUpdtUser());
		myStatement.setString(2, k.getcVName());
		myStatement.setString(3, k.getcNName());
		myStatement.setLong(4, k.getcId());
		myStatement.setLong(5, k.getcVersion());
		
		myStatement.executeUpdate();
		
		myDataSource.doDisConnect();
		
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
