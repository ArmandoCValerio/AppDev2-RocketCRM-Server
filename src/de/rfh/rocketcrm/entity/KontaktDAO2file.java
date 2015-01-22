package de.rfh.rocketcrm.entity;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import de.rfh.rocketcrm.control.Kontakt;
import de.rfh.rocketcrm.util.HashQueue;

public class KontaktDAO2file implements KontaktDAO {
	
	public DataSource myDataSource = new FileDataSource();
	
	@Override
	public List<Kontakt> getKontakte() throws Exception {
		// TODO Auto-generated method stub
		List<Kontakt> kontaktarray = new ArrayList<Kontakt>();
		Enumeration<Long> e = FileDataSource.hash.keys();
		
	    while (e.hasMoreElements()) 
	    {
	       Long hashId = e.nextElement();
	       Kontakt objK = new Kontakt();
	       objK = FileDataSource.hash.get(hashId);
	       if  (objK != null)
	       {
				kontaktarray.add(objK);	
	       }
	    }
		
		return kontaktarray;
	}

	@Override
	public Kontakt getKontakt(Kontakt c) throws Exception {
		// TODO Auto-generated method stub
		Kontakt getRec = new Kontakt();
		
		getRec = FileDataSource.hash.get(c.getcId());
		if  (getRec == null)
		{
			getRec = new Kontakt();
		} 		
		
		return getRec;
	}

	@Override
	public Kontakt createKontakt(Kontakt c) throws Exception {
		// TODO Auto-generated method stub
		c.setcUpdtDate("2015-01-20");
		c.setcCrtDate("2015-01-20");
		c.setcUpdtUser("admin");
		c.setcCrtUser("admin");
		c.setcVersion(1);
		
		FileDataSource.hash.put(c.getcId(),c);
		
		//myDataSource.doDisConnect();
		HashQueue.hash2Queue(FileDataSource.hash);
		return c;
	}

	@Override
	public Kontakt deleteKontakt(Kontakt c) throws Exception {
		// TODO Auto-generated method stub
		Kontakt getRec = new Kontakt();
		
		getRec = FileDataSource.hash.get(c.getcId());
		if  (getRec == null)
		{
			getRec = new Kontakt();
		} else
		{
			if  (getRec.getcVersion() == c.getcVersion())
			{		
				FileDataSource.hash.remove(c.getcId());
				
				//myDataSource.doDisConnect();
				HashQueue.hash2Queue(FileDataSource.hash);
			}
		}
		
		return getRec;
	}

	@Override
	public Kontakt editKontakt(Kontakt c) throws Exception {
		// TODO Auto-generated method stub
		Kontakt getRec = new Kontakt();
		getRec = FileDataSource.hash.get(c.getcId());
		if  (getRec == null)
		{
			getRec = new Kontakt();
		} else		
		{
			if  (getRec.getcVersion() == c.getcVersion())
			{		
				c.setcUpdtDate("2015-01-20");
				c.setcUpdtUser("admin");
				long id = c.getcVersion();
				id++;
				c.setcVersion(id);
				FileDataSource.hash.put(c.getcId(),c);
			
				//myDataSource.doDisConnect();
				HashQueue.hash2Queue(FileDataSource.hash);
			}
		}
		
		return c;
	}

	@Override
	public String sayHello(String msg) throws Exception {
		// TODO Auto-generated method stub		
		System.out.println("Server_Message_DAOfile: " + msg);
		msg = "Hello Client! Your Message:" + msg; 
		return msg;	
	}
	
}
