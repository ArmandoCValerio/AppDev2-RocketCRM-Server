package de.rfh.rocketcrm.boundary;

import java.util.List;
import java.rmi.Remote;

import de.rfh.rocketcrm.control.Kontakt;

public interface KontaktService extends Remote {
	
	List<Kontakt> getKontakte() throws Exception;
	Kontakt getKontakt(Kontakt c) throws Exception;
	Kontakt createKontakt(Kontakt c) throws Exception;
	Kontakt deleteKontakt(Kontakt c) throws Exception;
	Kontakt editKontakt(Kontakt c) throws Exception;
	String sayHello(String msg) throws Exception;

	}