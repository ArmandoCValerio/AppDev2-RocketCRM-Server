package de.rfh.rocketcrm.entity;

import java.sql.Connection;

public interface DataSource {

	Connection getConnection() throws Exception;
	void doDisConnect() throws Exception;	
}
