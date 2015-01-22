package de.rfh.rocketcrm.control;

import java.io.Serializable;

public class Kontakt implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private long cId;
	private String cCrtDate;
	private String cCrtUser;
	private String cUpdtDate;
	private String cUpdtUser;
	private String cVName;
	private String cNName;
	private String cCompany;
	private String cCity;
	private String cBirthDay;
	private String cMail;
	private String cPhone;
	private long cVersion;
	
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}
	public String getcCrtDate() {
		return cCrtDate;
	}
	public void setcCrtDate(String cCrtDate) {
		this.cCrtDate = cCrtDate;
	}
	public String getcCrtUser() {
		return cCrtUser;
	}
	public void setcCrtUser(String cCrtUser) {
		this.cCrtUser = cCrtUser;
	}
	public String getcUpdtDate() {
		return cUpdtDate;
	}
	public void setcUpdtDate(String cUpdtDate) {
		this.cUpdtDate = cUpdtDate;
	}
	public String getcUpdtUser() {
		return cUpdtUser;
	}
	public void setcUpdtUser(String cUpdtUser) {
		this.cUpdtUser = cUpdtUser;
	}
	public String getcVName() {
		return cVName;
	}
	public void setcVName(String cVName) {
		this.cVName = cVName;
	}
	public String getcNName() {
		return cNName;
	}
	public void setcNName(String cNName) {
		this.cNName = cNName;
	}
	public String getcCompany() {
		return cCompany;
	}
	public void setcCompany(String cCompany) {
		this.cCompany = cCompany;
	}
	public String getcCity() {
		return cCity;
	}
	public void setcCity(String cCity) {
		this.cCity = cCity;
	}
	public String getcBirthDay() {
		return cBirthDay;
	}
	public void setcBirthDay(String cBirthDay) {
		this.cBirthDay = cBirthDay;
	}
	public String getcMail() {
		return cMail;
	}
	public void setcMail(String cMail) {
		this.cMail = cMail;
	}
	public String getcPhone() {
		return cPhone;
	}
	public void setcPhone(String cPhone) {
		this.cPhone = cPhone;
	}
	public long getcVersion() {
		return cVersion;
	}
	public void setcVersion(long cVersion) {
		this.cVersion = cVersion;
	}	
	
}
