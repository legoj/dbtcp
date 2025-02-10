# dbtcp
Simple database table copy utility which takes instruction from a config xml file. This tool was developed to ease copying subset of data from a big database table to another database. 
For example, copying subset of CMBoundary table data from SCCM database to an SQL Express database to be used by another tool (cmbAssess) for assessing IP scopes' coverage on the SCCM boundaries.

## Sample config xml
The config file contains the following elements:
 - *dbtcp* - root element
 - *db* - defines the database connection string, identified and referenced by its id attribute
 - *table* - defines the table info which is used to create the SQL query. see sample below for different types of declarations to customize the query.
 - *task* - defines the tasks which will be executed by the program; which basically declares the src and dst databases, and which tables to copy.

```
﻿<?xml version="1.0" encoding="utf-8" ?> 
<dbtcp>
	<db id="jgdb">Server=WORKSVR\SQLEXP;Database=MYDB;Trusted_Connection=True</db>
	<db id="cmdb">Server=SCCMSVR;Database=CM_JPP;Trusted_Connection=True</db>
	<table id="cmb" src="BoundaryEx" dst="CMBoundary">
		<where>Name LIKE 'JPP%'</where>
	</table>
	<table id="cbg" src="BoundaryGroup" dst="CMBoundaryGroup">
		<where>Name LIKE 'JPP%'</where>
	</table>
	<table id="cus" src="Users" dst="CMUsers">
		<where>Domain='MyDomain'</where>
	</table>
	<table id="crs" src="v_R_System" dst="CMDevices">
		<where>Resource_Domain_OR_Workgr0='MyDomain'</where>
	</table>
	<table id="vos" src="v_GS_OPERATING_SYSTEM" dst="CMIOS"/>
	<table id="vcs" src="v_GS_COMPUTER_SYSTEM" dst="CMIComputer">
		<select>
			<f id="ResourceID"/>
			<f id="TimeStamp"/>
			<f id="CurrentTimeZone0"/>
			<f id="Description0"/>
			<f id="Domain0"/>
			<f id="DomainRole0"/>
			<f id="Manufacturer0"/>
			<f id="Model0"/>
			<f id="Name0"/>
			<f id="NumberOfProcessors0"/>
			<f id="Roles0"/>
			<f id="Status0"/>
			<f id="SystemType0"/>
			<f id="TotalPhysicalMemory0"/>
			<f id="UserName0"/>
		</select>
	</table>
	<table id="vnc" src="v_GS_NETWORK_ADAPTER_CONFIGURATION" dst="CMINetConfig">
		<select>
			<f id="ResourceID" />
			<f id="TimeStamp" />
			<f id="RevisionID" />
			<f id="TimeStamp" />
			<f id="DefaultIPGateway0" />
			<f id="DHCPEnabled0" />
			<f id="DHCPLeaseExpires0" />
			<f id="DHCPLeaseObtained0" />
			<f id="DHCPServer0" />
			<f id="DNSHostName0" />
			<f id="Index0" />
			<f id="IPAddress0" />
			<f id="IPEnabled0" />
			<f id="IPSubnet0" />
			<f id="MACAddress0" />
			<f id="ServiceName0" />
		</select>
		<where>DefaultIPGateway0 IS NOT NULL</where>
	</table>
	<table id="vsv" src="v_GS_SERVICE" dst="CMIService"/>
	<table id="vgs" src="v_GS_SYSTEM" dst="CMISystem"/>
	<table id="vwu" src="v_GS_WindowsUpdate10" dst="CMIWinUpdate"/>
	<table id="vqf" src="v_GS_QUICK_FIX_ENGINEERING" dst="CMIQFE"/>

	<task id="CMTableExport" desc="Export the SCCM table data" src="cmdb" dst="jgdb">
		<table id="cmb"/>
		<table id="vsv"/>
	</task>
</dbtcp>
```
