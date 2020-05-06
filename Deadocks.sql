--   XEvents Deadlocks

--Create XEvents Session
-- sp_configure 'show advanced options',1;
-- reconfigure
-- sp_configure 'xp_cmdshell',1; 
-- reconfigure;
/*
IF EXISTS (select * from sys.dm_xe_sessions where name = 'XE_Deadlocks')
BEGIN
	DROP EVENT SESSION [XE_Deadlocks] ON SERVER ;
	exec xp_cmdshell 'del c:\temp\XE_Deadlocks*.*'
END

CREATE EVENT SESSION [XE_Deadlocks] ON SERVER 
ADD EVENT sqlserver.xml_deadlock_report 
ADD TARGET package0.event_file(SET filename=N'C:\XEvents\XE_Deadlocks')
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_MULTIPLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=1 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [XE_Deadlocks] ON SERVER
STATE=START;
GO

*/
/*

--Setup : Deadlock Demo
--Two global temp tables with sample data for demo purposes.
DROP TABLE IF EXISTS ##Employees;
DROP TABLE IF EXISTS ##Suppliers;
CREATE TABLE ##Employees (
    EmpId INT IDENTITY,
    EmpName VARCHAR(16),
    Phone VARCHAR(16)
)
GO

INSERT INTO ##Employees (EmpName, Phone)
VALUES ('Martha', '800-555-1212'), ('Jimmy', '619-555-8080')
GO

CREATE TABLE ##Suppliers(
    SupplierId INT IDENTITY,
    SupplierName VARCHAR(64),
    Fax VARCHAR(16)
)
GO

INSERT INTO ##Suppliers (SupplierName, Fax)
VALUES ('Acme', '877-555-6060'), ('Rockwell', '800-257-1234')
GO
-------------------------------------------------------------------------------
--Session 1
BEGIN TRAN;                 
UPDATE ##Employees
SET EmpName = 'Mary'
WHERE EmpId = 1


--Session 1
UPDATE ##Suppliers
SET Fax = N'555-1212'
WHERE SupplierId = 1

--Session 2
BEGIN TRAN;
UPDATE ##Suppliers
SET Fax = N'555-1212'
WHERE SupplierId = 1

--Session 2
UPDATE ##Employees
SET Phone = N'555-9999'
WHERE EmpId = 1

*/



---------------------------------------------------
use tempdb 
go 


DROP TABLE IF EXISTS Deadlocks_Summary

CREATE TABLE [dbo].[Deadlocks_Summary](
	[ID] [int] NULL,
	[alerttime] [datetime] NULL,
	[Victim] [varchar](6) NULL,
	[clientapp] [nvarchar](255) NULL,
	[hostname] [varchar](255) NULL,
	[transactionname] [varchar](100) NULL,
	[spid] [int] NULL,
	[dbname] [sysname] NULL,
	[object] [sysname] NULL,
	[sql_statement] [nvarchar](max) NULL,
	[PageLock_ObjectName] [varchar](500) NULL,
	[keylock_ObjectName] [varchar](500) NULL,
	[keylock_Resource_Index] [varchar](500) NULL
) 

SET NOCOUNT ON
SET QUOTED_IDENTIFIER ON

DECLARE @Num_Deadlocks int = 50

DECLARE @outputfile varchar(1000)

SELECT @outputfile = soc.column_value
FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns soc
    ON s.address = soc.event_session_address
WHERE s.name like  '%XE_Deadlocks%'
and soc.column_name='filename'

select @outputfile=@outputfile+'*.xel'

 IF (object_id( 'tempdb..#CollectedDeadlocks' ) IS NOT NULL) drop table #CollectedDeadlocks

SELECT  top (@Num_Deadlocks)
		IDENTITY (int, 1,1) as ID
		,(CONVERT(xml, event_data).value('(event[@name="xml_deadlock_report"]/@timestamp)[1]','datetime')) as AlertTime
		,CONVERT(xml, event_data).query('/event/data/value/child::*') as deadlockGraph
	
 into #CollectedDeadlocks 
FROM sys.fn_xe_file_target_read_file(@outputfile, null, null, null)
 
WHERE object_name like 'xml_deadlock_report'
ORDER BY 2 desc

----ALTER TABLE #CollectedDeadlocks ADD ID INT IDENTITY

--SELECT * FROM #CollectedDeadlocks

DECLARE @i int =1
WHILE @i <=@Num_Deadlocks
BEGIN

			IF (object_id( 'tempdb..#CollectedDeadlocks_work' ) IS NOT NULL) DROP TABLE #CollectedDeadlocks_work ;
			SELECT * INTO #CollectedDeadlocks_work
			FROM #CollectedDeadlocks WHERE ID=@i
			
	
			IF (object_id( 'tempdb..#TMP_Deadlock' ) IS NOT NULL) drop table #TMP_Deadlock
			IF (object_id( 'tempdb..#TMP_SQLH' ) IS NOT NULL) drop table #TMP_SQLH
			IF (object_id( 'tempdb..#TMP_CPU' ) IS NOT NULL) drop table #TMP_CPU
		
			Create table #TMP_Deadlock
			(
				 ID int 
				,AlertTime datetime
				,spid int
				,victim varchar(100)
				,[process id] varchar(100)
				,clientapp varchar(255)
				,hostname varchar(255)
				,lasttranstarted datetime
				,transactionname varchar(100)
				,procname varchar(255)
				,line int
				,stmtstart int
				,stmtend int
				,sqlhandle varchar(255)	
				,PageLock_ObjectName varchar(500)
				,keylock_ObjectName varchar(500)
				,keylock_Resource_Index varchar(500)
			)

			INSERT INTO #TMP_Deadlock
			SELECT 
				X.ID,
				X.AlertTime
				--,T.N.value('../../@id','varchar(255)') process_id
				,T.N.value('../../@spid','int') spid
	
				,T.N.value('../../../../victim-list[1]/victimProcess[1]/@id','varchar(100)') victim
	
				,T.N.value('../../@id','varchar(100)') [process id]
				,T.N.value('../../@clientapp','varchar(255)') clientapp
				,T.N.value('../../@hostname','varchar(255)') hostname
				,T.N.value('../../@lasttranstarted','datetime') lasttranstarted
				,T.N.value('../../@transactionname','varchar(255)') transactionname
	
	
				,T.N.value('./@procname','varchar(255)') procname
				, T.N.value('./@line','int') line
				, T.N.value('./@stmtstart','int') stmtstart
				, T.N.value('./@stmtend','int') stmtend 
				, T.N.value('./@sqlhandle','varchar(255)') sqlhandle
				,T.N.value('../../../../resource-list[1]/pagelock[1]/@objectname','varchar(500)') PageLock_ObjectName
				,T.N.value('../../../../resource-list[1]/keylock[1]/@objectname','varchar(500)') keylock_ObjectName
				,T.N.value('../../../../resource-list[1]/keylock[1]/@indexname','varchar(500)') keylock_Resource_Index

			FROM ( 
					select cast(deadlockGraph as xml) xmlData,* from #CollectedDeadlocks_work
        
			) X 
			CROSS APPLY X.xmlData.nodes('//executionStack/frame') T(N) 
			order by AlertTime Desc 

			Create table #TMP_SQLH
			(
				ID int
				,spid int
				,victim varchar(100)
				,[process id] varchar(100)
				,alerttime datetime
				,clientapp varchar(255)
				,hostname varchar(255)
				,transactionname varchar(100)
				,Ident int identity not null
				,statement_start_offset int not null
				,statement_end_offset int not null
				,sql_handle varbinary(64) not null
				,PageLock_ObjectName varchar(500)
				,keylock_ObjectName varchar(500)
				,keylock_Resource_Index varchar(500)
			)

			INSERT into #TMP_SQLH
			SELECT ID
				,spid
				,victim
				,[process id]
				,alerttime
				,clientapp
				,hostname
				,transactionname
				,ISNULL(stmtstart,1)
				,ISNULL(stmtend,10000)
				,CONVERT(varbinary(64), sqlhandle, 1)
				,PageLock_ObjectName
				,keylock_ObjectName
				,keylock_Resource_Index
			FROM #TMP_Deadlock

			Select	
				S.ID
				,S.spid
				,S.victim
				,S.[process id]
				,S.alerttime
				,S.clientapp
				,S.hostname
				,S.transactionname
				 ,S.Ident
				,S.sql_handle
				,SUBSTRING(s2.text,  statement_start_offset / 2, ABS( (CASE WHEN statement_end_offset = -1 THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2) 
																	WHEN statement_end_offset-statement_start_offset/2 < 0 THEN 1000000000000
																	ELSE statement_end_offset END)  - statement_start_offset) / 2) as sql_statement
				,s2.DBID
				,convert(sysname, null ) as DBName
				,s2.ObjectId as ObjID
				,convert(sysname, null ) as Object
				,S.statement_start_offset 
				,S.statement_end_offset 
				,PageLock_ObjectName
				,keylock_ObjectName
				,keylock_Resource_Index
			into #TMP_CPU
			from #TMP_SQLH S
			CROSS APPLY sys.dm_exec_sql_text(S.sql_handle) AS s2  

			Update T Set DBName = d.name
			from
				#TMP_CPU T
				inner join master.dbo.sysdatabases d (nolock) on d.dbid = t.dbid

			DECLARE curObj CURSOR -- can also add READ_ONLY or SCROLL CURSOR
				FOR Select Distinct DBID, DBName from #TMP_CPU where DBName is not null

			Declare @Cmd varchar(max)
			Declare @DBID2 int
			Declare @DBName sysname
			OPEN curObj

			WHILE (1=1)
			BEGIN
				FETCH NEXT FROM curObj INTO  @DBID2, @DBName
				IF (@@fetch_status <> 0)
					break
				Set @Cmd = 'Update T set Object = name from #TMP_CPU T inner join ' + @DBName + '.dbo.sysobjects O (nolock) on O.ID = T.ObjID where t.dbid = ' + convert(varchar,@DBID2)
				exec ( @Cmd )
			END

			CLOSE curObj
			DEALLOCATE curObj

			--REPORT
			INSERT INTO tempdb.dbo.Deadlocks_Summary(ID
					,alerttime
					,Victim
					,clientapp
					,hostname
					,transactionname
					,spid
					,dbname
					,object
					,sql_statement
					,PageLock_ObjectName
					,keylock_ObjectName
					,keylock_Resource_Index)
			Select ID
				,alerttime
				,case 
					WHEN victim=[process id] THEN 'Victim' END as 'Victim'	
	
				,CASE LEFT(clientapp,15) 
					WHEN 'SQLAgent - TSQL' THEN  
					(	select top 1 'SQL Job = '+j.name from msdb.dbo.sysjobs (nolock) j
						inner join msdb.dbo.sysjobsteps (nolock) s on j.job_id=s.job_id
						where right(cast(s.job_id as nvarchar(50)),10) = RIGHT(substring(clientapp,30,34),10) )
					WHEN 'SQL Server Prof' THEN 'SQL Server Profiler'
					ELSE clientapp
					END as clientapp

				,hostname
				,transactionname
				,spid
				--,Ident
				--,dbid
				,dbname
				--,objid
				,object
				,sql_statement
				,PageLock_ObjectName
				,keylock_ObjectName 
				,keylock_Resource_Index
		   
			from 
				#TMP_CPU T
			order by
				alerttime desc;
		
			IF (object_id( 'tempdb..#TMP_Deadlock' ) IS NOT NULL) drop table #TMP_Deadlock
			IF (object_id( 'tempdb..#TMP_SQLH' ) IS NOT NULL) drop table #TMP_SQLH
			IF (object_id( 'tempdb..#TMP_CPU' ) IS NOT NULL) drop table #TMP_CPU

			SET @i=@i+1

			
END
	--IF (object_id( 'tempdb..#CollectedDeadlocks' ) IS NOT NULL) drop table #CollectedDeadlocks

	SELECT [object],[Victim],count(*) FROM tempdb.dbo.Deadlocks_Summary GROUP BY [object],[Victim]
	ORDER BY 3 desc


	SELECT * FROM tempdb.dbo.Deadlocks_Summary

	SELECT * FROM tempdb.dbo.Deadlocks_Summary WHERE id = 5


