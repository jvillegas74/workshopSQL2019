
--   XEvents SQL Errors

--Create XEvents Session
-- sp_configure 'show advanced options',1;
-- reconfigure
-- sp_configure 'xp_cmdshell',1; 
-- reconfigure;
/*
IF EXISTS (select * from sys.dm_xe_sessions where name = 'XE_SQL_Errors')
BEGIN
	DROP EVENT SESSION [XE_SQL_Errors] ON SERVER ;
	exec xp_cmdshell 'del C:\XEvents\XE_SQL_Errors*.*'
END

CREATE EVENT SESSION [XE_SQL_Errors] ON SERVER 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.sql_text,sqlserver.tsql_stack,sqlserver.username)
    WHERE ([error_number]<>(2528) AND [error_number]<>(3014) AND [error_number]<>(4035) AND [error_number]<>(5701) AND [error_number]<>(5703) AND [error_number]<>(8153) AND [error_number]<>(22803) AND [error_number]<>(18265) AND [error_number]<>(14205) AND [error_number]<>(14213) AND [error_number]<>(14214) AND [error_number]<>(14215) AND [error_number]<>(14216) AND [error_number]<>(14549) AND [error_number]<>(14558) AND [error_number]<>(14559) AND [error_number]<>(14560) AND [error_number]<>(14561) AND [error_number]<>(14562) AND [error_number]<>(14563) AND [error_number]<>(14564) AND [error_number]<>(14565) AND [error_number]<>(14566) AND [error_number]<>(14567) AND [error_number]<>(14568) AND [error_number]<>(14569) AND [error_number]<>(14570) AND [error_number]<>(14635) AND [error_number]<>(14638) AND [error_number]<>(951) AND [error_number]<>(3211) AND [error_number]<>(9104) AND [error_number]<=(50000)))
ADD TARGET package0.event_file(SET filename=N'C:\XEvents\XE_SQL_Errors',max_file_size=(4000))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=1 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [XE_SQL_Errors] ON SERVER
STATE=START;
GO
*/

/*
SELECT 1/0;
CREATE PROCEDURE #SP1
AS
BEGIN
SELECT 1/0;

SELECT top 10 * FROM faketable;
END
GO
*/


USE Tempdb
GO




--Check path to use as @outputfile variable
SELECT @@servername as [Server Name],s.name,
    soc.column_name,
    soc.column_value
FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns soc
    ON s.address = soc.event_session_address
WHERE s.name like  '%error%'
and soc.column_name='filename'

--select * FROM sys.dm_xe_sessions s


SET NOCOUNT ON
SET QUOTED_IDENTIFIER ON

DECLARE @outputfile varchar(500)
 --SET @outputfile='K:\Interlink_XEvents_output\xEvent_Interlink_Target'

   SELECT @outputfile = soc.column_value
FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns soc
    ON s.address = soc.event_session_address
WHERE s.name like  '%error%'
and soc.column_name='filename'

 
IF NOT EXISTS (select * from Tempdb.sys.tables where name = 'XE_SQL_Errors')
BEGIN
	CREATE TABLE Tempdb.dbo.XE_SQL_Errors(
		[LogDate] datetime,
		[ServerName] sysname,
		[DatabaseName] [sysname] NULL,
		[ObjectName] [sysname] NULL,
		[Ex1_statement_text] [varchar](4000) NULL,
		[Ex2_statement_text] [varchar](4000) NULL,
		[Ex1_SQL_Text] [varchar](4000) NULL,
		[Ex2_SQL_Text] [varchar](4000) NULL,
		[event_name] [varchar](4000) NULL,
		[Min_event_time] [datetime] NULL,
		[Max_event_time] [datetime] NULL,
		[Error_Value] [int] NULL,
		[message] [varchar](4000) NULL,
		[AppName] [varchar](256) NULL,
		[Ex1_HostName] [varchar](256) NULL,
		[Ex2_HostName] [varchar](256) NULL,
		[Ex1_UserName] [varchar](256) NULL,
		[Ex2_UserName] [varchar](256) NULL,
		[# of Errors] [int] NULL)	

	CREATE CLUSTERED INDEX [XLogDate] ON [dbo].[XE_SQL_Errors](LogDate)
	
END



IF NOT EXISTS (select * from Tempdb.sys.tables where name = 'XE_EventDetail')
BEGIN

		CREATE TABLE [dbo].[XE_EventDetail](
			[event_time] [datetime] NULL,
			[event_name] [varchar](4000) NULL,
			[message] [varchar](4000) NULL,
			[error_number] [int] NULL,
			[activity_guid] [varchar](500) NULL,
			[activity_sequence] [int] NULL,
			[tsql_stack] [xml] NULL,
			[sql_text] [varchar](4000) NULL,
			[DBID] [int] NULL,
			[ObjectID] [int] NULL,
			[username] [varchar](256) NULL,
			[client_app_name] [varchar](256) NULL,
			[client_hostname] [varchar](256) NULL,
			[handle] [varbinary](1000) NULL,
			[offsetstart] [int] NULL,
			[offsetend] [int] NULL,
			[Statement_Text] [varchar](4000) NULL,
			[DatabaseName] [sysname] NULL,
			[ObjectName] [sysname] NULL,
			[severity] [int] NULL,
			[category_value] [int] NULL,
			[category_text] [varchar](100) NULL,
			[destination_value] [varchar](100) NULL,
			[destination_text] [varchar](100) NULL
		) ON [PRIMARY]

		CREATE CLUSTERED INDEX [XLogDate] ON [dbo].[XE_EventDetail](event_time)

END

--DELETE FROM Tempdb.dbo.XE_SQL_Errors WHERE Logdate < DATEADD(dd,-15,getdate())

IF NOT EXISTS (SELECT * FROM sys.dm_xe_sessions WHERE name = 'XE_SQL_Errors')
BEGIN
	SELECT 'XE Event Session "XE_SQL_Errors" does not exist'
	GOTO FINAL
END

----------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------
-- <<<< Target File Mode >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
-- STEK: Get the Events captured - Note that the session can be still running & this can be repeated


DROP TABLE IF EXISTS Tempdb.dbo.XE_EventXML ;

DECLARE 
	@path NVARCHAR(260) = @outputfile+'*', 
	@mdpath NVARCHAR(260) = @outputfile+'*.xem', 
	@initial_file_name NVARCHAR(260) = NULL, 
	@initial_offset BIGINT = NULL 
	select @path,@mdpath,@initial_file_name

	
Select 
	 Identity(int,1,1) as ID
	,* 
	,cast
		( 
			Replace( 
						E.event_data, char(3), '?'
					) 
			as xml
		) as X
into Tempdb.dbo.XE_EventXML
FROM 
	master.sys.fn_xe_file_target_read_file (@path, @mdpath, @initial_file_name, @initial_offset) E


DECLARE @Error_Count int,@First_Error datetime
SELECT @Error_Count=COUNT(*) FROM Tempdb.dbo.XE_EventXML (NOLOCK) 
select @First_Error=min(event_time) from Tempdb.dbo.XE_EventDetail (NOLOCK)

SELECT @Error_Count as [# Errors],@First_Error as [First Error],getdate() as [Now]


----------------------------------------------------------------------------------------------
-- STEK: Shred the XML for the above event types
IF EXISTS(select * from Tempdb.sys.tables where name = 'XE_EventDetail') DROP TABLE Tempdb.dbo.XE_EventDetail ;

--Shred the XML
SELECT 
	 (node.value('./@timestamp', 'datetime')) AS event_time
	,node.value('./@name', 'varchar(4000)') AS event_name
	,node.value('./data[@name="message"][1]','varchar(4000)') as [message]
	,node.value('./data[@name="error_number"][1]','int') as [error_number]
	,node.value('./action[@name="attach_activity_id"][1]','varchar(500)') as [activity_guid]	
	,cast(null as int) as activity_sequence
	,node.query('./action[@name="tsql_stack"][1]/value[1]/frames[1]/frame') as [tsql_stack]
	,node.value('./action[@name="sql_text"][1]','varchar(4000)') as [sql_text]
	,node.value('./action[@name="database_id"][1]','int') as [DBID]
	,cast(null as int) as ObjectID
	,node.value('./action[@name="username"][1]','varchar(256)') as username   
	,node.value('./action[@name="client_app_name"][1]','varchar(256)') as client_app_name 	
	,node.value('./action[@name="client_hostname"][1]','varchar(256)') as client_hostname
	,cast(null as varbinary(1000) ) AS handle
	,cast( null as int) as offsetstart
	,cast( null as int) as offsetend

	,cast(null as varchar(4000) ) as Statement_Text
	,cast( null as sysname) as [DatabaseName]
	,cast(null as sysname) as [ObjectName]
	
	--,substring(node.value('(./data)[6]', 'varchar(4000)'),len(node.value('(./data/value)[6]', 'varchar(4000)'))+1,1000) as [Type]
	
	,node.value('./data[@name="severity"][1]','int') as [severity]
	
	,node.value('./data[@name="category"][1]/value[1]','int') as category_value
	,node.value('./data[@name="category"][1]/text[1]','varchar(100)') as category_text
	,node.value('./data[@name="destination"][1]/value[1]','varchar(100)') as destination_value
	,node.value('./data[@name="destination"][1]/text[1]','varchar(100)') as destination_text
	
	--,#EventXML.*

INTO Tempdb.dbo.XE_EventDetail
FROM Tempdb.dbo.XE_EventXML (NOLOCK) X
CROSS APPLY X.x.nodes('//event') n (node)
order by 1

--select * from Tempdb.dbo.XE_EventDetail (nolock)
--where destination_text='USER'
--order by event_time


----------------------------------------------------------------------------------------------
-- STEK: Separate Activity GUID from Sequence number - for sorting later on (should be combined w above step)
Update D Set
	 activity_sequence	= CONVERT(int, RIGHT(activity_guid, LEN(activity_guid) - 37)) 
	,activity_guid		= CONVERT(uniqueidentifier, LEFT(activity_guid, 36))
FROM Tempdb.dbo.XE_EventDetail D

----------------------------------------------------------------------------------------------
-- STEK: Extract handles & Offsets (should be combined w above step)
--Get the SQL handles
Update D Set
	 Handle = CONVERT(varbinary(1000), frame.node.value('@handle', 'varchar(1000)'), 1)
	,offsetstart = frame.node.value('@offsetStart', 'int')
	,offsetend = frame.node.value('@offsetEnd', 'int')
FROM Tempdb.dbo.XE_EventDetail D
OUTER APPLY D.tsql_stack.nodes('(/frame)[1]') frame (node)
----------------------------------------------------------------------------------------------
-- STEK: For each handle, grab the SQL text (should be combined w single table above)
Update D Set
	  statement_text = Left( SUBSTRING(t.text, (IsNull(offsetstart,0)/2) + 1, ((case when IsNull(offsetend,0) > 0 then offsetend else 2*IsNull(len(t.text),0) end - IsNull(offsetstart,0))/2) + 1) , 4000 )
	,[DatabaseName] = DB.Name
	,objectid = T.objectid
FROM 
	Tempdb.dbo.XE_EventDetail D
	cross APPLY sys.dm_exec_sql_text(D.handle) t
	inner join master.sys.sysdatabases db (nolock) on db.dbid = D.dbid
-----------------------------------------------------------------------------------------------------
-- Dereference ObjectName
set nocount on
Declare @Tbl table ( Name sysname )
Declare @Stmt varchar(max)
Declare @DBName sysname
Declare @ObjectName sysname
Declare @ObjectId int

DECLARE curObj CURSOR FOR
	 Select Distinct [DatabaseName], 
			case 
				when objectid < 0 THEN -objectid
				ELSE objectid
				END as objectid
	from Tempdb.dbo.XE_EventDetail D (NOLOCK) where ObjectName is null and [DatabaseName] is not null and [objectid] is not null

OPEN curObj

WHILE (1=1)
BEGIN
	FETCH NEXT FROM curObj INTO @DBName, @ObjectId
	IF (@@fetch_status <> 0)
		break
		
	Set @Stmt = 'select Name from [' + @DBName + '].sys.sysobjects (nolock) where id = ' + convert( varchar(10), @ObjectId)
	Insert into @Tbl
		exec ( @Stmt )
	Set @ObjectName = null
	Select @ObjectName = Name from @Tbl
	Delete from @Tbl
	
	Update Tempdb.dbo.XE_EventDetail Set ObjectName = @ObjectName where [DatabaseName] = @DBName and objectid = @ObjectId

END

CLOSE curObj
DEALLOCATE curObj

-----------------------------------------------------------------------------------------------------	

----------------------------------------------------------------------------------------------
--Clean up
/*
	-- Note that you can stop & restart a event session without deleting the files to act like a pause session
	DROP EVENT SESSION XE_SQL_Errors
	ON SERVER
	GO

	-- Show the sessions currently running
	Select * from Tempdb.dbo.FNTB_XE_Current( ) order by Event_Session_id	
	Select * from Tempdb.dbo.FNTB_XE_Current_Events( ) CE order by CE.event_package_guid , CE.Event_Name

	-- Delete the file for this session	
	exec master.dbo.xp_cmdshell 'Dir "C:\Temp\xEvent_Interlink_Target*.*"'
	exec master.dbo.xp_cmdshell 'Del "C:\Temp\xEvent_Interlink_Target*.*"'
	exec master.dbo.xp_cmdshell 'Dir "C:\Temp\xEvent_Interlink_Target*.*"'

*/
----------------------------------------------------------------------------------------------



/*
select * from Tempdb.dbo.XE_EventDetail (nolock)
where destination_text='USER'
order by event_time
*/


	
-- Show Details
Select 
			
			count(*) as [# of Errors]
			,[message]
		   ,[DatabaseName]
		  ,[ObjectName]
		  ,'"'+min([statement_text])+'"' as Ex1_statement_text
		  ,'"'+max([statement_text])+'"' as Ex2_statement_text
		  ,'"'+min([SQL_Text])+'"' as Ex1_SQL_Text
		  ,'"'+max([SQL_Text])+'"' as Ex2_SQL_Text
		  --,[event_name]
		  ,min([event_time]) as Min_event_time
		  ,max([event_time]) as Max_event_time
		  ,[Error_number]
		  
		  ,[Client_App_Name]
		  ,min([Client_HostName]) as Ex1_Client_HostName
		  ,max([Client_HostName]) as Ex2_Client_HostName
		  ,min([UserName]) as Ex1_UserName
		  ,max([UserName]) as Ex2_UserName
		  
	from 
		  Tempdb.dbo.XE_EventDetail (NOLOCK)
	where
				IsNull([Error_number],0) < 50000 --and [Error_number] IN (6603,6624)
				--and [message]  like '%xml%'
				--and objectname like '%bill_of_%'
				
	group by
		   [DatabaseName]
		  ,[ObjectName]
		  ,[event_name]
		  ,[Error_number]
		  ,[message]
		  ,[Client_App_Name]
	order by 
		   [# of Errors] desc


Select 
			@@servername as Server_Name
				,[message]
		   ,[DatabaseName]
		  ,[ObjectName]
		  ,([statement_text]) as Ex1_statement_text
		 
		  ,([SQL_Text]) as Ex1_SQL_Text
		  --,[event_name]
		   ,([event_time]) as event_time
		  ,[Error_number]
		  
		  ,[Client_App_Name]
		  ,([Client_HostName]) as Client_HostName
		  ,([UserName]) as UserName
		  
		  
	from 
		  Tempdb.dbo.XE_EventDetail (NOLOCK)
	where
				IsNull([Error_number],0) < 50000
				--and [message]  like '%xml%'
				--AND [Error_number] IN (6603,6624)
				--and [message] not like '%was killed by hostname%'
				--and event_time >= '2014-06-22 00:00'		
	
	order by 
	event_time



		   /*
	
IF (SELECT count(*) FROM Tempdb.dbo.XE_EventDetail (NOLOCK) ) >0
BEGIN
	INSERT INTO Tempdb.dbo.XE_SQL_Errors     
	Select 
			getdate() as [date],
			@@servername as server_name,
		   [DatabaseName]
		  ,[ObjectName]
		  ,min([statement_text]) as Ex1_statement_text
		  ,max([statement_text]) as Ex2_statement_text
		  ,min([SQL_Text]) as Ex1_SQL_Text
		  ,max([SQL_Text]) as Ex2_SQL_Text
		  ,[event_name]
		  ,min([event_time]) as Min_event_time
		  ,max([event_time]) as Max_event_time
		  ,[Error_number]
		  ,[message]
		  ,[client_app_name]
		  ,min([Client_HostName]) as Ex1_HostName
		  ,max([Client_HostName]) as Ex2_HostName
		  ,min([UserName]) as Ex1_UserName
		  ,max([UserName]) as Ex2_UserName
		  ,count(*) as [# of Errors]	  
	 
	from 
		  Tempdb.dbo.XE_EventDetail (NOLOCK) 
	where
				IsNull([Error_number],0) < 50000 --and destination_text='USER'
	group by
		   [DatabaseName]
		  ,[ObjectName]
		  ,[event_name]
		  ,[Error_number]
		  ,[message]
		  ,[client_app_name]
	order by 
		   [# of Errors] desc
END



ALTER EVENT SESSION XE_SQL_Errors
ON SERVER
STATE = STOP

-- Delete the file for this session	
	SELECT @outputfile='DEL "'+@outputfile+'*.*"'
	exec master.dbo.xp_cmdshell @outputfile
	
ALTER EVENT SESSION XE_SQL_Errors
ON SERVER
STATE = START	

--SELECT * FROM Tempdb.dbo.XE_SQL_Errors   
--TRUNCATE TABLE Tempdb.dbo.XE_SQL_Errors 

*/
FINAL:
    --SELECT @@servername,getdate()
GO

/*
-- Show Details
Select 
			getdate() as now
			,@@servername as Server_Name
			--,count(*) as [# of Errors]
			,[message]
		   ,[DatabaseName]
		  ,[ObjectName]
		  ,([statement_text]) 
		  ,([SQL_Text]) 
		  ,[event_Time]
		  ,[Error_number]
		  
		  ,[Client_App_Name]
		  ,([Client_HostName])
		  ,([UserName]) as Ex2_UserName
		  
	from 
		  Tempdb.dbo.XE_EventDetail (NOLOCK) 
		  where [error_number]<> 8153
		  and Client_App_Name not like 'Microsoft SQL Server Management Studio%'
		  order by event_time 
*/
/*
select [message] 
,'"'+Ex1_SQL_Text+'"' as SQL_Text
,ObjectName
,'"'+Ex1_Statement_Text+'"' as Statement
,Min_Event_Time
,Max_Event_Time
,Error_Value
,AppName
,Ex1_HostName as HostName
,Ex1_UserName as UserName
,[# of Errors]

 from dbo.XE_SQL_Errors (nolock) 
where 
	LogDate >='2013-08-20'
	--and appname like '%crystal%'
	 --and [message] like '%index%'
	 */

