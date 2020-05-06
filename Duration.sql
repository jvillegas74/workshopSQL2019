
--   XEvents Duration

--Create XEvents Session
-- sp_configure 'show advanced options',1;
-- reconfigure
-- sp_configure 'xp_cmdshell',1; 
-- reconfigure;
/*
IF EXISTS (select * from sys.dm_xe_sessions where name = 'XE_Duration')
BEGIN
	DROP EVENT SESSION [XE_Duration] ON SERVER ;
	exec xp_cmdshell 'del c:\temp\XE_Duration*.*'
END

CREATE EVENT SESSION [XE_Duration] ON SERVER 
ADD EVENT sqlserver.rpc_completed(SET collect_statement=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.query_hash,sqlserver.server_principal_name,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(0))),
ADD EVENT sqlserver.sp_statement_completed(SET collect_object_name=(1),collect_statement=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.query_hash,sqlserver.server_principal_name,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(0))),
ADD EVENT sqlserver.sql_batch_completed(SET collect_batch_text=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.query_hash,sqlserver.server_principal_name,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(0))),
ADD EVENT sqlserver.sql_statement_completed(SET collect_statement=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.database_name,sqlserver.query_hash,sqlserver.server_principal_name,sqlserver.session_id,sqlserver.sql_text)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(0)))
ADD TARGET package0.event_file(SET filename=N'C:\XEvents\XE_Duration',max_file_size=(100),max_rollover_files=(10))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=5 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=ON)
GO


ALTER EVENT SESSION [XE_Duration] ON SERVER
STATE=START;
GO
*/

SET NOCOUNT ON
SET QUOTED_IDENTIFIER ON

DECLARE @outputfile varchar(500)

   SELECT @outputfile = soc.column_value
FROM sys.dm_xe_sessions s
JOIN sys.dm_xe_session_object_columns soc
    ON s.address = soc.event_session_address
WHERE s.name like  '%duration%'
and soc.column_name='filename'

drop table if exists Tempdb.dbo.XE_EventXML_RPC_Completed

DECLARE 
	@path NVARCHAR(260) = @outputfile+'*', 
	@mdpath NVARCHAR(260) = @outputfile+'*.xem', 
	@initial_file_name NVARCHAR(260) = NULL, 
	@initial_offset BIGINT = NULL 
	
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
into Tempdb.dbo.XE_EventXML_RPC_Completed
FROM 
	master.sys.fn_xe_file_target_read_file (@path, @mdpath, @initial_file_name, @initial_offset) E

--select * FROM 
--	master.sys.fn_xe_file_target_read_file (@path, @mdpath, @initial_file_name, @initial_offset) T	

--select * from Tempdb.dbo.XE_EventXML_RPC_Completed
drop table if exists #TMP01;
SELECT 
T.object_name as Event_Class,
--D.Name as DB_Name,
--AO.Name as Object_Name,
(event_data_XML.value('event[1]/@timestamp','datetime')) AS [TimeStamp]
,event_data_XML.value('(event/data[1])[1]','varchar(max)') AS cpu_time
,event_data_XML.value('(event/data[2])[1]','BIGINT') AS duration
--,event_data_XML.value('(event/data[3])[1]','BIGINT') AS physical_reads
--,event_data_XML.value('(event/data[4])[1]','BIGINT') AS logical_reads
--,event_data_XML.value('(event/data[5])[1]','BIGINT') AS writes
--,event_data_XML.value('(event/data[7])[1]','BIGINT') AS row_count

--,event_data_XML.value('(event/data[4])[1]','VARCHAR(512)') AS CPU
--,event_data_XML.value('(event/data[5])[1]','VARCHAR(512)')/1000000 AS Duration

,event_data_XML.value('(event/data[9])[1]','VARCHAR(512)') AS [object_name]
,event_data_XML.value('(event/data[10])[1]','VARCHAR(8000)') AS [statement]

--,event_data_XML.value('(event/action[1])[1]','VARCHAR(512)') AS Client_App_Name

,event_data_XML.value('(event/action[2])[1]','VARCHAR(512)') AS server_principal_name
,event_data_XML.value('(event/action[4])[1]','VARCHAR(512)') AS database_name
,event_data_XML.value('(event/action[6])[1]','VARCHAR(512)') AS client_hostname

--,event_data_XML.value('(event/action[5])[1]','VARCHAR(512)') AS session_nt_username

--,event_data_XML.value('(event/action[6])[1]','VARCHAR(8000)') AS Sql_Text
--,event_data_XML.value('(event/action[8])[1]','VARCHAR(512)') AS Username

--INTO Tempdb.dbo.XEventsTemp
--,CAST(event_data_XML.value('(event/action[2])[1]','VARCHAR(512)') AS XML).value('(frame/@handle)[1]','VARCHAR(50)') AS handle
--,T.*
INTO #TMP01
FROM
(
SELECT CAST(event_data AS XML) event_data_XML, *
FROM 
	master.sys.fn_xe_file_target_read_file (@path, @mdpath, @initial_file_name, @initial_offset)) T
	
--INNER JOIN sys.databases D on T.event_data_XML.value('(event/data[1])[1]','INT')=D.database_id 
--LEFT JOIN sys.all_objects AO on T.event_data_XML.value('(event/data[2])[1]','INT')=AO.object_id
ORDER BY T.file_offset 	
GO
-----------------------------------------------


select object_name,min(timestamp) as [First Execution],max(timestamp) as [Last Execution],min(duration) as [Min Duration],max(duration) as [Max Duration],avg(duration) as [Avg. Duration],count(*) as [Counter]
from #TMP01 
--where cast(timestamp as date) = '2020-02-21'
group by object_name--where object_name='SP_PROTOTYPE_NEW_ICS_FULL'

select * from #TMP01

