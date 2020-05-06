

/*
use master
go
DROP DATABASE IF EXISTS [DB_Audit]
GO
CREATE DATABASE [DB_Audit]
GO
ALTER DATABASE [DB_Audit] SET ENABLE_BROKER 
GO
*/
USE DB_Audit
GO
IF EXISTS (SELECT * from sys.tables where name = 'DBA_NS_Audit')
	DROP TABLE dbo.DBA_NS_Audit
GO
USE [DB_Audit]
GO

CREATE TABLE [dbo].[DBA_NS_Audit](
	[id] [int] IDENTITY(1,1) NOT NULL primary key,
	PostTime [datetime] NULL,
	EventType [sysname] NOT NULL,
	LoginName [sysname] NOT NULL,
	DatabaseName [sysname] NOT NULL,
	SchemaName [sysname] NOT NULL,
	[ObjectName] [sysname] NOT NULL,
	ObjectType [sysname] NOT NULL,
	[spid] [int] NULL,
	[hostname] [varchar](500) NULL,
	[eventXMLData] [xml] NULL)

GO

IF EXISTS (select * from sys.procedures where name = 'SP_DBA_NS_Audit')
	DROP PROCEDURE dbo.SP_DBA_NS_Audit 
GO
CREATE PROCEDURE dbo.SP_DBA_NS_Audit  
    AS  
    BEGIN  
     DECLARE @eventDataXML XML, @hostname nvarchar(128), @spid smallint ;  
     RECEIVE TOP(1) @eventDataXML=message_body  
      FROM DB_Audit.dbo.AuditQueue         
       
     IF CAST(@eventDataXML as XML) is not null   
     BEGIN     

              SELECT @hostname = host_name FROM sys.dm_exec_sessions where session_id = @eventDataXML.value('(/EVENT_INSTANCE/SPID)[1]','int')
              --SELECT @hostname=client_net_address    FROM sys.dm_exec_connections where session_id = @eventDataXML.value('(/EVENT_INSTANCE/SPID)[1]','int')
    
       INSERT INTO DB_Audit.dbo.DBA_NS_Audit (PostTime, EventType,LoginName,DatabaseName,SchemaName,ObjectName,ObjectType,SPID,hostname, eventXMLData)  
        VALUES (
		@eventDataXML.value('(/EVENT_INSTANCE/PostTime)[1]','datetime')
		,@eventDataXML.value('(/EVENT_INSTANCE/EventType)[1]','nvarchar(500)')
		,@eventDataXML.value('(/EVENT_INSTANCE/LoginName)[1]','nvarchar(500)')
		,@eventDataXML.value('(/EVENT_INSTANCE/DatabaseName)[1]','nvarchar(500)')
		,@eventDataXML.value('(/EVENT_INSTANCE/SchemaName)[1]','nvarchar(500)')
		,@eventDataXML.value('(/EVENT_INSTANCE/ObjectName)[1]','nvarchar(500)')
		,@eventDataXML.value('(/EVENT_INSTANCE/ObjectType)[1]','nvarchar(500)')		
		,@eventDataXML.value('(/EVENT_INSTANCE/SPID)[1]','int')		
		,@hostname
		,cast(@eventDataXML as XML)
		);       
     END  
    END;
GO
IF EXISTS (select * from sys.services where name = 'AuditService')
	DROP SERVICE AuditService
GO
IF EXISTS (select * from sys.service_queues where name = 'AuditQueue')
	DROP QUEUE AuditQueue
GO
CREATE QUEUE DB_Audit.dbo.AuditQueue
                     WITH STATUS = ON,
                     ACTIVATION (
                           procedure_name = DB_Audit.dbo.SP_DBA_NS_Audit,
                           MAX_QUEUE_READERS = 2,
                           EXECUTE AS N'dbo')
                     ON [DEFAULT] ;
GO

CREATE SERVICE 
                     AuditService ON QUEUE dbo.AuditQueue
                     ([http://schemas.microsoft.com/SQL/Notifications/PostEventNotification]);
GO

IF EXISTS (select * from sys.server_event_notifications where name = 'ENDBAudit')
	DROP EVENT NOTIFICATION ENDBAudit ON SERVER
GO
CREATE EVENT NOTIFICATION ENDBAudit
              ON SERVER
              FOR 
                     CREATE_FUNCTION,
                     ALTER_FUNCTION,
                     DROP_FUNCTION,
                     CREATE_INDEX,
                     ALTER_INDEX,
                     DROP_INDEX,
                     CREATE_PROCEDURE,
                     ALTER_PROCEDURE,
                     DROP_PROCEDURE,
                     CREATE_TABLE,
                     ALTER_TABLE,
                     DROP_TABLE,
                     CREATE_TRIGGER,
                     ALTER_TRIGGER,
                     DROP_TRIGGER,
                     CREATE_VIEW,
                     ALTER_VIEW,
                     DROP_VIEW
              TO SERVICE 'AuditService', 'current database';
GO
   

/*
-- Creacion de objectos
CREATE TABLE dbo.Table_Prueba1 (A int,B varchar(10))
GO
ALTER TABLE  dbo.Table_Prueba1 ADD C int
GO
DROP TABLE dbo.Table_Prueba1 
GO
CREATE PROCEDURE dbo.SPDBA_Proc_1 
AS
BEGIN
	SELECT * FROM sys.tables
END
GO
DROP PROCEDURE dbo.SPDBA_Proc_1 
GO
*/   


SELECT * from DB_Audit.dbo.DBA_NS_Audit (nolock) ORDER BY PostTime desc

