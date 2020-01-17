set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
if exists (select 1
          from sysobjects
          where  id = object_id('usp_UA_Refs_Delete')
          and type in ('P','PC'))
   drop procedure usp_UA_Refs_Delete
go


create procedure [dbo].[usp_UA_Refs_Delete] 
   @UAServer_ID          UASERVER_ID_TYPE ,  
   @SourceUANodeID       UAEXPANDEDNODEID_TYPE,
   @DestinationUANodeID  UAEXPANDEDNODEID_TYPE,
   @UAReferenceType      UANODEID_TYPE,
   @UAIsForward          bit
as

begin
    Delete from UA_Refs  where UAServer_ID=@UAServer_ID 
 and ISNULL(@SourceUANodeID,0)=ISNULL(SourceUANodeID,0)
 and ISNULL(@DestinationUANodeID,0)=ISNULL(DestinationUANodeID,0)
 and ISNULL(@UAReferenceType,0)=ISNULL(UAReferenceType,0)
 and ISNULL(@UAIsForward,0)=ISNULL(UAIsForward,0)
  return

end
GO
 grant execute on usp_UA_Refs_Delete to [UserCalcService]
GO

if exists (select 1
          from sysobjects
          where  id = object_id('usp_UA_Server_Delete')
          and type in ('P','PC'))
   drop procedure usp_UA_Server_Delete
go

create procedure [dbo].[usp_UA_Server_Delete] 
   @UAServer_ID          UASERVER_ID_TYPE   
as

begin
    Delete from UA_Servers  where UAServer_ID=@UAServer_ID  
  return

end


GO
 grant execute on usp_UA_Server_Delete to [UserCalcService]
GO
if exists (select 1
          from sysobjects
          where  id = object_id('usp_UA_Nodes_Delete')
          and type in ('P','PC'))
   drop procedure usp_UA_Nodes_Delete
go



create procedure [dbo].[usp_UA_Nodes_Delete] 
   @UAServer_ID          UASERVER_ID_TYPE ,  
   @UANodeID             UAEXPANDEDNODEID_TYPE
  
as

begin
    Delete from UA_Nodes where UAServer_ID=@UAServer_ID 
            and ISNULL(UANodeID,0)=ISNULL(@UANodeID,0);      
  return

end

GO
 grant execute on usp_UA_Nodes_Delete to [UserCalcService]
GO