if exists (select 1
          from sysobjects
          where  id = object_id('usp_UA_NamespaceUris_Insert')
          and type in ('P','PC'))
   drop procedure usp_UA_NamespaceUris_Insert
go



create procedure usp_UA_NamespaceUris_Insert 
    @Server_Id int, 
	@NamespaceName nvarchar(200) 
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
  

	--добавляем UA_Servers_NamespaceUris 
	declare @NamespaceID  UANAMESPACEINDEX_ID_TYPE = 0

	select @NamespaceID=max(UANamespaceIndex_ID) from UA_Servers_NamespaceUris where UAServer_ID= @Server_Id  and NamespaceUri like @NamespaceName
	if (isnull(@NamespaceID,0)<=0)
	begin	
		select @NamespaceID= max(UANamespaceIndex_ID) from UA_Servers_NamespaceUris where UAServer_ID= @Server_Id

		if (isnull(@NamespaceID,0)<0)
			set @NamespaceID=0
				
		set @NamespaceID=isnull(@NamespaceID,0)+1
	
		insert into UA_Servers_NamespaceUris (UAServer_ID,UANamespaceIndex_ID,NamespaceUri,CreateDateTime)
		values  (@Server_Id,@NamespaceID,@NamespaceName,getdate())

	end

	return @NamespaceID
end
go

grant EXEC on usp_UA_NamespaceUris_Insert to UserExportService
go
grant EXEC on usp_UA_NamespaceUris_Insert to UserCalcService
go