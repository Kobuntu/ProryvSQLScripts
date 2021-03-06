
if exists (select 1
          from sysobjects
          where  id = object_id('usp_Import_LERS_Structure')
          and type in ('P','PC'))
   drop procedure usp_Import_LERS_Structure
go


create procedure usp_Import_LERS_Structure 
    @Server_Id int, 
	@Namespace_ID UANAMESPACEINDEX_ID_TYPE ,
	@TITypeUA TI_Type = 11
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
  



	declare @RootObjectNodeID varchar(200)
	select @RootObjectNodeID= UANodeID from UA_Nodes 
	where UAServer_ID = @Server_Id
	and UANodeID = 'i=85'
	and UABrowseNameName like 'Objects'
 
	declare @rootTIID varchar(200)= 'ns='+convert(varchar,@Namespace_ID)+';minenergo340.root.tilist'

	DECLARE  @tempUANodes DataCollectUANodesVariableTableType
	DECLARE @tempUANodeRefs DataCollectUARefsTableType

	if (not exists (select top 1 1 from UA_Nodes 
					where UAServer_ID = @Server_ID and UANodeID=@rootTIID 
					))
	begin 

		--создаем корневой узел для списка ТИ
		INSERT INTO @tempUANodes (
		 [UANode_ID],
		 [UAServer_ID],
		 [TIType],
		 [UANodeID] ,
		 [UABrowseNameNamespaceIndex] ,
		 [UABrowseNameName] ,
		 [UADisplayNameLocale] ,
		 [UADisplayNameText] ,
		 [UANodeClass_ID] ,
		 [UATypeNode_ID] ,
		 [UATypeDefinition] ,
		 [UABaseAttributeDescription],
		 [UABaseAttributeWriteMask] ,
		 [UABaseAttributeUserWriteMask] ,
		 [CreateDateTime] ,
		 [ModifyDateTime] ,
		 [Value] ,
		 [UADataTypeNode_ID] ,
		 [UADataType] ,
		 [ValueRank] ,
		 [ArrayDimensions],
		 [AccessLevel],
		 [UserAccessLevel] ,
		 [MinimumSamplingInterval] ,
		 [Historizing] ) 
  
		  select   
			null,
			@Server_Id,
			@TITypeUA,
			@rootTIID,
			@Namespace_ID, 
			'Теплоучет ЛЭРС',
			null,
			'Теплоучет ЛЭРС',	1,
			4052,	'i=61',
			'Теплоучет ЛЭРС',
			0,	0,	getdate(),getdate(),
			value =null,null,'i=10',
			-1,
			'',
			1,
			1,
			0,
			0 	
		

		INSERT INTO @tempUANodeRefs (
		UAServer_ID,	
		SourceUANode_ID, SourceUANodeID,	DestinationUANode_ID,
		DestinationUANodeID,DestinationUANodeClass_ID,
		UAReferenceNode_ID, UAReferenceType ,UAIsForward, UATypeNode_ID, UATypeId, CreateDateTime, ModifyDateTime)
		SELECT 
		@Server_Id,
		NULL, @RootObjectNodeID, 
		NULL, @rootTIID, 
		Null,
		6057,'i=47',0,NULL,'i=518',getdate(),getdate()
	
		
		EXEC usp_UA_Nodes_InsertNew_Variable_Fast @UAVariableTable =@tempUANodes

		EXEC [dbo].[usp_UA_Refs_InsertNew_Fast] @RefsTable = @tempUANodeRefs
	
	

	end 


	delete from @tempUANodes
	delete from @tempUANodeRefs





			
	--Еще подстанции добавим из	
	--select min(LERSCODE_ID), GroupName from EXPL_LERS_Codes
	--group by GroupName



	update EXPL_LERS_Codes set StringName = '' where StringName is null
 
		INSERT INTO @tempUANodes (
		 [UANode_ID],
		 [UAServer_ID],
		 [TIType],
		 [UANodeID] ,
		 [UABrowseNameNamespaceIndex] ,
		 [UABrowseNameName] ,
		 [UADisplayNameLocale] ,
		 [UADisplayNameText] ,
		 [UANodeClass_ID] ,
		 [UATypeNode_ID] ,
		 [UATypeDefinition] ,
		 [UABaseAttributeDescription],
		 [UABaseAttributeWriteMask] ,
		 [UABaseAttributeUserWriteMask] ,
		 [CreateDateTime] ,
		 [ModifyDateTime] ,
		 [Value] ,
		 [UADataTypeNode_ID] ,
		 [UADataType] ,
		 [ValueRank] ,
		 [ArrayDimensions],
		 [AccessLevel],
		 [UserAccessLevel] ,
		 [MinimumSamplingInterval] ,
		 [Historizing] ) 
  
		  select  distinct 
			null,
			@Server_Id,
			@TITypeUA,
			'ns='+convert(varchar,@Namespace_ID)+';minenergo340.PSCode.'+Convert(varchar,temp.PSCode),
			@Namespace_ID, 
			temp.GroupName,	
			null,
			temp.GroupName,	
			1,
			4052,	'i=61',
			temp.GroupName,	
			0,	0,	getdate(),getdate(),
			value =null,null,'i=10',
			-1,
			'',
			1,
			1,
			0,
			0
		from EXPL_LERS_Codes temp
		where   isnull(temp.PSCode,'')<>''  	
		and not exists (select top 1 1 from UA_Nodes where UAServer_ID= @server_ID and UANodeID ='ns='+convert(varchar,@Namespace_ID)+';minenergo340.PSCode.'+Convert(varchar,temp.PSCode))
	
		INSERT INTO @tempUANodeRefs (
		UAServer_ID,	
		SourceUANode_ID, SourceUANodeID,	DestinationUANode_ID,
		DestinationUANodeID,DestinationUANodeClass_ID,
		UAReferenceNode_ID, UAReferenceType ,UAIsForward, UATypeNode_ID, UATypeId, CreateDateTime, ModifyDateTime)
		SELECT 
		@Server_Id,
		NULL, @rootTIID, 
		NULL, UANodeID, 
		Null,
		6057,'i=47',0,NULL,'i=518',getdate(),getdate()	
		from @tempUANodes
		where UAServer_ID= @Server_Id
		 and not exists (select top 1 1 from UA_Refs where UAServer_ID= @Server_ID and (DestinationUANodeID=@rootTIID and SourceUANodeID=UANodeID))
		 and UANodeID <> @rootTIID


 
		EXEC usp_UA_Nodes_InsertNew_Variable_Fast @UAVariableTable =@tempUANodes

		EXEC [dbo].[usp_UA_Refs_InsertNew_Fast] @RefsTable = @tempUANodeRefs
	
	
	
	delete from @tempUANodes
	delete from @tempUANodeRefs


	update EXPL_LERS_Codes set StringName = '' where StringName is null
 
		INSERT INTO @tempUANodes (
		 [UANode_ID],
		 [UAServer_ID],
		 [TIType],
		 [UANodeID] ,
		 [UABrowseNameNamespaceIndex] ,
		 [UABrowseNameName] ,
		 [UADisplayNameLocale] ,
		 [UADisplayNameText] ,
		 [UANodeClass_ID] ,
		 [UATypeNode_ID] ,
		 [UATypeDefinition] ,
		 [UABaseAttributeDescription],
		 [UABaseAttributeWriteMask] ,
		 [UABaseAttributeUserWriteMask] ,
		 [CreateDateTime] ,
		 [ModifyDateTime] ,
		 [Value] ,
		 [UADataTypeNode_ID] ,
		 [UADataType] ,
		 [ValueRank] ,
		 [ArrayDimensions],
		 [AccessLevel],
		 [UserAccessLevel] ,
		 [MinimumSamplingInterval] ,
		 [Historizing] ) 
  
		  select   
			null,
			@Server_Id,
			@TITypeUA,
			'ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,''))),
			@Namespace_ID, 
			StringName,
			null,
			StringName+ ' ('+Code+')',	
			1,
			4052,	'i=61',
			'код в сторонней системе ='+Code,
			0,	0,	getdate(),getdate(),
			value =null,null,'i=10',
			-1,
			'',
			1,
			1,
			0,
			0
		from EXPL_LERS_Codes
		where   isnull(Code,'')<>''  	
		and not exists (select top 1 1 from UA_Nodes where UAServer_ID= @server_ID and UANodeID ='ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,''))))
	
		INSERT INTO @tempUANodeRefs (
		UAServer_ID,	
		SourceUANode_ID, SourceUANodeID,	
		DestinationUANode_ID,DestinationUANodeID,
		DestinationUANodeClass_ID,
		UAReferenceNode_ID, UAReferenceType ,UAIsForward, UATypeNode_ID, UATypeId, CreateDateTime, ModifyDateTime)
		SELECT 
		@Server_Id,
		NULL, 'ns='+convert(varchar,@Namespace_ID)+';minenergo340.PSCode.'+Convert(varchar,PSCode), 
		NULL, 'ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,''))), 
		Null,
		6057,'i=47',0,NULL,'i=518',getdate(),getdate()	
		from EXPL_LERS_Codes
		where
		 not exists (select top 1 1 from UA_Refs where UAServer_ID= @Server_ID 
						 and (DestinationUANodeID='ns='+convert(varchar,@Namespace_ID)+';minenergo340.PSCode.'+Convert(varchar,PSCode) 
						 and SourceUANodeID='ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,'')))))
		  


 
		EXEC usp_UA_Nodes_InsertNew_Variable_Fast @UAVariableTable =@tempUANodes

		EXEC [dbo].[usp_UA_Refs_InsertNew_Fast] @RefsTable = @tempUANodeRefs
	
	

	
		DECLARE  @tempUA_ChildTINodes DataCollectUANodesVariableTableType
		DECLARE @tempUA_ChildTIRefs DataCollectUARefsTableType




		declare @tempDatatable2 table (AddIDname nvarchar(200), StringName nvarchar(200))
		insert into @tempDatatable2
		values ('OD', 'Оперативные данные')
		--('MD', 'Уточненные данные'),

	
	
		INSERT INTO @tempUA_ChildTINodes (
		 [UANode_ID],
		 [UAServer_ID],
		 [TIType],
		 [UANodeID] ,
		 [UABrowseNameNamespaceIndex] ,
		 [UABrowseNameName] ,
		 [UADisplayNameLocale] ,
		 [UADisplayNameText] ,
		 [UANodeClass_ID] ,
		 [UATypeNode_ID] ,
		 [UATypeDefinition] ,
		 [UABaseAttributeDescription],
		 [UABaseAttributeWriteMask] ,
		 [UABaseAttributeUserWriteMask] ,
		 [CreateDateTime] ,
		 [ModifyDateTime] ,
		 [Value] ,
		 [UADataTypeNode_ID] ,
		 [UADataType] ,
		 [ValueRank] ,
		 [ArrayDimensions],
		 [AccessLevel],
		 [UserAccessLevel] ,
		 [MinimumSamplingInterval] ,
		 [Historizing] )   
		  select  distinct
			null,
			@Server_Id,
			@TITypeUA,
			'ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,'')))+'.'+temp.AddIDname,
			@Namespace_ID, 
			EXPL_LERS_Codes.StringName,--+ ' ('+temp.StringName+')',
			null,
			EXPL_LERS_Codes.StringName,--+ ' ('+temp.StringName+')',	
			2,
			4052,	'i=61',
			EXPL_LERS_Codes.StringName,--+ ' ('+temp.StringName+')',
			0,	0,	getdate(),getdate(),
			value =null,null,'i=10',
			-1,
			'',
			1,
			1,
			0,
			0 
		from EXPL_LERS_Codes,  @tempDatatable2 temp
		where   isnull(Code,'')<>''  	
		and not exists (select top 1 1 from UA_Nodes a where a.UAServer_ID= @Server_Id and a.UANodeID= 'ns='+convert(varchar,@Namespace_ID)+';minenergo340.ticode.'+ltrim(rtrim(isnull(Code,'')))+'.'+temp.AddIDname)
	
	
		INSERT INTO @tempUA_ChildTIRefs (
		UAServer_ID,	
		SourceUANode_ID, SourceUANodeID,	DestinationUANode_ID,
		DestinationUANodeID,DestinationUANodeClass_ID,
		UAReferenceNode_ID, UAReferenceType ,UAIsForward, UATypeNode_ID, UATypeId, CreateDateTime, ModifyDateTime)
		SELECT distinct
		@Server_Id,
		NULL, replace(replace(UANodeID,'.MD',''),'.OD',''), --родит находим удалив часть ИД
		NULL, UANodeID, 
		Null,
		6057,'i=47',0,NULL,'i=518',getdate(),getdate()	
		from @tempUA_ChildTINodes
		where UAServer_ID= @Server_Id
		 and not exists (select top 1 1 from UA_Refs where UAServer_ID= @Server_ID and (SourceUANodeID=UANodeID)) -- один к одному все равно
		 and UANodeID <> @rootTIID

	 
		EXEC usp_UA_Nodes_InsertNew_Variable_Fast @UAVariableTable =@tempUA_ChildTINodes

		EXEC [dbo].[usp_UA_Refs_InsertNew_Fast] @RefsTable = @tempUA_ChildTIRefs

	


	
	


		EXEC usp_UA_BrowseFinish @UAServer_ID =@Server_Id

	
	


end
go
grant EXEC on usp_Import_LERS_Structure to UserExportService
go
grant EXEC on usp_Import_LERS_Structure to UserCalcService
go
