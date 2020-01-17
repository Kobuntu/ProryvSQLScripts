
IF   EXISTS (SELECT * FROM sysobjects WHERE name = N'usp2_Create_UANode_FreeItem' and xtype like 'P' )
 drop procedure usp2_Create_UANode_FreeItem
 GO
 
create procedure [dbo].[usp2_Create_UANode_FreeItem]   

 @PSID int , @UANode_ID bigint,   @UANodeID nvarchar(200) , 
@PSName nvarchar(400), @UANodeName  nvarchar(400)

as 
begin
set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted
begin try
declare  @TreeID int, @PS_FreeHierItem_ID int, @H3ID int, @H2ID int, @H2HierID hierarchyid
select @H3ID = HierLev3_ID, @H2ID= HierLev2_ID from vw_Dict_HierarchyPS where PS_ID = @PSID
--находим ИД дерева по уровню 2
select top 1 @TreeID= Dict_FreeHierarchyTree.FreeHierTree_ID, @H2HierID= Dict_FreeHierarchyTree.HierID
from Dict_FreeHierarchyTree
join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID=Dict_FreeHierarchyTree_Description.FreeHierItem_ID
where 
Dict_FreeHierarchyTree_Description.HierLev2_ID= @H2ID
--находим ИД узла ПС, если нет то создаем
select top 1 @PS_FreeHierItem_ID= Dict_FreeHierarchyTree.FreeHierItem_ID
from Dict_FreeHierarchyTree
join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID=Dict_FreeHierarchyTree_Description.FreeHierItem_ID
where 
Dict_FreeHierarchyTree.FreeHierTree_ID= @TreeID
and Dict_FreeHierarchyTree_Description.PS_ID= @PSID
if (@PS_FreeHierItem_ID is null)
begin
	--находим 3й уровень
	declare @H3_FreeHierItem_ID int
	select  top 1 @H3_FreeHierItem_ID=FreeHierItem_ID from Dict_FreeHierarchyTree
	where 
	Dict_FreeHierarchyTree.FreeHierTree_ID= @TreeID
	and HierID.GetAncestor(1)=@H2HierID
	and StringName like 'ТП'
	if (@H3_FreeHierItem_ID is not null)
	begin
		declare @psStringID varchar(255)
		set @psStringID=Convert(varchar(200),@PSID)
		declare @tempResult table (ID int)
		insert into @tempResult
		exec [usp2_FreeHierarchyTree_Insert] 
			@TreeID,
			@H3_FreeHierItem_ID	,
			@PSName,
			4,
			0,
			@psStringID, 
			0,
			null
		select top 1 @PS_FreeHierItem_ID=  ID from @tempResult
	end
end
declare @TI_FreeHierItem_ID int =null

if (@PS_FreeHierItem_ID is not null)
begin
		--добавляем ТИ (если найдется)
		declare @TI_ID int, @TINAme nvarchar(400)
		select top 1 @TI_ID=a.TI_ID, @TINAme= a.TIName from  
		Info_TI a 
		where a.PS_ID= @PSID
		and @UANodeName like  a.TIName +'%'
		--и нет ТИ с большим названием которое тоже входит в название узла UA
		and not exists (select top 1 1 from info_TI b where b.PS_ID = @PSID and b.TI_ID<>a.TI_ID and len(b.TIName)>len(b.TIName) and  @UANodeName like  b.TIName +'%')
		
		--если ТИ найдена
		--ищем эту ТИ в дереве своб иерархии под этой ПС, если нет т одобавляем
		select top 1 @TI_FreeHierItem_ID =Dict_FreeHierarchyTree.FreeHierItem_ID
		from Dict_FreeHierarchyTree
		join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID=Dict_FreeHierarchyTree_Description.FreeHierItem_ID
		where 
		Dict_FreeHierarchyTree.FreeHierTree_ID= @TreeID
		and Dict_FreeHierarchyTree_Description.TI_ID= @TI_ID
		--Добавляем ТИ в дерево
		if (@TI_ID is not null and @TI_FreeHierItem_ID is null)
		begin
			declare @TIStringID varchar(255)
			set @TIStringID=Convert(varchar(200),@TI_ID)
			declare @tempResultTI table (ID int)
			insert into @tempResultTI
			exec [usp2_FreeHierarchyTree_Insert] 
				@TreeID,
				@PS_FreeHierItem_ID	,
				@TINAme,
				5,
				0,
				@TIStringID, 
				0,
				null
			select top 1 @TI_FreeHierItem_ID=  ID from @tempResultTI
		end
end 

--добавляем на найденную ТИ или на ПС
declare @UANodeParent_FreeHierItem_ID  int 
set @UANodeParent_FreeHierItem_ID= case when @TI_FreeHierItem_ID is null then   @PS_FreeHierItem_ID else @TI_FreeHierItem_ID end
if (@UANodeParent_FreeHierItem_ID is not null)
begin
	declare @UANode_IDstring nvarchar(200)
	set @UANode_IDstring= convert(varchar(200),@UANode_ID)
	exec [usp2_FreeHierarchyTree_Insert] 
				@TreeID,
				@UANodeParent_FreeHierItem_ID	,
				@UANodeName,
				23,
				0,
				@UANode_IDstring, 
				1,
				null
end
end try
begin catch
	DECLARE @ErrorMessage NVARCHAR(4000);  
    DECLARE @ErrorSeverity INT;  
    DECLARE @ErrorState INT;  
    SELECT   
        @ErrorMessage = ERROR_MESSAGE(),  
        @ErrorSeverity = ERROR_SEVERITY(),  
        @ErrorState = ERROR_STATE();  
    RAISERROR (@ErrorMessage, -- Message text.  
               @ErrorSeverity, -- Severity.  
               @ErrorState -- State.  
               ); 
end catch
return @UANode_ID
end
GO

grant EXECUTE on usp2_Create_UANode_FreeItem to UserCalcService
go
grant EXECUTE on usp2_Create_UANode_FreeItem to UserDeclarator
go
  


IF   EXISTS (SELECT * FROM sysobjects WHERE name = N'usp2_Create_UANode' and xtype like 'P' )
 drop procedure usp2_Create_UANode
 GO
 
create procedure [dbo].[usp2_Create_UANode]   
			 @Server_ID int,
			 @TIType int =11,
			 @UANodeID nvarchar(400),
			 @UABrowseNameNamespaceIndex int ,
			 @UAName nvarchar(400),
			 @UANodeClass_ID int,
			 @UATypeNode_ID bigint,
			 @UATypeDefinition nvarchar(400),
			 @Parent_UANode_ID bigint,
			 @RefDestinationUANodeClass_ID int,
			 @RefUAReferenceNode_ID int,
			 @RefUAIsForward bit,
			 @AllowSubscription bit,
			 @UANode_ID bigint output
as 
begin


set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted



begin try

--UAReferenceNode_ID=
--Organizes=9131
--HasProperty=9141

		declare @Parent_UANodeID nvarchar(400),	 @RefUAReferenceNodeID nvarchar(400)

		select @RefUAReferenceNodeID= UANodeID from UA_Nodes where UANode_ID= @RefUAReferenceNode_ID

		set @UANode_ID=null		 
		select  @UANode_ID= UANode_ID from UA_Nodes where UAServer_ID=@Server_ID and UANodeID= @UANodeID
		 

		 
		--Добавляем если узел не найден
		if (@UANode_ID is null)
		begin

			insert into UA_Nodes
			(UAServer_ID,TIType,UANodeID,
			UABrowseNameNamespaceIndex,UABrowseNameName,UADisplayNameLocale,UADisplayNameText,UANodeClass_ID,UATypeNode_ID,	UATypeDefinition,
			UABaseAttributeDescription,UABaseAttributeWriteMask,UABaseAttributeUserWriteMask,CreateDateTime,ModifyDateTime)
		
			select top 1 
			@Server_ID,@TIType,@UANodeID,	
			@UABrowseNameNamespaceIndex,@UAName,null,@UAName,@UANodeClass_ID,@UATypeNode_ID,@UATypeDefinition,
			@UAName,0,0,getdate(), getdate()
			from UA_Nodes 
			where 
			UAServer_ID=@Server_ID
			and not exists (select top 1 1 from UA_nodes where UAServer_ID=@Server_ID and UANodeID=@UANodeID)
			order by CreateDateTime asc		 

			select  @UANode_ID=SCOPE_IDENTITY()


		end
				
		

		--доабвляем ссылку на родительский объект (если надо)
		if (@Parent_UANode_ID is not null and not exists (select top 1 1 from UA_Refs 
		where  UAServer_ID=@Server_ID 
		and SourceUANode_ID is not null and SourceUANode_ID=@Parent_UANode_ID 
		and DestinationUANode_ID is not null and DestinationUANode_ID=@UANode_ID))
		begin
	
			select @Parent_UANodeID= UANodeID from UA_Nodes where UANode_ID= @Parent_UANode_ID
		
			insert into UA_Refs
			(UAServer_ID,
			SourceUANode_ID,SourceUANodeID,
			DestinationUANode_ID,DestinationUANodeID,DestinationUANodeClass_ID,
			UAReferenceNode_ID,UAReferenceType,UAIsForward,UATypeNode_ID,UATypeId,CreateDateTime,ModifyDateTime)
			values (
			@Server_ID,	
			@Parent_UANode_ID,	@Parent_UANodeID,
			@UANode_ID, @UANodeID, @RefDestinationUANodeClass_ID,
			@RefUAReferenceNode_ID,	@RefUAReferenceNodeID,	@RefUAIsForward,	NULL,	'i=518',	getdate(), getdate())

		end
				
		

		--добавление в подписку		
		if (@AllowSubscription =1 and not exists (select top 1 1 from [UA_Servers_Subscriptions_Content] 
		where  [UANode_ID]=@UANode_ID ))
		begin


			DECLARE @subscribtion_id1 uniqueidentifier 
			SET @subscribtion_id1 = (SELECT top 1 [UAServerSubscription_ID] FROM [UA_Servers_Subscriptions_Content]
			GROUP BY [UAServerSubscription_ID] ORDER by count(*))
	

			INSERT INTO [UA_Servers_Subscriptions_Content]( [UAServerSubscription_ID]
			,[UANode_ID]
			,[SamplingInterval]
			,[QueueSize]
			,[DiscardOldest]
			,[CreateDateTime])
			SELECT top 1 @subscribtion_id1 as [UAServerSubscription_ID]
			,@UANode_ID as [UANode_ID]
			, -1 as [SamplingInterval]
			,1000 as [QueueSize]
			,1 [DiscardOldest]
			,GETDATE() AS[CreateDateTime]
		end


		insert into UA_Nodes_Attributes_Variable
		(UANode_ID, 
		Value,	
		UADataTypeNode_ID,	
		UADataType,	
		ValueRank,	
		ArrayDimensions,	
		AccessLevel,	
		UserAccessLevel,	
		MinimumSamplingInterval,	
		Historizing)
		select  distinct
		UA_Nodes.UANode_ID, 
		--UATypeNode_ID,
		--UATypeDefinition,
		--UABrowseNameName,
		uavar.Value,	
		uavar.UADataTypeNode_ID,	
		uavar.UADataType,	
		uavar.ValueRank,	
		uavar.ArrayDimensions,	
		uavar.AccessLevel,	
		uavar.UserAccessLevel,	
		uavar.MinimumSamplingInterval,	
		uavar.Historizing
		from UA_Nodes
		--выбираем для узла первую попавшуюся запись из UA_Nodes_Attributes_Variable
		--которая соответствует какому нибудь другому узлу с таким же типом
		outer apply (select top 1 
							 Value,	UADataTypeNode_ID,	
							 UADataType,	ValueRank,	
							 ArrayDimensions,	AccessLevel,	
							 UserAccessLevel,	MinimumSamplingInterval,	
							 Historizing
						from 
							UA_Nodes ua 
							join UA_Nodes_Attributes_Variable uav on ua.UANode_ID=uav.UANode_ID 
						where 
						uav.Value is not null
						and ((ua.UATypeDefinition is not null and ua.UATypeDefinition= UA_Nodes.UATypeDefinition)
							or (ua.UATypeNode_ID is not null and ua.UATypeNode_ID=UA_Nodes.UATypeNode_ID))
					) uavar
		where 
		UANodeID like 'ns=3;s=IOA60870%'
		and UANode_ID= @UANode_ID
		--and UA_Nodes.UABrowseNameName like '%мгнов%'
		and UANode_ID not in (select UANode_ID from UA_Nodes_Attributes_Variable)
		and uavar.Value is not null
 

 
						
		
end try
begin catch

	DECLARE @ErrorMessage NVARCHAR(4000);  
    DECLARE @ErrorSeverity INT;  
    DECLARE @ErrorState INT;  
  
    SELECT   
        @ErrorMessage = ERROR_MESSAGE(),  
        @ErrorSeverity = ERROR_SEVERITY(),  
        @ErrorState = ERROR_STATE();  
   
    RAISERROR (@ErrorMessage, -- Message text.  
               @ErrorSeverity, -- Severity.  
               @ErrorState -- State.  
               ); 



end catch



--INSERT INTO TEMP_UANODEINSERT 
--(Server_ID,TIType,UANodeID,UABrowseNameNamespaceIndex,UAName,UANodeClass_ID,UATypeNode_ID,
--UATypeDefinition,Parent_UANode_ID,RefDestinationUANodeClass_ID,RefUAReferenceNode_ID,RefUAIsForward,AllowSubscription,UANode_ID) 

--VALUES (@Server_ID,
--@TIType,
--@UANodeID,
--@UABrowseNameNamespaceIndex,
--@UAName,
--@UANodeClass_ID,
--@UATypeNode_ID,
--@UATypeDefinition,
--@Parent_UANode_ID,
--@RefDestinationUANodeClass_ID,
--@RefUAReferenceNode_ID,
--@RefUAIsForward,
--@AllowSubscription,
--@UANode_ID)

return @UANode_ID

end
GO

grant EXECUTE on usp2_Create_UANode to UserCalcService
go
grant EXECUTE on usp2_Create_UANode to UserDeclarator
go





IF   EXISTS (SELECT * FROM sysobjects WHERE name = N'usp2_Import_UANodes_Master60870' and xtype like 'P' )
 drop procedure usp2_Import_UANodes_Master60870
 GO
create procedure usp2_Import_UANodes_Master60870   
			 @IOA bigint,
			 @ChannelID int , --это Master60870Sector_ID!!!!
			 @ServerID int,
			 @name nvarchar(400),
			 @UATypeNodeID nvarchar(400)
    as 
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted


begin try


 

declare @PS_ID int, @PS_UANode_ID bigint,@PS_UANodeID varchar(200), @PSName nvarchar(200)

declare @IP varchar(200)


declare @Master_ID int, @HardCommChannelID int


if (exists (select top 1 1 from Expl_DBVersion where DBVersion<=2583))
begin
	select 
	@PS_ID=Dict_PS.PS_ID, @PSName=Dict_PS.StringName , @IP= Hard_CommChannels.IPAddress,
	@Master_ID=Hard_Master60870_Sectors.Master60870_ID, @HardCommChannelID= Hard_CommChannels.CommChannel_ID
	from Hard_CommChannels 
	join Dict_PS on Hard_CommChannels.PS_ID = Dict_PS.PS_ID
	join Hard_Master60870CommChannels_Links on Hard_Master60870CommChannels_Links.CommChannel_ID= Hard_CommChannels.CommChannel_ID
	join Hard_Master60870_Sectors on Hard_Master60870CommChannels_Links.Master60870_ID=Hard_Master60870_Sectors.Master60870_ID
	where
	 Hard_CommChannels.CommChannel_ID =@ChannelID
end
else
begin
	--после 2584 версии это Master60870Sector_ID так как раньше были ошибки
	select 
	@PS_ID=Dict_PS.PS_ID, @PSName=Dict_PS.StringName , @IP= Hard_CommChannels.IPAddress,
	@Master_ID=Hard_Master60870_Sectors.Master60870_ID, @HardCommChannelID= Hard_CommChannels.CommChannel_ID
	from Hard_CommChannels 
	join Dict_PS on Hard_CommChannels.PS_ID = Dict_PS.PS_ID
	join Hard_Master60870CommChannels_Links on Hard_Master60870CommChannels_Links.CommChannel_ID= Hard_CommChannels.CommChannel_ID
	join Hard_Master60870_Sectors on Hard_Master60870CommChannels_Links.Master60870_ID=Hard_Master60870_Sectors.Master60870_ID
	where
	 Hard_Master60870_Sectors.Master60870Sector_ID =@ChannelID

end



--находим корневой узел = 'ОБЪЕКТЫ'
declare @rootUANodeID varchar(200)  = 'ns=2;s=Telescope.XMLDB.Dict_PS'
declare @rootUANode_ID bigint					
select @rootUANode_ID= UANode_ID from  UA_Nodes where  UAServer_ID=@ServerID and UANodeID= @rootUANodeID


--находим/добавляем ПС
set @PS_UANodeID='ns=3;s=XML.Dict_PS.'+convert(varchar,isnull(@PS_ID,-1))

SET @PS_UANode_ID =  (SELECT TOP 1 UANode_ID FROM UA_Nodes WHERE UANodeID = @PS_UANodeID)

IF @PS_UANode_ID IS NULL
BEGIN		

exec [usp2_Create_UANode]
@ServerID,11,@PS_UANodeID,
3,
@PSName,
2,
7880,
'i=63',
@rootUANode_ID,
2,
9131,
1,
0,
 @PS_UANode_ID  output

 END
  
--находим/добавляем канал связи

declare @HardCommChannel_UANode_ID bigint, @HardCommChannel_UANodeID varchar(200)
set  @HardCommChannel_UANodeID='ns=3;s=Master60870.'+convert(varchar,@Master_ID)

SET @HardCommChannel_UANode_ID =  (SELECT TOP 1 UANode_ID FROM UA_Nodes WHERE UANodeID = @HardCommChannel_UANodeID)

if (@HardCommChannel_UANode_ID IS null)
begin

	exec [usp2_Create_UANode]
	@ServerID,11,@HardCommChannel_UANodeID,
	3,
	@IP,
	2,
	7880,
	'i=63',
	@PS_UANode_ID,
	2,
	9131,
	1,
	0,
	@HardCommChannel_UANode_ID  output
	

	





end



			--метод и состояние связи на @Master_ID добавляется судя по всему, остальные события на сектор

			declare @Node_ID bigint, @UANodeId nvarchar(400)
			set @UANodeId= 'ns=3;s=Master60870.'+convert(varchar,isnull(@Master_ID,-1))+'.Method'

			 SET @Node_ID = (SELECT  TOP 1 UANode_ID FROM UA_Nodes WHERE UANodeID = @UANodeId) 
			 IF (@Node_ID IS NULL) 
			 begin

					exec [usp2_Create_UANode]
					@ServerID,11,@UANodeId,
					3,
					'Метод',
					4,
					null,
					'i=0',
					@HardCommChannel_UANode_ID,
					4,
					9131,
					1,
					0,
					@Node_ID  output




			end


			-- добавляем атрибуты переменной 

			insert into UA_Nodes_Attributes_Variable
		(UANode_ID, 
		Value,	
		UADataTypeNode_ID,	
		UADataType,	
		ValueRank,	
		ArrayDimensions,	
		AccessLevel,	
		UserAccessLevel,	
		MinimumSamplingInterval,	
		Historizing)
		select  distinct
		UA_Nodes.UANode_ID, 
		--UATypeNode_ID,
		--UATypeDefinition,
		--UABrowseNameName,
		uavar.Value,	
		uavar.UADataTypeNode_ID,	
		uavar.UADataType,	
		uavar.ValueRank,	
		uavar.ArrayDimensions,	
		uavar.AccessLevel,	
		uavar.UserAccessLevel,	
		uavar.MinimumSamplingInterval,	
		uavar.Historizing
		from UA_Nodes
		--выбираем для узла первую попавшуюся запись из UA_Nodes_Attributes_Variable
		--которая соответствует какому нибудь другому узлу с таким же типом
		outer apply (select top 1 
							 Value,	UADataTypeNode_ID,	
							 UADataType,	ValueRank,	
							 ArrayDimensions,	AccessLevel,	
							 UserAccessLevel,	MinimumSamplingInterval,	
							 Historizing
						from 
							UA_Nodes ua 
							join UA_Nodes_Attributes_Variable uav on ua.UANode_ID=uav.UANode_ID 
						where 
						uav.Value is not null
						and ((ua.UATypeDefinition is not null and ua.UATypeDefinition= UA_Nodes.UATypeDefinition)
							or (ua.UATypeNode_ID is not null and ua.UATypeNode_ID=UA_Nodes.UATypeNode_ID))
					) uavar
		where 
		UANodeID like '%.LinkState%'
		and UANode_ID= @Node_ID
		--and UA_Nodes.UABrowseNameName like '%мгнов%'
		and UANode_ID not in (select UANode_ID from UA_Nodes_Attributes_Variable)
		and uavar.Value is not null

			
			IF(SELECT TOP 1 FreeHierItem_ID FROM Dict_FreeHierarchyTree_Description WHERE Dict_FreeHierarchyTree_Description.UANode_ID = @Node_ID) IS NULL
		
		    BEGIN
			exec [dbo].[usp2_Create_UANode_FreeItem] 
					 @PS_ID  , @Node_ID ,  @UANodeId  , @PSName , 'Метод'  


			declare @TempUAID bigint=0

			if (@Node_ID is not null)
			begin
								 
					set @UANodeId =	'ns=3;s=Master60870.'+convert(varchar,isnull(@Master_ID,-1))+'.MethodOA'
					
					
					exec [usp2_Create_UANode]
					@ServerID,11,@UANodeId,
					0,
					'OutputArguments',
					2,
					7881,
					'i=68',
					@Node_ID,
					2,
					9141,
					1,
					1,
					@TempUAID output

					
					set @UANodeId =	'ns=3;s=Master60870.'+convert(varchar,isnull(@Master_ID,-1))+'.MethodIA'

					exec [usp2_Create_UANode]
					@ServerID,11,@UANodeId,
					0,
					'InputArguments',
					2,
					7881,
					'i=68',
					@Node_ID,
					2,
					9141,
					1,
					1,
					@TempUAID output
					
					
			end

			END
			--Состояние связи проверяем/добавляем
			set @UANodeId = 'ns=3;s=Master60870.'+convert(varchar,isnull(@Master_ID,-1))+'.LinkState'

			 SET @TempUAID = (SELECT  TOP 1 UANode_ID FROM UA_Nodes WHERE UANodeID = @UANodeId) 
			 IF (@TempUAID IS NULL) 
			 begin
				  exec [usp2_Create_UANode]
				@ServerID,11,@UANodeId,
				3,
				'Состояние связи',
				2,
				16824,
				'ns=2;s=LinkStateType',
				@HardCommChannel_UANode_ID,
				2,
				9131,
				1,
				1,
				@TempUAID output
			 end

			

		    IF(SELECT TOP 1 FreeHierItem_ID FROM Dict_FreeHierarchyTree_Description WHERE Dict_FreeHierarchyTree_Description.UANode_ID = @TempUAID) IS NULL
			BEGIN
			exec [dbo].[usp2_Create_UANode_FreeItem] 
					 @PS_ID  , @TempUAID ,  @UANodeId  , @PSName , 'Состояние связи' 

			END
			--сам узел
			declare @UATypeNode_ID bigint

			set @UATypeNodeID =REPLACE(REPLACE(REPLACE(@UATypeNodeID,'ns=2;i=','ns=2;s='),'ns=1;','ns=2;'),'ns=2;i=','ns=2;s=')

			select @UATypeNode_ID= UANode_ID from UA_Nodes where UAServer_ID=@ServerID and UANodeID=@UATypeNodeID
			
--INSERT INTO TEMP_UANODEINSERT 
--(Server_ID,TIType,UANodeID,UABrowseNameNamespaceIndex,UAName,UANodeClass_ID,UATypeNode_ID,
--UATypeDefinition,Parent_UANode_ID,RefDestinationUANodeClass_ID,RefUAReferenceNode_ID,RefUAIsForward,AllowSubscription,UANode_ID) 

--VALUES (NULL,
--NULL,
--NULL,
--NULL,
--NULL,
--NULL,
--@UATypeNode_ID,
--@UATypeNodeID,
--NULL,
--NULL,
--NULL,
--NULL,
--NULL,
--NULL)

	
			set @UANodeId = 'ns=3;s=IOA60870.'+convert(varchar,@ChannelID)+'.'+convert(varchar,@IOA)
				
			exec [usp2_Create_UANode]
			@ServerID,11,@UANodeId,
			3,
			@name,
			2,
			@UATypeNode_ID,
			@UATypeNodeID,
			@HardCommChannel_UANode_ID,
			2,
			9131,
			1,
			1,
			@TempUAID output
			
			exec [dbo].[usp2_Create_UANode_FreeItem] 
					 @PS_ID  , @TempUAID ,  @UANodeId  , @PSName , @name  

	


 
 
end try
begin catch

--на всякий
	SET IDENTITY_INSERT UA_Nodes Off 
 
	DECLARE @ErrorMessage NVARCHAR(4000);  
    DECLARE @ErrorSeverity INT;  
    DECLARE @ErrorState INT;  
  
    SELECT   
        @ErrorMessage = ERROR_MESSAGE(),  
        @ErrorSeverity = ERROR_SEVERITY(),  
        @ErrorState = ERROR_STATE();  
   
    RAISERROR (@ErrorMessage, -- Message text.  
               @ErrorSeverity, -- Severity.  
               @ErrorState -- State.  
               ); 
			   
			
	   

end catch



END

go

grant EXECUTE on usp2_Import_UANodes_Master60870 to UserCalcService
go
grant EXECUTE on usp2_Import_UANodes_Master60870 to UserDeclarator
go


 
 --CREATE TABLE TEMP_UANODEINSERT (
	--		 Server_ID int null,
	--		 TIType int null,
	--		 UANodeID nvarchar(400) null,
	--		 UABrowseNameNamespaceIndex int  null,
	--		 UAName nvarchar(400) null,
	--		 UANodeClass_ID int null,
	--		 UATypeNode_ID bigint null,
	--		 UATypeDefinition nvarchar(400) null,
	--		 Parent_UANode_ID bigint null,
	--		 RefDestinationUANodeClass_ID int null,
	--		 RefUAReferenceNode_ID int null,
	--		 RefUAIsForward bit null,
	--		 AllowSubscription bit null,
	--		 UANode_ID bigint null)

	--		 SELECT * FROM TEMP_UANODEINSERT WHERE TIType IS NULL 

	--		 TRUNCATE TABLE TEMP_UANODEINSERT

		