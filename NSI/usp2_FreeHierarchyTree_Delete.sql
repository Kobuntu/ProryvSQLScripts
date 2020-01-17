if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_Delete')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_Delete
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		2014
--
-- Описание:
--
--		удаление объекта и узла в дереве свободных иерархий
--
-- ======================================================================================


create proc [dbo].[usp2_FreeHierarchyTree_Delete] 
(
@FreeHierItem_ID	int,
@FreeHierTree_ID int --передаем ид дерева, т.к. ИД виртуального узла может быть положительным и совпасть с узлом из другого дерева!
)
AS
BEGIN
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
	
declare 
	@sHierID varchar(200), 
	@Objecttype int, 
	@Object_ID int,
	@HierLev1_ID tinyint,
	@HierLev2_ID int,
	@HierLev3_ID int,
	@PS_ID int,
	@TI_ID int,
	@Formula_UN varchar(22),
	@Section_ID int,
	@TP_ID int,
	@USPD_ID int,
	@XMLSystem_ID tinyint,
	@JuridicalPerson_ID int,
	@JuridicalPersonContract_ID int,
	@DistributingArrangement_ID int,
	@BusSystem_ID int,
	@UANode_ID bigint,
	@OurFormula_UN varchar(22),
	@ForecastObject_UN varchar(22),
	@IncludeObjectChildren bit;

select @sHierID = HierID.ToString(),
@Objecttype=FreeHierItemType, 
@Object_ID= case 
when PS_ID is not null and tree.FreeHierItemType=4 then PS_ID
when HierLev3_ID is not null and tree.FreeHierItemType=3 then HierLev3_ID
when HierLev2_ID is not null and tree.FreeHierItemType=2 then HierLev2_ID
when HierLev1_ID is not null and tree.FreeHierItemType=1 then HierLev1_ID
when Section_ID is not null and tree.FreeHierItemType=7 then Section_ID
else null end,
@HierLev1_ID = HierLev1_ID,
@HierLev2_ID = HierLev2_ID,
@HierLev3_ID = HierLev3_ID,
@PS_ID = PS_ID,
@TI_ID = TI_ID,
@Formula_UN = Formula_UN,
@Section_ID = Section_ID,
@TP_ID = TP_ID,
@USPD_ID = USPD_ID,
@XMLSystem_ID = XMLSystem_ID,
@JuridicalPersonContract_ID = JuridicalPersonContract_ID,
@JuridicalPerson_ID = JuridicalPerson_ID,
@DistributingArrangement_ID = DistributingArrangement_ID,
@BusSystem_ID = BusSystem_ID,
@UANode_ID = UANode_ID,
@OurFormula_UN = OurFormula_UN,	
@ForecastObject_UN = ForecastObject_UN,
@IncludeObjectChildren = IncludeObjectChildren
from Dict_FreeHierarchyTree tree
join Dict_FreeHierarchyTree_Description d on d.FreeHierItem_ID=tree.FreeHierItem_ID
where FreeHierTree_ID=@FreeHierTree_ID and tree.FreeHierItem_ID=@FreeHierItem_ID

/*
если узел просто узел то удаляем его как раньше

если он объект
то удаляем объект если нет связейс ТИ и с другими деревьями
а затем удаляем узел

только для уровне1 1-4 и сечений
остальне объекты -удаляем узел а объекты через справочники

*/

--если узел не найден то .. выход
if (@sHierID is null)
begin
  return 1;
end 

BEGIN TRY BEGIN TRANSACTION;

	--никогда будем удаять все дерево сразу!!! тк. могут удалить узел у которого куча дочерних ПС без привязок, 
	--они повиснут в воздухе, потом не разберешься кто есть кто
	--см внизу


	/*
	--НУЖНО ВЫДАВАТЬ ОШИБКУ ТОЛЬКО ЕСЛИ ЕСТЬ ПС КОТОРЫЕ ПОСЛЕ УДАЛЕНИЯ ОСТАНУТСЯ БЕЗ ПРИВЯЗОК
	Т.Е. берем все уадялемые узлы , находим на них ПС без привязок и смотрим чтобы у них были ссылки на другие (не удаляемые ) узлы

	if exists (
	select top 1  1
	FROM Dict_FreeHierarchyTree join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree_Description.FreeHierItem_ID= Dict_FreeHierarchyTree.FreeHierItem_ID
	where HierID.IsDescendantOf(@sHierID) = 1 and FreeHierTree_ID=@FreeHierTree_ID
	and 
	 exists(select 1
				 from Dict_FreeHierarchyTree_Description
				 where FreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID
				 and
					(
					 HierLev1_ID is not null
					 or HierLev2_ID is not null 
					 or HierLev3_ID is not null 
					 or PS_ID is not null 
					 or TI_ID is not null 
					 or Formula_UN is not null 
					 or Section_ID is not null 
					 or TP_ID is not null 
					 or USPD_ID is not null 
					 or XMLSystem_ID is not null 
					 or JuridicalPerson_ID is not null 
					 or JuridicalPersonContract_ID is not null 
					 or SqlSelectString_ID is not null 
					 or DistributingArrangement_ID is not null 
					 or BusSystem_ID is not null 
					 or UANode_ID is not null 
					)
			   )
	)
	begin
		RAISERROR ('Удаление невозможно, так как имеются дочерние узлы. Сначала удалите их%s',16, 1, '');
		return 1;
	end
	*/


	--удаляем йобъект если нет связей
	if @Object_ID is not null begin

		if @Objecttype= 1
		begin
			if (not exists (select 1 from Dict_HierLev2 where HierLev1_ID=@Object_ID)
				and not exists (select 1 from Info_Section_List where isnull(HierLev1_ID,0)=@Object_ID)
				and not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and HierLev1_ID=@Object_ID)
				)		
			begin
				delete from Dict_HierLev1 where HierLev1_ID=@Object_ID
			end
		end
		else if @Objecttype= 2
		begin
			if (not exists (select 1 from Dict_HierLev3 where HierLev2_ID=@Object_ID)
				and not exists (select 1 from Info_Section_List where isnull(HierLev2_ID,0)=@Object_ID)
				and not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and HierLev2_ID=@Object_ID)
				)		
			begin
				delete from Dict_HierLev2 where HierLev2_ID=@Object_ID
			end
		end
		else if @Objecttype= 3
		begin
		if (not exists (select 1 from Dict_PS where HierLev3_ID=@Object_ID)
				and not exists (select 1 from Info_Section_List where isnull(HierLev3_ID,0)=@Object_ID)
				and not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and HierLev3_ID=@Object_ID)
				)		
			begin
				delete from Dict_HierLev3 where HierLev3_ID=@Object_ID
			end
		end
		else if @Objecttype= 4
		begin
		if (not exists (select 1 from Info_TI where PS_ID=@Object_ID)
				and not exists (select 1 from Info_Section_List where isnull(PS_ID,0)=@Object_ID)
				and not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and PS_ID=@Object_ID)
				and not exists (select 1 from Hard_CommChannels where PS_ID=@Object_ID)
				and not exists (select 1 from ExplDoc_DocumentsFiles_To_Objects where isnull(PS_ID,0)=@Object_ID) 
				and not exists (select 1 from Dict_PS_PowerSupply_PS_List where isnull(PS_ID,0)=@Object_ID or isnull(PowerSupplyPS_ID,0)=@Object_ID)
				and not exists (select 1 from Dict_DistributingArrangement where isnull(PS_ID,0)=@Object_ID) 
				and not exists (select 1 from Info_Balance_PS_List_2 where isnull(PS_ID,0)=@Object_ID)  
				)		
			begin		
				delete from Dict_PS where PS_ID=@Object_ID
			end
		end
		else if  @Objecttype= 7
		begin
			--ТП?
			if	not exists (select 1 from Info_Section_Description2 where Section_ID=@Object_ID)
					and not exists (select 1 from ExplDoc_DocumentsFiles_To_Objects where isnull(Section_ID,0)=@Object_ID)
					and not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and Section_ID=@Object_ID)
			begin  
				delete from Info_Section_List where Section_ID=@Object_ID
			end 
		end

		--если ПС не удалилась но у нее нет привязок к уровням 3 и к своб иерархиям, то узел не удаляем, чтобы она не осталась подвешенной в воздухе
		if (@Objecttype=4 and exists (select top 1 1 from Dict_PS where PS_ID= @Object_ID)) begin
			declare @hl3_ID int 
			select @hl3_ID=HierLev3_ID from Dict_PS where PS_ID=@Object_ID
			if (@hl3_ID is null)
			begin
				if  not exists (select 1 from Dict_FreeHierarchyTree_Description where FreeHierItem_ID<> @FreeHierItem_ID  and PS_ID=@Object_ID)
				begin
					RAISERROR ('Удаление невозможно, так как удаляемый объект (ПС, жилой дом) не имеет привязок к структуре%s',16, 1, '');
					return @FreeHierItem_ID;
				end
			end			
		end		
	end

	
			

	--пока разрешаем удалять все
	--ЕСЛИ НЕТ ссылок на скрипты или объекты базы то удаляем пустые узлы
	--if not exists(
	--select top 1  1
	--FROM Dict_FreeHierarchyTree join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree_Description.FreeHierItem_ID= Dict_FreeHierarchyTree.FreeHierItem_ID
	--where HierID.IsDescendantOf(@sHierID) = 1 and FreeHierTree_ID=@FreeHierTree_ID
	--and 
	-- exists(select 1
	--			 from Dict_FreeHierarchyTree_Description
	--			 where FreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID
	--			 and
	--				(
	--				 HierLev1_ID is not null
	--				 or HierLev2_ID is not null 
	--				 or HierLev3_ID is not null 
	--				 or PS_ID is not null 
	--				 or TI_ID is not null 
	--				 or Formula_UN is not null 
	--				 or Section_ID is not null 
	--				 or TP_ID is not null 
	--				 or USPD_ID is not null 
	--				 or XMLSystem_ID is not null 
	--				 or JuridicalPerson_ID is not null 
	--				 or JuridicalPersonContract_ID is not null 
	--				 or SqlSelectString_ID is not null 
	--				 or DistributingArrangement_ID is not null 
	--				 or BusSystem_ID is not null 
	--				 or UANode_ID is not null 
	--				)
	--		   )
	--)

	DECLARE @DeletedFreeHierarchyTree TABLE 
			(
				[FreeHierTree_ID] [dbo].[FREEHIERTREE_ID_TYPE] NOT NULL,
				[HierID] [hierarchyid] NOT NULL,
				[FreeHierItem_ID] [dbo].[FREEHIERITEM_ID_TYPE] NOT NULL,
				[StringName] [nvarchar](255) NULL,
				[FreeHierItemType] [tinyint] NOT NULL,
				[Expanded] [bit] NOT NULL
			)

	DELETE FROM Dict_FreeHierarchyTree 
	OUTPUT DELETED.FreeHierTree_ID, DELETED.HierID, DELETED.FreeHierItem_ID, DELETED.StringName, DELETED.FreeHierItemType, DELETED.Expanded into @DeletedFreeHierarchyTree
	where FreeHierTree_ID=@FreeHierTree_ID and (FreeHierItem_ID= @FreeHierItem_ID OR HierID.IsDescendantOf(@sHierID) = 1)


	--DELETE FROM Dict_FreeHierarchyTree where FreeHierItem_ID= @FreeHierItem_ID and FreeHierTree_ID=@FreeHierTree_ID

	--select * from @DeletedFreeHierarchyTree

	declare @routing nvarchar(4000)

		declare @jsonObject nvarchar(max)
		select @jsonObject= 'Dict_FreeHierarchyTree:'+
				 dbo.usf_ConvertSQL_To_JSON(
				  (SELECT top 1 
					n.*
				   FROM @DeletedFreeHierarchyTree n 
				   --WHERE n.FreeHierItem_ID=@FreeHierItem_ID
				   FOR XML path, root)
				  ) +
				  ',Dict_FreeHierarchyTree_Description:'+
				  dbo.usf_ConvertSQL_To_JSON(
				  (SELECT
				  @FreeHierItem_ID as FreeHierItem_ID,
				  @HierLev1_ID as HierLev1_ID,
						 @HierLev2_ID as HierLev2_ID,
						 @HierLev3_ID as HierLev3_ID,
						 @PS_ID as PS_ID,
						 @TI_ID as TI_ID,
						 @Formula_UN as Formula_UN,
						 @Section_ID as Section_ID,
						 @TP_ID as TP_ID,
						 @USPD_ID as USPD_ID,
						 @XMLSystem_ID as XMLSystem_ID,
						 @JuridicalPersonContract_ID as JuridicalPersonContract_ID,
						 @JuridicalPerson_ID as JuridicalPerson_ID,
						 @DistributingArrangement_ID as DistributingArrangement_ID,
						 @BusSystem_ID as BusSystem_ID,
						 @UANode_ID as UANode_ID,
						 @OurFormula_UN as OurFormula_UN, 	
						 @ForecastObject_UN as ForecastObject_UN,
						 @IncludeObjectChildren as IncludeObjectChildren
				   FOR XML path, root)
				  )

	--print @jsonObject

	set @routing =  'Dict_FreeHierarchyTree.Delete.' + convert(varchar(200),@FreeHierTree_ID)+'.'+@sHierID 
	exec spclr_MQ_TryPostMessage @jsonObject, @routing, null

	COMMIT
	END TRY		
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 16 
		--SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH

select @FreeHierItem_ID

end

go
   grant EXECUTE on usp2_FreeHierarchyTree_Delete to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Delete to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Delete to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTree_Delete to [UserExportService]
go
