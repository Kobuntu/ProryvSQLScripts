if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyTree_GetBranch')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyTree_GetBranch
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2018
--
-- Описание:
--
--		возвращаем ветку дерева для родительского объекта
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchyTree_GetBranch]
(
	@userId varchar(22),
	@parentId int,
	@treeID int,
	@isReadOnlyHaveSeeRight bit
)

returns @objectTable TABLE
(
 TreeId int,
 HierId int, 
 DbHierItemType smallint,
 Expanded bit,
 FreeHierIcon_ID int NULL,
 IncludeObjectChildren bit,
 SortNumber int NULL,
 Id varchar(22),
 ObjectTypeName varchar(255),
 HierSeeRights tinyint NULL,
 DbRights nvarchar(max)
)
AS

BEGIN
	--set nocount on
	--set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	--set numeric_roundabort off
	--set transaction isolation level read uncommitted

	declare @rightId uniqueidentifier;
	set @rightId = '6D95CECF-327A-408b-96E7-AF8EF27C0F64'; --Идентификатор прав на просмотр

	declare @hierId hierarchyid, @minHielLevel int;

	if (@parentId is not null) begin

		set @hierId = (select top 1 HierID from Dict_FreeHierarchyTree where FreeHierTree_ID = @treeID and FreeHierItem_ID = @parentId);

	end else begin
		--Если узлы не заданы, то возвращаем только рутовые
		set @minHielLevel = (select MIN(HierLevel) from Dict_FreeHierarchyTree WHERE FreeHierTree_ID= @treeID)
	end
	
	insert into @objectTable
	select t.*, dbo.usf2_UserHaveRights(@userId, t.Id,t.ObjectTypeName) from 
	(
		select distinct
		t.FreeHierTree_ID as TreeId,
		t.FreeHierItem_ID as HierId,
		t.FreeHierItemType,
		t.Expanded,
		t.FreeHierIcon_ID,
		ISNULL(d.IncludeObjectChildren, 0) as IncludeObjectChildren,
		--d.SqlSelectString_ID,
		t.SortNumber,
		--t.HierLevel
		f.[Object_ID] as Id,
		f.[ObjectTypeName],
		dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, f.[Object_ID], f.[ObjectTypeName], t.FreeHierItemType, @treeID, HierID) as HierObjectRights  --Права на просмотр собственные(1) или у дочернего(2)
		from Dict_FreeHierarchyTree t 
		left join Dict_FreeHierarchyTree_Description d on t.FreeHierItem_ID=d.FreeHierItem_ID 
		cross apply usf2_FreeHierarchyTreeDescriptionTypeAndId(d.HierLev1_ID, d.HierLev2_ID, d.HierLev3_ID, d.PS_ID, d.TI_ID, d.Formula_UN, d.Section_ID, d.TP_ID, d.USPD_ID, d.XMLSystem_ID, 
				d.JuridicalPerson_ID, d.JuridicalPersonContract_ID, d.DistributingArrangement_ID, d.BusSystem_ID, d.UANode_ID, d.OurFormula_UN, d.ForecastObject_UN, d.FreeHierItem_ID) f
		where 
		t.FreeHierTree_ID= @treeID 
		and ((@parentId is null and t.HierLevel = @minHielLevel) OR t.HierID.GetAncestor(1)= @hierId)
		--Контроль целостности
		and 
		(
			t.FreeHierItemType = 0 or
			  (t.FreeHierItemType = 1 and d.HierLev1_ID is not null) or
			  (t.FreeHierItemType = 2 and d.HierLev2_ID is not null) or
			  (t.FreeHierItemType = 3 and d.HierLev3_ID is not null) or
			  (t.FreeHierItemType = 4 and d.PS_ID is not null) or
			  (t.FreeHierItemType = 5 and d.TI_ID is not null and (select top 1 Deleted from Info_TI where TI_ID = d.TI_ID) <> 1) or
			  (t.FreeHierItemType = 6 and d.Formula_UN is not null) or
			  (t.FreeHierItemType = 7 and d.Section_ID is not null) or
			  (t.FreeHierItemType = 8 and d.TP_ID is not null) or
			  (t.FreeHierItemType = 9 and d.USPD_ID is not null) or
			  (t.FreeHierItemType = 12 and d.XMLSystem_ID is not null) or
			  (t.FreeHierItemType = 13 and d.JuridicalPerson_ID is not null) or
			  (t.FreeHierItemType = 10 and d.JuridicalPersonContract_ID is not null) or
			  (t.FreeHierItemType = 18 and d.DistributingArrangement_ID is not null) or
			  (t.FreeHierItemType = 19 and d.BusSystem_ID is not null) or
			  (t.FreeHierItemType = 23 and d.UANode_ID is not null) or
			  (t.FreeHierItemType = 29 and d.ForecastObject_UN is not null and (select top 1 IsDeleted from Forecast_Objects where ForecastObject_UN = d.ForecastObject_UN) <> 1) or
			  (t.FreeHierItemType = 14 and d.OurFormula_UN is not null) 
		)
	)
	t 
	where @isReadOnlyHaveSeeRight = 0 or HierObjectRights > 0
	order by case when t.SortNumber is null then 1 else 0 end, t.SortNumber
	
	RETURN
END
go
grant select on usf2_FreeHierarchyTree_GetBranch to [UserCalcService]
go