set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchyTreeDescriptionReturnHierId')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchyTreeDescriptionReturnHierId
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июнь 2018
--
-- Описание:
--
--		Возвращаем идентификатор объекта на дереве по его типу и идентификатору
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchyTreeDescriptionReturnHierId]
(
	@objectId varchar(255),
	@objectTypeName varchar(255),
	@treeID int,
	@freeHierItemId int = null
)
returns hierarchyid

AS
BEGIN

	if (@treeID is not null and @freeHierItemId is not null) begin
		return (select top 1 HierID from Dict_FreeHierarchyTree t
		join [dbo].[Dict_FreeHierarchyTree_Description] d on d.FreeHierItem_ID = t.FreeHierItem_ID
		where t.FreeHierTree_ID = @treeID and t.FreeHierItem_ID = @freeHierItemId)
	end else if (@freeHierItemId is not null) begin
		return (select top 1 HierID from Dict_FreeHierarchyTree t
		join [dbo].[Dict_FreeHierarchyTree_Description] d on d.FreeHierItem_ID = t.FreeHierItem_ID
		where t.FreeHierItem_ID = @freeHierItemId)
	end 

	--Если узел на дереве не задан явно, ищем первый попавшийся на котором этот объект находится
	return (select top 1 HierID from Dict_FreeHierarchyTree t
	join [dbo].[Dict_FreeHierarchyTree_Description] d on d.FreeHierItem_ID = t.FreeHierItem_ID
		where (@treeID is null or t.FreeHierTree_ID = @treeID) and
		@objectId = case when @objectTypeName = 'Dict_PS_' then convert(varchar(200),PS_ID)
		when @objectTypeName = 'Dict_HierLev3' then convert(varchar(200),HierLev3_ID)
		when @objectTypeName = 'Dict_HierLev2_' then convert(varchar(200),HierLev2_ID)
		when @objectTypeName = 'Dict_HierLev1_' then convert(varchar(200),HierLev1_ID)
		when @objectTypeName = 'Info_Section_List'  then convert(varchar(200),Section_ID)
		when @objectTypeName = 'UANode' then convert(varchar(200),UANode_ID)
		when @objectTypeName = 'Dict_JuridicalPersons_Contracts' then convert(varchar(200),JuridicalPersonContract_ID)
		when @objectTypeName = 'Dict_JuridicalPersons' then convert(varchar(200),JuridicalPerson_ID)
		when @objectTypeName = 'Expl_XML_System_List' then convert(varchar(200),XMLSystem_ID)
		when @objectTypeName = 'Dict_BusSystem' then convert(varchar(200),BusSystem_ID)
		when @objectTypeName = 'Forecast_Objects' then convert(varchar(200),ForecastObject_UN)
		when @objectTypeName = 'Info_TI' then convert(varchar(200),TI_ID)
		when @objectTypeName = 'Info_TP2' then convert(varchar(200),TP_ID)
		when @objectTypeName = 'Dict_DistributingArrangement' then convert(varchar(200),DistributingArrangement_ID)
		when @objectTypeName = 'Info_Formula_List' then convert(varchar(200),Formula_UN)
		when @objectTypeName = 'Info_TP2_OurSide_Formula_List' then convert(varchar(200),OurFormula_UN)
		when @objectTypeName = 'Hard_USPD' then convert(varchar(200),USPD_ID)
		when @objectTypeName = 'Dict_FreeHierarchyTree' then convert(varchar(200),d.FreeHierItem_ID)
	end)
END
go
grant EXECUTE on usf2_FreeHierarchyTreeDescriptionReturnHierId to [UserCalcService]
go