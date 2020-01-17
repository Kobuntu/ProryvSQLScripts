if exists (select 1
          from sysobjects
          where  id = object_id('usf2_FreeHierarchy_GetFullStringPath')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_FreeHierarchy_GetFullStringPath
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2014
--
-- Описание:
--
--		Возвращаем полный путь в виде строки
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_FreeHierarchy_GetFullStringPath]
(	
	@FreeHierItem_ID int,
	@delimer nvarchar(3),
	@FreeHierTree_ID int,
	@minHierLev int, 
	@isIncludeSelfName bit,
	@isIncludeTreeName bit = 1
)
RETURNS varchar(1000)
AS
begin
declare @fullPath nvarchar(1000)
set @fullPath = '';
with freeHier(HierID, ParentHierID, FreeHierItem_ID, FreeHierTree_ID, FreeHierItemType, StringName, isSelf) as
(
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierItem_ID, f.FreeHierTree_ID, f.FreeHierItemType, f.StringName, cast(1 as bit) as isSelf
	from Dict_FreeHierarchyTree f
	where f.FreeHierItem_ID = @FreeHierItem_ID and f.FreeHierTree_ID = @FreeHierTree_ID
	--Рекурсия
	union all
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierItem_ID, f.FreeHierTree_ID, f.FreeHierItemType, f.StringName, cast(0 as bit)
	from Dict_FreeHierarchyTree f
	join freeHier r on r.ParentHierID = f.HierID and r.FreeHierTree_ID = f.FreeHierTree_ID
	where f.[HierID].GetLevel() >= @minHierLev
)
select @fullPath = ISNULL(case FreeHierItemType 
	when 1 then (select top 1 StringName from Dict_HierLev1 l where l.HierLev1_ID = d.HierLev1_ID)
	when 2 then (select top 1 StringName from Dict_HierLev2 l where l.HierLev2_ID = d.HierLev2_ID)
	when 3 then (select top 1 StringName from Dict_HierLev3 l where l.HierLev3_ID = d.HierLev3_ID)
	when 4 then (select top 1 StringName from Dict_PS l where l.PS_ID = d.PS_ID)
	when 5 then (select top 1 TIName from Info_TI l where l.TI_ID = d.TI_ID)
	when 6 then (select top 1 FormulaName from Info_Formula_List l where l.Formula_UN = d.Formula_UN)
	when 7 then (select top 1 SectionName from Info_Section_List l where l.Section_ID = d.Section_ID)
	when 8 then (select top 1 StringName from Info_TP2 l where l.TP_ID = d.TP_ID)
	when 9 then (select top 1 USPDIPMain from Hard_USPD l where l.USPD_ID = d.USPD_ID)
	when 10 then (select top 1 StringName from Dict_JuridicalPersons_Contracts l where l.JuridicalPersonContract_ID = d.JuridicalPersonContract_ID)
	when 12 then (select top 1 XMLSystemName from Expl_XML_System_ID_List l where l.XMLSystem_ID = d.XMLSystem_ID)
	when 13 then (select top 1 StringName from Dict_JuridicalPersons l where l.JuridicalPerson_ID = d.JuridicalPerson_ID)
	when 18 then (select top 1 StringName from Dict_DistributingArrangement l where l.DistributingArrangement_ID = d.DistributingArrangement_ID)
	when 19 then (select top 1 StringName from Dict_BusSystem l where l.BusSystem_ID = d.BusSystem_ID)
	when 23 then (select top 1 UADisplayNameText from UA_Nodes l where l.UANode_ID = d.UANode_ID)
	else f.StringName
end, f.StringName) + @delimer + @fullPath
from freeHier f
join Dict_FreeHierarchyTree_Description d on d.FreeHierItem_ID = f.FreeHierItem_ID
where @isIncludeSelfName = 1 or (@isIncludeSelfName = 0 and isSelf = 0);

if (@isIncludeTreeName = 1) begin
--Добавляем пути дерева FreeHierarchyTypes
with freeHierTypes(HierID, ParentHierID, FreeHierTree_ID, StringName) as
(
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierTree_ID, f.StringName
	from Dict_FreeHierarchyTypes f
	where f.FreeHierTree_ID = @FreeHierTree_ID
	--Рекурсия
	union all
	select f.HierID, f.HierID.GetAncestor(1), f.FreeHierTree_ID, f.StringName
	from Dict_FreeHierarchyTypes f
	join freeHierTypes r on r.ParentHierID = f.HierID 
)
select @fullPath = ISNULL(StringName, '') + @delimer + @fullPath
from freeHierTypes 
return @fullPath
end 
return @fullPath
end
go
grant EXECUTE on usf2_FreeHierarchy_GetFullStringPath to [UserCalcService]
go