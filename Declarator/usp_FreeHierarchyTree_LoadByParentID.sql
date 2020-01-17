if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyTree_LoadByParentID')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyTree_LoadByParentID
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
--		2013
--
-- Описание:
--
--		Win 
--		eclarator Win 
--
-- ======================================================================================


create proc [dbo].usp_FreeHierarchyTree_LoadByParentID 
(
@sParentHierID nvarchar(4000), @FreeHierTree_ID	 int
)
AS
begin
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	declare @MinLvl int
	select @MinLvl = MIN(Dict_FreeHierarchyTree.HierID.GetLevel())
	from Dict_FreeHierarchyTree
	where 
	FreeHierTree_ID=@FreeHierTree_ID

	if isnull(@sParentHierID,'') =''
	set @sParentHierID = null
	DECLARE @ParentHierID  as hierarchyid
	set @ParentHierID = convert(HIERARCHYID,@sParentHierID)
	--возвращаем новую запись
	select distinct Dict_FreeHierarchyTree.HierID.ToString() SHierID,	
	Dict_FreeHierarchyTree.FreeHierTree_ID,
	Dict_FreeHierarchyTree.HierID, 
	Dict_FreeHierarchyTree.HierLevel, 
	Dict_FreeHierarchyTree.FreeHierItem_ID,
	Dict_FreeHierarchyTree.StringName, 
	Dict_FreeHierarchyTree.FreeHierItemType, 
	Dict_FreeHierarchyTree.Expanded,
	Dict_FreeHierarchyTypes.StringName ParentTypeStringName ,
	Dict_FreeHierarchyTree_Description.HierLev1_ID,
	Dict_FreeHierarchyTree_Description.HierLev2_ID,
	Dict_FreeHierarchyTree_Description.HierLev3_ID,
	Dict_FreeHierarchyTree_Description.PS_ID,
	Dict_FreeHierarchyTree_Description.TI_ID,
	Dict_FreeHierarchyTree_Description.Formula_UN,
	Dict_FreeHierarchyTree_Description.Section_ID,
	Dict_FreeHierarchyTree_Description.TP_ID,
	Dict_FreeHierarchyTree_Description.USPD_ID,
	Dict_FreeHierarchyTree_Description.XMLSystem_ID,
	Dict_FreeHierarchyTree_Description.JuridicalPersonContract_ID,
	Dict_FreeHierarchyTree_Description.JuridicalPerson_ID,
	Dict_FreeHierarchyTree_Description.DistributingArrangement_ID,
	Dict_FreeHierarchyTree_Description.BusSystem_ID,
	Dict_FreeHierarchyTree_Description.IncludeObjectChildren,
	Dict_FreeHierarchyTree_Description.SqlSelectString_ID,
	Dict_FreeHierarchyTree_Description.UANode_ID,
	Dict_FreeHierarchyTree_Description.OurFormula_UN,
	Dict_FreeHierarchyTree_Description.ForecastObject_UN,
	Dict_FreeHierarchySQL.SqlSelectString,	
	convert(bit,CASE WHEN child.HierID IS NULL then 0 ELSE 1 end) ChildrenAny
	from dbo.Dict_FreeHierarchyTree 
	join  Dict_FreeHierarchyTypes on Dict_FreeHierarchyTypes.FreeHierTree_ID=Dict_FreeHierarchyTree.FreeHierTree_ID
	left join Dict_FreeHierarchyTree child ON   child.HierID.GetAncestor(1) =Dict_FreeHierarchyTree.HierID and child.FreeHierTree_ID= @FreeHierTree_ID
	left JOIN Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree.FreeHierItem_ID= Dict_FreeHierarchyTree_Description.FreeHierItem_ID
	left JOIN Dict_FreeHierarchySQL on Dict_FreeHierarchySQL.SqlSelectString_ID= Dict_FreeHierarchyTree_Description.SqlSelectString_ID
	where 
	(@sParentHierID is null
	and Dict_FreeHierarchyTree.HierID.GetLevel()=@MinLvl and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID	)
	or
	(Dict_FreeHierarchyTree.HierID.GetAncestor(1) =@ParentHierID  and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID)		
END
go
   grant exec on usp_FreeHierarchyTree_LoadByParentID to UserDeclarator
go