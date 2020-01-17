if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyType_LoadByParentID')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyType_LoadByParentID
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
--		загрузка деревьев свободных иерархий
--
-- ======================================================================================


create proc[dbo].[usp_FreeHierarchyType_LoadByParentID] 
@sParentHierID NVARCHAR(4000)
AS
BEGIN
	SET NOCOUNT ON

	DECLARE @ParentHierID  AS HIERARCHYID

	IF (isnull(@sParentHierID,'') ='')
		SELECT DISTINCT parent.HierID.ToString() SHierID
					  , parent.HierID
					  , parent.FreeHierTree_ID
					  , parent.StringName
					  , convert(BIT, CASE WHEN child.HierID IS NULL THEN 0 ELSE	1 END) ChildrenAny 
					  , parent.ModuleFilter
		FROM
			Dict_FreeHierarchyTypes parent
			LEFT JOIN Dict_FreeHierarchyTypes child
				ON child.HierID.GetAncestor(1) = parent.HierID
		WHERE
			parent.HierID = HIERARCHYID ::GetRoot() 		
	ELSE
	BEGIN
		SET @ParentHierID = convert(HIERARCHYID, @sParentHierID)
		SELECT DISTINCT parent.HierID.ToString() SHierID
					  , parent.HierID
					  , parent.FreeHierTree_ID
					  , parent.StringName
					  , convert(BIT, CASE WHEN child.HierID IS NULL THEN 0 ELSE	1 END) ChildrenAny  
					  , parent.ModuleFilter
		FROM
			Dict_FreeHierarchyTypes parent
			LEFT JOIN Dict_FreeHierarchyTypes child
				ON child.HierID.GetAncestor(1) = parent.HierID
		WHERE
			parent.HierID.GetAncestor(1) = @sParentHierID  
	END
END
go

   grant exec on usp_FreeHierarchyType_LoadByParentID to UserDeclarator
go