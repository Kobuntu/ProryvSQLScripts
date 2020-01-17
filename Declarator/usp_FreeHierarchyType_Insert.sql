if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyType_Insert')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyType_Insert
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
--		добавление дерева свободных иерархий
--
-- ======================================================================================


create proc[dbo].[usp_FreeHierarchyType_Insert]
     @sParentHierID NVARCHAR(4000), @StringName NVARCHAR(255), @ModuleFilter bigint 
AS
BEGIN
SET NOCOUNT ON

	DECLARE @ParentHierID  AS HIERARCHYID, @LastNode  AS HIERARCHYID, @NewNode   AS HIERARCHYID, @MaxID INT

	IF @sParentHierID='' SET @sParentHierID = NULL
	SET @ParentHierID = CONVERT(HIERARCHYID, @sParentHierID)

	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;	
	BEGIN TRANSACTION;

		SELECT @LastNode = MAX(HierID)	FROM Dict_FreeHierarchyTypes WHERE HierID.GetAncestor(1) = @ParentHierID;

		SELECT @MaxID = MAX(FreeHierTree_ID)
		FROM Dict_FreeHierarchyTypes;

		SET @MaxID = ISNULL(@MaxID, 0) + 1

		SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);
		 
		IF (@ParentHierID is null) SET @NewNode = HIERARCHYID ::GetRoot()
		
		INSERT INTO dbo.Dict_FreeHierarchyTypes (HierID, FreeHierTree_ID, StringName, ModuleFilter)
		VALUES (@NewNode, @MaxID, @StringName,@ModuleFilter);

	COMMIT;
	
	SELECT DISTINCT parent.HierID.ToString() SHierID
					, parent.HierID
					, parent.FreeHierTree_ID
					, parent.StringName
					, CONVERT(BIT, CASE WHEN child.HierID IS NULL THEN 0 ELSE	1 END) ChildrenAny,
					parent.ModuleFilter  
	FROM
		Dict_FreeHierarchyTypes parent
		LEFT JOIN Dict_FreeHierarchyTypes child
			ON child.HierID.GetAncestor(1) = parent.HierID
	WHERE
		parent.HierID=@NewNode 
	
end
go

   grant exec on usp_FreeHierarchyType_Insert to UserDeclarator
go