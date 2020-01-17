if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyTree_Insert')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyTree_Insert
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
--		Опистаель Win
--
-- ======================================================================================


create proc [dbo].[usp_FreeHierarchyTree_Insert] 
(
@sParentHierID nvarchar(4000),
@FreeHierTree_ID	int,
@FreeHierItem_ID	int,
@StringName	nvarchar(255),
@FreeHierItemType	tinyint,
@Expanded	bit,
@ObjectStringID nvarchar(255),
@IncludeObjectChildren bit,
@SqlSelectString_ID int
)
AS
BEGIN
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	

IF (@SqlSelectString_ID=0) SET @SqlSelectString_ID = NULL
	DECLARE 
	@ParentHierID  as hierarchyid, 
	@LastNode  as hierarchyid, 
	@NewNode   as hierarchyid,
	@MaxFreeHierItem_ID int,
	@ObjectID int
	IF isnull(@ObjectStringID,'')=''
	BEGIN
		SET @ObjectStringID = NULL
		SET @ObjectID = NULL
	END
	IF (@FreeHierItemType<> 6 and @FreeHierItemType<> 14 and @FreeHierItemType<> 29 and isnull(@ObjectStringID,'')!='') SET @ObjectID = convert(INT, @ObjectStringID)
	if @sParentHierID = ''  SET @sParentHierID = NULL
	SET @ParentHierID = convert(HIERARCHYID, @sParentHierID)
	--находим максимальные идентификаторы и сохраняем запись в таблицу
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;	
		BEGIN TRANSACTION;
		SELECT @LastNode = max(HierID)
		FROM Dict_FreeHierarchyTree
		WHERE HierID.GetAncestor(1) = @ParentHierID	AND FreeHierTree_ID = @FreeHierTree_ID; 
		IF (@ParentHierID is null) SET @NewNode = hierarchyid ::GetRoot()
		ELSE SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);
		SELECT @MaxFreeHierItem_ID = max(FreeHierItem_ID)
		FROM Dict_FreeHierarchyTree
		SET @MaxFreeHierItem_ID = isnull(@MaxFreeHierItem_ID, 0) + 1
		INSERT INTO dbo.Dict_FreeHierarchyTree (FreeHierTree_ID
											  , HierID
											  , FreeHierItem_ID
											  , StringName
											  , FreeHierItemType
											  , Expanded)
		VALUES (@FreeHierTree_ID, @NewNode, @MaxFreeHierItem_ID, @StringName, @FreeHierItemType, @Expanded)
	COMMIT; 
	--добавляем описание
	IF NOT EXISTS (SELECT 1 FROM Dict_FreeHierarchyTree_Description	 WHERE	 FreeHierItem_ID = @MaxFreeHierItem_ID) 
		INSERT INTO dbo.Dict_FreeHierarchyTree_Description 
					(FreeHierItem_ID
					, IncludeObjectChildren
					, HierLev1_ID
					, HierLev2_ID
					, HierLev3_ID
					, PS_ID
					, TI_ID
					, Formula_UN
					, Section_ID
					, TP_ID
					, USPD_ID	
					,XMLSystem_ID	
					, JuridicalPersonContract_ID
					, JuridicalPerson_ID
					, DistributingArrangement_ID
					, BusSystem_ID
					, UANode_ID			
					,OurFormula_UN		
					,ForecastObject_UN							
					, SqlSelectString_ID)
			VALUES (@MaxFreeHierItem_ID,
					 @IncludeObjectChildren, 
					 CASE WHEN @FreeHierItemType = 1 THEN @ObjectID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 2 THEN @ObjectID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 3 THEN @ObjectID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 4 THEN @ObjectID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 5 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 6 THEN @ObjectStringID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 7 THEN @ObjectID ELSE NULL END, 
					 CASE WHEN @FreeHierItemType = 8 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 9 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 12 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 10 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 13 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 18 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 19 THEN @ObjectID ELSE NULL END,
					 CASE WHEN @FreeHierItemType = 23 THEN @ObjectID ELSE NULL END,					 
					 CASE WHEN @FreeHierItemType = 14 THEN @ObjectStringID ELSE NULL END, 		 
					 CASE WHEN @FreeHierItemType = 29 THEN @ObjectStringID ELSE NULL END, 
					 @SqlSelectString_ID)
		--возвращаем новую запись
		SELECT distinct Dict_FreeHierarchyTree.HierID.ToString() SHierID
			 , Dict_FreeHierarchyTree.FreeHierTree_ID
			 , Dict_FreeHierarchyTree.HierID
			 , Dict_FreeHierarchyTree.HierLevel
			 , Dict_FreeHierarchyTree.FreeHierItem_ID
			 , Dict_FreeHierarchyTree.StringName
			 , Dict_FreeHierarchyTree.FreeHierItemType
			 , Dict_FreeHierarchyTree.Expanded
			 , Dict_FreeHierarchyTypes.StringName ParentTypeStringName
			 , Dict_FreeHierarchyTree_Description.HierLev1_ID
			 , Dict_FreeHierarchyTree_Description.HierLev2_ID
			 , Dict_FreeHierarchyTree_Description.HierLev3_ID
			 , Dict_FreeHierarchyTree_Description.PS_ID
			 , Dict_FreeHierarchyTree_Description.TI_ID
			 , Dict_FreeHierarchyTree_Description.Formula_UN
			 , Dict_FreeHierarchyTree_Description.Section_ID
			 , Dict_FreeHierarchyTree_Description.TP_ID
			 , Dict_FreeHierarchyTree_Description.USPD_ID			 
			 , Dict_FreeHierarchyTree_Description.XMLSystem_ID	 
			 , Dict_FreeHierarchyTree_Description.JuridicalPersonContract_ID	 
			 , Dict_FreeHierarchyTree_Description.JuridicalPerson_ID
			 , Dict_FreeHierarchyTree_Description.DistributingArrangement_ID
			 , Dict_FreeHierarchyTree_Description.BusSystem_ID
			 , Dict_FreeHierarchyTree_Description.IncludeObjectChildren
			 , Dict_FreeHierarchyTree_Description.SqlSelectString_ID
			 , Dict_FreeHierarchySQL.SqlSelectString
			 , Dict_FreeHierarchyTree_Description.UANode_ID
			 , Dict_FreeHierarchyTree_Description.OurFormula_UN
			 , Dict_FreeHierarchyTree_Description.ForecastObject_UN
			 , convert(BIT, CASE WHEN child.HierID IS NULL THEN 0 ELSE 1 END) ChildrenAny
		FROM
			dbo.Dict_FreeHierarchyTree
			JOIN Dict_FreeHierarchyTypes
				ON Dict_FreeHierarchyTypes.FreeHierTree_ID = Dict_FreeHierarchyTree.FreeHierTree_ID
			LEFT JOIN Dict_FreeHierarchyTree child
				ON child.HierID.GetAncestor(1) = Dict_FreeHierarchyTree.HierID AND child.FreeHierTree_ID = @FreeHierTree_ID
			LEFT JOIN Dict_FreeHierarchyTree_Description
				ON Dict_FreeHierarchyTree.FreeHierItem_ID = Dict_FreeHierarchyTree_Description.FreeHierItem_ID
			LEFT JOIN Dict_FreeHierarchySQL
				ON Dict_FreeHierarchySQL.SqlSelectString_ID = Dict_FreeHierarchyTree_Description.SqlSelectString_ID
		WHERE
			Dict_FreeHierarchyTree.hierID = @NewNode
			AND Dict_FreeHierarchyTree.FreeHierTree_ID = @FreeHierTree_ID
END
go
   grant exec on usp_FreeHierarchyTree_Insert to UserDeclarator
go