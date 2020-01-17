if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTypeTree_Insert')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTypeTree_Insert
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



CREATE proc [dbo].[usp2_FreeHierarchyTypeTree_Insert] 
(
@Parent_ID	int,
@StringName	varchar(255),
@ModuleFilter	bigint
)
AS
BEGIN
set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
IF (@Parent_ID<=0) SET @Parent_ID = NULL
	
	DECLARE 
	@ParentHierID  as hierarchyid, 
	@LastNode  as hierarchyid, 
	@NewNode   as hierarchyid,
	@MaxFreeHierTree_ID int,
	@ObjectID int
	

	--находим максимальные идентификаторы и сохраняем запись в таблицу
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;	
		BEGIN TRANSACTION;
		
		--находим род идентификатор
		select @ParentHierID = HierID 
		from Dict_FreeHierarchyTypes
		where  FreeHierTree_ID=@Parent_ID
		
		--выбираем мин уровень
		declare @MinLvl int
		select @MinLvl = MIN(Dict_FreeHierarchyTypes.HierID.GetLevel())
		from Dict_FreeHierarchyTypes
							
		SELECT @LastNode = max(HierID)
		FROM Dict_FreeHierarchyTypes
		WHERE 
		(@ParentHierID is null
			and Dict_FreeHierarchyTypes.HierID.GetLevel()=@MinLvl )
			or
			(Dict_FreeHierarchyTypes.HierID.GetAncestor(1) =@ParentHierID )		
			
		--если родитель не указан то ставим parent=0
		IF (@ParentHierID is null) SET @ParentHierID = hierarchyid ::GetRoot()
			
		--если корень=0 есть и добваляют еще корень то добавляем с уровнем 1
		if (@LastNode!=@ParentHierID)
			SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);	
		else 
		begin
			SELECT @LastNode = max(HierID)
			FROM Dict_FreeHierarchyTypes
			WHERE 
				(Dict_FreeHierarchyTypes.HierID.GetAncestor(1) =@LastNode )				

			SET @NewNode = @ParentHierID.GetDescendant(@LastNode, NULL);	
		end			
					
		
		SELECT @MaxFreeHierTree_ID = max(FreeHierTree_ID)
		FROM Dict_FreeHierarchyTypes
		SET @MaxFreeHierTree_ID = isnull(@MaxFreeHierTree_ID, 0) + 1
		
		
		INSERT INTO dbo.Dict_FreeHierarchyTypes (FreeHierTree_ID
											  , HierID
											  , StringName
											  , ModuleFilter)
		VALUES (@MaxFreeHierTree_ID, @NewNode,  @StringName,  @ModuleFilter)
	COMMIT; 
		
	select result = @MaxFreeHierTree_ID 
END



go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Insert to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Insert to [UserDeclarator]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Insert to [UserImportService]
go
   grant EXECUTE on usp2_FreeHierarchyTypeTree_Insert to [UserExportService]
go



