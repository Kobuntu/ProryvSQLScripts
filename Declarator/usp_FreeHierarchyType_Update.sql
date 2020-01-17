if exists (select 1
          from sysobjects
          where  id = object_id('usp_FreeHierarchyType_Update')
          and type in ('P','PC'))
   drop procedure usp_FreeHierarchyType_Update
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
--		изменение дерева свободных иерархий
--
-- ======================================================================================


create proc [dbo].[usp_FreeHierarchyType_Update] 
@sHierID nvarchar(4000), @StringName nvarchar(255), @ModuleFilter bigint 
as
begin
SET NOCOUNT ON

	DECLARE @HierID  as hierarchyid
	SET @HierID = convert(HIERARCHYID, @sHierID)
	
	UPDATE Dict_FreeHierarchyTypes
	SET
		StringName = @StringName, ModuleFilter= @ModuleFilter
	WHERE
		HierID = @HierID

END
go

   grant exec on usp_FreeHierarchyType_Update to UserDeclarator
go