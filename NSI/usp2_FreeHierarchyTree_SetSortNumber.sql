if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchyTree_SetSortNumber')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchyTree_SetSortNumber
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- ======================================================================================
-- Автор:
--
--		Александр Карташев
--
-- Дата создания:
--
--		Май, 2017
--
-- Описание:
--
--		Задаем номер сортировки и присваеваем актуальные значения для остальных элеметнов
--
-- ======================================================================================
create proc [dbo].[usp2_FreeHierarchyTree_SetSortNumber]

 @newIndex int  ,
 @FreeHierItem_ID int,
 @FreeHierTree_ID int,
 @ParentFreeHierTree_ID int = null	
as

begin
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	
DECLARE @temp TABLE (RowNumber int, FreeHierTree_ID int, FreeHierItem_ID int,StringName nvarchar(max),SortNumber int null)

DECLARE @OldIndex int;

INSERT INTO @temp SELECT ROW_NUMBER() OVER (order by HierLevel, case when SortNumber is null then 1 else 0 end, SortNumber,StringName) AS RowNumber , FreeHierTree_ID,FreeHierItem_ID,StringName,SortNumber FROM [Dict_FreeHierarchyTree]
WHERE FreeHierTree_ID = @FreeHierTree_ID AND (@ParentFreeHierTree_ID IS NULL OR (HierID.IsDescendantOf((SELECT TOP 1 HierID FROM [Dict_FreeHierarchyTree] WHERE FreeHierItem_ID = @ParentFreeHierTree_ID) ) = 1)) AND HierLevel = (SELECT TOP 1 HierLevel FROM [Dict_FreeHierarchyTree] WHERE FreeHierItem_ID = @FreeHierItem_ID)
 order by HierLevel, case when SortNumber is null then 1 else 0 end, SortNumber,StringName




	 SELECT @OldIndex = (SELECT RowNumber FROM @temp  where  FreeHierItem_ID = @FreeHierItem_ID)
	 
	 
	 --SELECT @OldIndex 
	 --SELECT @newIndex
	



   UPDATE @temp SET RowNumber= RowNumber+1 where RowNumber >= @newIndex  AND RowNumber < @OldIndex
  

     UPDATE @temp SET RowNumber= @newIndex where  FreeHierItem_ID = @FreeHierItem_ID

	 UPDATE @temp SET RowNumber= RowNumber-1 where  FreeHierItem_ID <> @FreeHierItem_ID AND RowNumber = @newIndex

	--  SELECT * FROM @temp ORDER BY RowNumber


 UPDATE Dict_FreeHierarchyTree
 SET SortNumber = (SELECT RowNumber FROM @temp as t WHERE t.FreeHierItem_ID = Dict_FreeHierarchyTree.FreeHierItem_ID) WHERE FreeHierTree_ID = @FreeHierTree_ID AND HierLevel = (SELECT TOP 1 HierLevel FROM [Dict_FreeHierarchyTree] WHERE FreeHierItem_ID = @FreeHierItem_ID)

 

end

go
   grant EXECUTE on usp2_FreeHierarchyTree_SetSortNumber to [UserCalcService]
go

   grant EXECUTE on usp2_FreeHierarchyTree_SetSortNumber to [UserDeclarator]
go








 