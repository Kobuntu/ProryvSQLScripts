if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UA_FindNode')
          and type in ('P','PC'))
   drop procedure usp2_UA_FindNode
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Август, 2014
--
-- Описание:
--
--		Поиск по дереву OPC
--
-- ======================================================================================
create proc [dbo].[usp2_UA_FindNode]
(
	@searchText nvarchar(max), --Искомое название
	@searchParam nvarchar(100) = null, -- Параметр поиска (по какому полю искать)
	@freeHierTreeId int = null --По какому дереву ищем, если null искать по всем деревьям
)
AS
BEGIN 

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
	@nodeIds dbo.BigintType, --Список идентификаторов узлов
	@isForward bit, -- Раскручиваемое направление (от 1 - ищем зависимые узлы )
	@maxLevel int, -- Максимальный уровень раскрутки
	@isFindFreeHierarchyId bit -- искать описание во free hierarchy деревьях
	 
	select @isForward = 0, @maxLevel = 100, @isFindFreeHierarchyId = 1

	if (@searchParam = 'UANode_ID' and ISNUMERIC(@searchText) = 1) begin
		insert into @nodeIds
		select distinct top 50 UANode_ID from UA_Nodes 
		where UANode_ID like @searchText
	end else if (@searchParam = 'UANodeID') begin
		insert into @nodeIds
		select distinct top 50 UANode_ID from UA_Nodes 
		where UANodeID = @searchText
	end else begin
		insert into @nodeIds
		select distinct top 50 UANode_ID from UA_Nodes 
		where UADisplayNameText like '%' + @searchText +'%' 
	end

	create table #res
	(
		[UANode_ID] bigint NOT NULL, -- Узел
		[UAServer_ID] int NOT NULL, --Сервер
		[FromNode_ID] bigint NOT NULL, -- От чего идем
		[ToNode_ID] bigint NOT NULL, -- К чему идем
		[UAIsForward] bit not null, --Информативно был переворот или нет (не влияет на FromNode_ID и ToNode_ID)
		UAReferenceType int not NULL, -- Тип ссылки
		Lev int NOT NULL, --Уровень вложенности
		[TreePath] nvarchar(max), --Путь до рута
		[ParentIds] varchar(max), --Идентификаторы родителей от рута до объекта,
	);

	insert into #res
	exec usp2_UA_Refs @nodeIds, @isForward, 100

	select n.UANode_ID, n.UAServer_ID, isnull(r.TreePath, rm.TreePath) as UATreePath,
	isnull(r.ToNode_ID, isnull(rm.ToNode_ID, n.UANode_ID)) as FD_UANode_ID, FreeHierItem_ID, FreeHierTree_ID, dbo.usf2_FreeHierarchy_GetFullStringPath(FreeHierItem_ID, '\', FreeHierTree_ID, 0, 0, 1) as FreeHierarchyTreePath,
	n.UADisplayNameText, n.TIType, n.[UABrowseNameName], n.[UANodeClass_ID], n.[UABaseAttributeDescription], n.[UATypeNode_ID], 
	(select top 1 ServerName from UA_Servers where UAServer_ID = n.UAServer_ID) + '\' + rm.TreePath as UAFullTreePath, 
	isnull(r.ParentIds, rm.ParentIds) ParentIds
	from @nodeIds ids
	join UA_Nodes n on n.UANode_ID = ids.Id
	outer apply
	(
		select distinct r.TreePath, r.ToNode_ID, r.FromNode_ID, fd.FreeHierItem_ID, ft.FreeHierTree_ID, r.ParentIds from #res r 
		join Dict_FreeHierarchyTree_Description fd on fd.UANode_ID = r.ToNode_ID
		join Dict_FreeHierarchyTree ft  on ft.FreeHierItem_ID = fd.FreeHierItem_ID
		where r.UANode_ID = n.UANode_ID
	) r
	outer apply
	(
		select top 1 r.* from #res r 
		where r.UANode_ID = n.UANode_ID
		order by Lev desc
	) rm
	where @freeHierTreeId is null or r.FreeHierTree_ID = @freeHierTreeId or @freeHierTreeId = -1001
	order by ids.Id, r.FromNode_ID
	 

	drop table #res
end
go
   grant EXECUTE on usp2_UA_FindNode to [UserCalcService]
go