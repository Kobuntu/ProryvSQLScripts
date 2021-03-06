
if exists (select 1
          from sysobjects
          where  id = object_id('usp2_FreeHierarchy_UpdateFreeHierCachSQL')
          and type in ('P','PC'))
   drop procedure usp2_FreeHierarchy_UpdateFreeHierCachSQL
go

--Обновляем тип
--Удаляем если есть
IF EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'FreeHierId' AND ss.name = N'dbo') 
drop type FreeHierId
go

-- Пересоздаем заново
CREATE TYPE [dbo].[FreeHierId] AS TABLE(
	[ID] [int] NOT NULL,
	[TypeHierarchy] [tinyint] NOT NULL,
	[StringId] [varchar](22) NULL,
	[FreeHierItemId] [int] NULL,
	ParentID int NULL,
	ParentTypeHierarchy int NULL,
	ParentFreeHierItemId int NULL,
	[Path] [varchar](1000) NULL
)
GO

grant EXECUTE on TYPE::FreeHierId to [UserCalcService]
grant EXECUTE on TYPE::FreeHierId to [UserSlave61968Service]
grant EXECUTE on TYPE::FreeHierId to [UserDeclarator]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2019
--
-- Описание:
--
--		Обновляем информацию в таблице-кэш Dict_FreeHierarchyIncludedObjectChildren для объектов подгружаемых через SQL
--
-- ======================================================================================
create proc [dbo].[usp2_FreeHierarchy_UpdateFreeHierCachSQL]
(
@treeID int, --Идентификатор дерева
@objectIds FreeHierId readonly --Список объектов, по которым обновляем информацию

)
as
begin
	set nocount on

	merge [dbo].[Dict_FreeHierarchyIncludedObjectChildren] as d
	using (
		select @treeID as FreeHierTree_ID, ids.[Path] as ToParentFreeHierPath
		, '/' as ParentHierID
		, ids.typeHierarchy, case when ids.ID > 0 then ids.ID else null end as ID, case when ids.StringId <> '' then ids.StringId else null end as StringId
		, h.StringName as StringName, h.MeterSerialNumber, h.FreeHierItemType
		, ids.ParentID
		, ids.ParentTypeHierarchy  
		, null as ParentName, null as ParentFreeHierItemType --TODO тут непонятно как делать, возможно передавать при вызове
		from @objectIds ids
		cross apply dbo.usf2_FreeHierarchyStandartObject(ids.TypeHierarchy, ids.ID, ids.StringId) h
	) h
	on d.[FreeHierTree_ID] = h.[FreeHierTree_ID]
	and d.[ToParentFreeHierPath] = h.[ToParentFreeHierPath]
	when matched then update set [StringName] = h.[StringName], [MeterSerialNumber] = h.[MeterSerialNumber] -- Пока обновляем эти поля, т.к. они используются для поиска
	when not matched then insert
	([ToParentFreeHierPath]
	,[FreeHierTree_ID]
	,[ParentHierID]
	,[TypeHierarchy]
	,[ID]
	,[StringId]
	,[StringName]
	,[MeterSerialNumber]
	,[FreeHierItemType]
	,[ParentID]
	,[ParentTypeHierarchy]
	,[ParentName]
	,[ParentFreeHierItemType])
	values (h.[ToParentFreeHierPath]
	,h.[FreeHierTree_ID]
	,h.[ParentHierID]
	,h.[TypeHierarchy]
	,h.[ID]
	,h.[StringId]
	,h.[StringName]
	,h.[MeterSerialNumber]
	,h.[FreeHierItemType]
	,h.[ParentID]
	,h.[ParentTypeHierarchy]
	,h.[ParentName]
	,h.[ParentFreeHierItemType]);

	
end
go
   grant EXECUTE on usp2_FreeHierarchy_UpdateFreeHierCachSQL to [UserCalcService]
go
   grant EXECUTE on usp2_FreeHierarchy_UpdateFreeHierCachSQL to UserDeclarator
go