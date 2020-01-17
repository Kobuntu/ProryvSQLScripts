if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UserHasRightOrHasChild')
          and type in ('P','PC'))
   drop procedure usp2_UserHasRightOrHasChild
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UserHasRightOrHasChild_Declarator')
          and type in ('P','PC'))
   drop procedure usp2_UserHasRightOrHasChild_Declarator
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UserHasRightOrHasChild')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UserHasRightOrHasChild
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp2_UserHasRight')
          and type in ('P','PC'))
   drop procedure usp2_UserHasRight
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'UserRightId' AND ss.name = N'dbo')
DROP TYPE [dbo].[UserRightId]
-- Пересоздаем заново
CREATE TYPE UserRightId AS TABLE 
(
	Object_ID varchar(255),
	ObjectTypeName varchar(255),
	FreeHierItemId int
)
go
grant EXECUTE on TYPE::UserRightId to [UserCalcService]
go
grant EXECUTE on TYPE::UserRightId to [UserDeclarator]
go
grant EXECUTE on TYPE::UserRightId to [UserImportService]
go
grant EXECUTE on TYPE::UserRightId to [UserExportService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2018
--
-- Описание:
--
--		Проверка конкретного права на конкретный объект и его дочерние объекты
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_UserHasRightOrHasChild]
(
@userId varchar(22),
@rightId uniqueidentifier,
@objectId varchar(255),
@objectTypeName varchar(255),
@freeHierItemType tinyint = null,
@treeID int = null,
@hierId hierarchyid = null
)
RETURNS tinyint
AS
BEGIN
	--если роль 1
	if exists (select 1 from Expl_Users where User_ID like @userId and Expl_Users.UserRole=1) return 1;
	--если нет то смотрим группы	
	declare 
	@DBObject_ID uniqueidentifier;
	select @DBObject_ID = ID from Expl_Users_DBObjects where [ObjectTypeName] = @objectTypeName and [Object_ID] = @objectId

	--Права без ограничения по иерархии
	IF (@objectId IS NULL) begin
		if (exists(select top 1 1 from Expl_UserGroup_Right r 
		join Expl_User_UserGroup ug on ug.UserGroup_ID = r.UserGroup_ID
		where r.RIGHT_ID = @rightId and ug.Deleted = 0 and ug.[User_ID] = @userId)) return 1
	end

	--Собственные права объекта
	if (exists(select top 1 1 from Expl_UserGroup_Right r 
	join Expl_User_UserGroup ug on ug.UserGroup_ID = r.UserGroup_ID
	where r.DBObject_ID = @DBObject_ID and ug.Deleted = 0 and (r.RIGHT_ID = @rightId or r.RIGHT_ID = 'F7E018EE-C70B-4094-86F8-504057E7AF44') and ug.[User_ID] = @userId)) return 1

	--Если нет собственных прав, ищем права дочернего объекта
		
	--declare @hl1 tinyint, @hl2 int, @hl3 int, @ps int, @forecastObject varchar(22);


	if (@treeID is null) begin

		--Ищем по ближайшему дочернему объекту (только по стандартному дереву)
		if @objectTypeName = 'Dict_HierLev1_' begin
			if (exists(select top 1 1 from Dict_HierLev2 where HierLev1_ID = @objectId 
				and dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, convert(varchar(255), HierLev2_ID), 'Dict_HierLev2_', @freeHierItemType, @treeID, @hierId) > 0)) return 2;
		end else if @objectTypeName = 'Dict_HierLev2_' begin
			if (exists(select top 1 1 from Dict_HierLev3 where HierLev2_ID = @objectId 
				and dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, convert(varchar(255), HierLev3_ID), 'Dict_HierLev3', @freeHierItemType, @treeID, @hierId) > 0)) return 2;
		end else if @objectTypeName = 'Dict_HierLev3' begin
			if (exists(select top 1 1 from Dict_PS where HierLev3_ID = @objectId 
				and dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, convert(varchar(255), PS_ID), 'Dict_PS_', @freeHierItemType, @treeID, @hierId) > 0)) return 2;
		end	
		--Сечения пока не делаем, если надо делаем по аналогии с usf2_UserHasRight

	end else begin
		-- Поиск прав по свободной иерархии
		if (exists(select top 1 1 from Dict_FreeHierarchyTree t
				join Dict_FreeHierarchyTree_Description d on t.FreeHierItem_ID=d.FreeHierItem_ID
				cross apply usf2_FreeHierarchyTreeDescriptionTypeAndId(d.HierLev1_ID, d.HierLev2_ID, d.HierLev3_ID, d.PS_ID, d.TI_ID, d.Formula_UN, d.Section_ID, d.TP_ID, d.USPD_ID, d.XMLSystem_ID, 
				d.JuridicalPerson_ID, d.JuridicalPersonContract_ID, d.DistributingArrangement_ID, d.BusSystem_ID, d.UANode_ID, d.OurFormula_UN, d.ForecastObject_UN, d.FreeHierItem_ID) f
				where t.FreeHierTree_ID = @treeID and t.HierID.GetAncestor(1)= @hierId
				and dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, f.[Object_ID], f.[ObjectTypeName], FreeHierItemType, @treeID, HierID) > 0))
				return 2;
	end
	
	return 0; --Не нашли права
END
go
grant EXECUTE on usf2_UserHasRightOrHasChild to [UserCalcService]
go
grant EXECUTE on usf2_UserHasRightOrHasChild to [UserDeclarator]
go
grant EXECUTE on usf2_UserHasRightOrHasChild to [UserImportService]
go
grant EXECUTE on usf2_UserHasRightOrHasChild to [UserExportService]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2018
--
-- Описание:
--
--		Проверка прав на группу объектов, и их дочерние объекты. Возвращаем объекты, у которых есть права, либо есть такое право у дочернего объекта
--
-- ======================================================================================
create proc [dbo].[usp2_UserHasRightOrHasChild]

	@ids [dbo].[UserRightId] READONLY,
	@userId varchar(22),
	@rightId uniqueidentifier
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select ids.* from @ids ids
where dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, ids.Object_ID, ids.ObjectTypeName, null, null, null) = 1

end
go

   grant EXECUTE on usp2_UserHasRightOrHasChild to [UserCalcService]
go
grant EXECUTE on usp2_UserHasRightOrHasChild to [UserDeclarator]
go
grant EXECUTE on usp2_UserHasRightOrHasChild to [UserImportService]
go
grant EXECUTE on usp2_UserHasRightOrHasChild to [UserExportService]
go


-- ======================================================================================
-- Автор:
--
--		Карпов
--
-- Дата создания:
--
--		 2018
--
-- Описание:
--
--		проверка прав для описателя (чтобы не ломать остальное)
--
-- ======================================================================================
create proc [dbo].[usp2_UserHasRightOrHasChild_Declarator]
@userId varchar(22),
@rightId uniqueidentifier,
@objectId varchar(255),
@objectTypeName varchar(255),
@freeHierItemType tinyint = null,
@treeID int = null
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare @hierId hierarchyid = null

	if(@objectTypeName like 'Dict_FreeHierarchyTree')
	begin
	 select @hierId=HierID from Dict_FreeHierarchyTree where FreeHierItem_ID=@objectId 
	end

select dbo.usf2_UserHasRightOrHasChild(@userId, @rightId, @objectId, @objectTypeName, @freeHierItemType, @treeID, @hierId)  

end
go

grant EXECUTE on [usp2_UserHasRightOrHasChild_Declarator] to [UserCalcService]
go
grant EXECUTE on [usp2_UserHasRightOrHasChild_Declarator] to [UserDeclarator]
go
grant EXECUTE on [usp2_UserHasRightOrHasChild_Declarator] to [UserImportService]
go
grant EXECUTE on [usp2_UserHasRightOrHasChild_Declarator] to [UserExportService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Ноябрь, 2018
--
-- Описание:
--
--		Проверка прав на группу объектов, и их дочерние объекты. Возвращаем объекты, у которых есть права, либо есть такое право у дочернего объекта
--
-- ======================================================================================
create proc [dbo].[usp2_UserHasRight]

	@ids [dbo].[UserRightId] READONLY, -- Идентификаторы объектов, по которым проверяем права
	@userId varchar(22), -- Идентификатор пользователя
	@rightId uniqueidentifier, -- Идентификатор права
	@treeID int = null -- Идентификатор дерева
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

select ids.* from @ids ids
where dbo.usf2_UserHasRight(@userId, @rightId, ids.Object_ID, ids.ObjectTypeName, null, @treeID, ids.FreeHierItemId) = 1

end
go

   grant EXECUTE on usp2_UserHasRight to [UserCalcService]
go
grant EXECUTE on usp2_UserHasRight to [UserDeclarator]
go
grant EXECUTE on usp2_UserHasRight to [UserImportService]
go
grant EXECUTE on usp2_UserHasRight to [UserExportService]
go
