set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UserHasRight')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UserHasRight
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Январь, 2016
--
-- Описание:
--
--		Проверка конкретного права на конкретный объект и всех его родителей
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_UserHasRight]
(
@userId varchar(22), --Идентификатор пользователя
@rightId uniqueidentifier, --Идентификатор права
@objectId varchar(255), -- Идентификатор объекта
@objectTypeName varchar(255), -- Тип объекта строковый (в соответствии с полем ObjectTypeName таблицы Expl_Users_DBObjects)
@freeHierItemType tinyint = null, -- Тип объекта числовой (в соответствии с полем FreeHierItemType таблицы Dict_FreeHierarchyTree)
@treeID int = null,
@freeHierItemId int = null
)
RETURNS bit
AS
BEGIN
--если роль 1
if exists (select 1 from Expl_Users where User_ID like @userId and Expl_Users.UserRole=1) return 1;
--если нет то смотрим группы	
	declare 
	@DBObject_ID uniqueidentifier;
	select @DBObject_ID = ID from Expl_Users_DBObjects where [Object_ID] = @objectId and [ObjectTypeName] = @objectTypeName

	--Права без ограничения по иерархии
	IF (@objectId IS NULL) begin
		if (exists(select top 1 1 from Expl_UserGroup_Right r 
		join Expl_User_UserGroup ug on ug.UserGroup_ID = r.UserGroup_ID
		where r.RIGHT_ID = @rightId and ug.Deleted = 0 and ug.[User_ID] = @userId and r.IsAssent=1)) return 1
	end

	--Собственные права объекта
	if (exists(select top 1 1 from Expl_UserGroup_Right r 
	join Expl_User_UserGroup ug on ug.UserGroup_ID = r.UserGroup_ID
	where r.DBObject_ID = @DBObject_ID and (r.RIGHT_ID = @rightId or r.RIGHT_ID = 'F7E018EE-C70B-4094-86F8-504057E7AF44') and ug.Deleted = 0 and ug.[User_ID] = @userId and r.IsAssent=1)) return 1

	--Если нет собственных прав, ищем права родителя
	declare 
	@parentTypeName varchar(255),
	@parentId varchar(255),
	@parentFreeHierItemId int;

	declare @hl1 tinyint, @hl2 int, @hl3 int, @ps int, @forecastObject varchar(22);

	--Ищем по ближайшему родителю (только по стандартному дереву)
	if (@treeID is null and @objectTypeName <> 'Dict_FreeHierarchyTree' and @objectTypeName <> 'Forecast_Objects') begin

		--Поиск родителя по стандартному дереву свободной иерархии
		if (@objectTypeName = 'Info_TI') begin
			set @parentTypeName = 'Dict_PS_';
			set @parentId = (select PS_ID from Info_TI where TI_ID = @objectId)
		end else if @objectTypeName = 'Dict_PS_' begin
			set @parentTypeName = 'Dict_HierLev3';
			set @parentId = (select HierLev3_ID from Dict_PS where PS_ID = @objectId)
		end else if @objectTypeName = 'Dict_HierLev3' begin
			set @parentTypeName = 'Dict_HierLev2_';
			set @parentId = (select HierLev2_ID from Dict_HierLev3 where HierLev3_ID = @objectId)
		end else if @objectTypeName = 'Dict_HierLev2_' begin
			set @parentTypeName = 'Dict_HierLev1_';
			set @parentId = (select HierLev1_ID from Dict_HierLev2 where HierLev2_ID = @objectId)
		end else if @objectTypeName = 'Info_TP2' begin
			set @parentTypeName = 'Info_Section_List';
			set @parentId = (select Section_ID from Info_Section_Description2 where TP_ID = @objectId)
		end else if @objectTypeName = 'Info_Section_List' begin
		
			select top 1 @hl1 = [HierLev1_ID], @hl2 = [HierLev2_ID], @hl3 = [HierLev3_ID], @ps = [PS_ID] from Info_Section_List where Section_ID = @objectId
		
			if @ps is not null begin
				set @parentTypeName = 'Dict_PS_' 
				set @parentId = @ps
			end else if @hl3 is not null begin
				set @parentTypeName = 'Dict_HierLev3' 
				set @parentId = @hl3
			end else if @hl2 is not null begin 
				set @parentTypeName = 'Dict_HierLev2_' 
				set @parentId = @hl2
			end else if @hl1 is not null begin 
				set @parentTypeName = 'Dict_HierLev1_' 
				set @parentId = @hl1
			end
		end

		set @parentFreeHierItemId = null;

	end else if (@treeID is not null or @objectTypeName = 'Dict_FreeHierarchyTree' or @objectTypeName = 'Forecast_Objects') begin

		--Поиск родителя по дереву свободной иерархии
		set @parentTypeName = 'Dict_FreeHierarchyTree';
		declare @hierID hierarchyid

		--По свободной иерархии право 
		if (@objectTypeName = 'Dict_FreeHierarchyTree' and @freeHierItemType is not null) begin
			--Поиск по прямому идентификатору объекта 
			select @hierID=HierID from Dict_FreeHierarchyTree where FreeHierItem_ID=@objectId and FreeHierItemType = @freeHierItemType
		end else begin
			--Поик по собственному идентификатору объекта
			select @hierID= dbo.usf2_FreeHierarchyTreeDescriptionReturnHierId(@objectId, @objectTypeName, @treeID, @freeHierItemId)

			--Если дерево указано, но объект не найден - объект добавлен по галочке, ищем права по стандартному дереву
			if (@hierID is null) return dbo.usf2_UserHasRight(@userId, @rightId, @parentId, @parentTypeName, @freeHierItemType, null, null)
		end	

		if (@hierID = hierarchyid::GetRoot()) return 0; -- Объект рут, искать по дереву иерархии не имеет смысла


		select top 1 @parentTypeName = f.ObjectTypeName, @parentId = f.Object_ID, @parentFreeHierItemId = t.FreeHierItem_ID from Dict_FreeHierarchyTree t
		left join [dbo].[Dict_FreeHierarchyTree_Description] d on t.FreeHierItem_ID = d.FreeHierItem_ID
		cross apply usf2_FreeHierarchyTreeDescriptionTypeAndId(d.HierLev1_ID, d.HierLev2_ID, d.HierLev3_ID, d.PS_ID, d.TI_ID, d.Formula_UN, d.Section_ID, d.TP_ID, d.USPD_ID, d.XMLSystem_ID, 
			d.JuridicalPerson_ID, d.JuridicalPersonContract_ID, d.DistributingArrangement_ID, d.BusSystem_ID, d.UANode_ID, d.OurFormula_UN, d.ForecastObject_UN, d.FreeHierItem_ID) f
		where (@treeID is null or t.FreeHierTree_ID = @treeID) and t.HierID = @hierID.GetAncestor(1)

	end 

	if (@parentId is null) return 0; --Нет родителя
		--Ищем права у родителя
	return dbo.usf2_UserHasRight(@userId, @rightId, @parentId, @parentTypeName, @freeHierItemType, @treeID, @parentFreeHierItemId)
END
go
grant EXECUTE on usf2_UserHasRight to [UserCalcService]
go
grant EXECUTE on usf2_UserHasRight to [UserDeclarator]
go
grant EXECUTE on usf2_UserHasRight to [UserImportService]
go
grant EXECUTE on usf2_UserHasRight to [UserExportService]
go
grant EXECUTE on usf2_UserHasRight to [UserMaster61968Service]
go
grant EXECUTE on usf2_UserHasRight to [UserSlave61968Service]
go
grant EXECUTE on usf2_UserHasRight to [UserWebMonitoringService]
go

