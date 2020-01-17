set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf_UserHasRight')
          and type in ('IF', 'FN', 'TF'))
   drop function usf_UserHasRight
go

create FUNCTION [dbo].[usf_UserHasRight]
(
@userId varchar(22),
@rightId uniqueidentifier,
@dbObjectId uniqueidentifier = null
)
RETURNS bit
AS
BEGIN
-- Пользователь может быть удален
-- Группа может быть удалена
-- Объект может быть удален
-- Главное, чтобы запись о назначении прав группе не была удалена.
-- Если запрошено право и указан объект, то ищется такое право для объекта, 
-- его родителя или право без указания объекта.
-- Если запрошено право без указания объекта, то ищется право без указания объекта.
declare @ret bit
--если роль 1
if exists (select 1 from Expl_Users where User_ID like @userId and Expl_Users.UserRole=1)			
	set @ret = 1;
--если нет то смотрим группы	
else 
begin
if @dbObjectId is null
	begin
	if exists(
		select *
		from Expl_UserGroup_Right
		inner join Expl_User_UserGroup on Expl_User_UserGroup.UserGroup_ID = Expl_UserGroup_Right.UserGroup_ID
			and Expl_User_UserGroup.Deleted = 0
		where Expl_UserGroup_Right.RIGHT_ID = @rightId
			and Expl_User_UserGroup.User_ID = @userId
			and Expl_UserGroup_Right.Deleted = 0
			and Expl_UserGroup_Right.DBObject_ID is null
		)
		set @ret = 1;		
	else
		set @ret = 0;			
	end;	
else begin
	--declare @n int;
	--with te (Id, ParentId) as
	--(
	--	select ID, Parend_ID
	--	from Expl_Users_DBObjects
	--	where ID = @dbObjectId
	--	union all
	--	select Expl_Users_DBObjects.ID, Expl_Users_DBObjects.Parend_ID
	--	from Expl_Users_DBObjects
	--	inner join te on te.ParentId = Expl_Users_DBObjects.ID
	--)
	--select @n = count(*) 
	--from Expl_UserGroup_Right
	--inner join Expl_User_UserGroup on Expl_User_UserGroup.UserGroup_ID = Expl_UserGroup_Right.UserGroup_ID
	--	and Expl_User_UserGroup.Deleted = 0
	--inner join te on te.Id = Expl_UserGroup_Right.DBObject_ID or Expl_UserGroup_Right.DBObject_ID is null
	--where Expl_UserGroup_Right.RIGHT_ID = @rightId
	--	and Expl_User_UserGroup.User_ID = @userId
	--	and Expl_UserGroup_Right.Deleted = 0
	--if @n > 0
	--	set @ret =  1
	--else
	--	set @ret = 0
	--end

	declare 
	@objectTypeName varchar(255),
	@objectId int,
	@DBObject_ID uniqueidentifier;

	select @objectTypeName = ObjectTypeName, @objectId = [Object_ID], @DBObject_ID = ID from Expl_Users_DBObjects 
	where ID = @dbObjectId

	--Собственные права объекта
	if (exists(select top 1 1 from Expl_UserGroup_Right r 
	join Expl_User_UserGroup ug on ug.UserGroup_ID = r.UserGroup_ID
	where r.DBObject_ID = @DBObject_ID and r.RIGHT_ID = @rightId and ug.Deleted = 0 and ug.[User_ID] = @userId)) return 1

	--Если нет собственных прав, ищем права родителя
	declare 
	@parentTypeName varchar(255),
	@parentId int;

	--Ищем по ближайшему родителю (только по стандартному дереву)
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
	end 

	if (@parentId is null) return 0; --Нет родителя

	return dbo.usf_UserHasRight(@userId, @rightId, (select top 1 ID from Expl_Users_DBObjects where ObjectTypeName = @parentTypeName and [Object_ID] = @parentId))
	end
end
	return @ret
END
go
grant EXECUTE on usf_UserHasRight to [UserCalcService]
go
grant EXECUTE on usf_UserHasRight to [UserDeclarator]
go
grant EXECUTE on usf_UserHasRight to [UserImportService]
go
grant EXECUTE on usf_UserHasRight to [UserExportService]
go
grant EXECUTE on usf_UserHasRight to [UserMaster61968Service]
go
grant EXECUTE on usf_UserHasRight to [UserSlave61968Service]
go
grant EXECUTE on usf_UserHasRight to [UserWebMonitoringService]
go

