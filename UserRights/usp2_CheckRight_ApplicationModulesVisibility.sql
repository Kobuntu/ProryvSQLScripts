if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp2_CheckRight_ApplicationModulesVisibility')
          and type in ('P','PC'))
   drop procedure dbo.usp2_CheckRight_ApplicationModulesVisibility
go

-- ======================================================================================
-- Автор:
--
--		карпов
--
-- Дата создания:
--
--		Март, 2017
--
-- Описание:
--
--		проверка прав на открытие модуля (видимость кнопок в приложениях)
--
-- ======================================================================================

Create procedure usp2_CheckRight_ApplicationModulesVisibility
@ModuleIds BigintType READONLY, @User_ID varchar(200)
AS
BEGIN
	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted
	set dateformat dmy
	
	--можно использовать для ограничения видимости модулей даже админам!
	--но если явно не скрыто, то отображаем, а дальше в соответствии с обычными правилами

	--если пользователь заблокирован или отсутствует то не отображаем ничего
	declare @UserBlocked bit =1 
	select @UserBlocked=case when Expl_Users.IsApproved=0 or Expl_Users.IsLockedOut=1 then 1 else 0 end
	from Expl_Users 
	where
	User_ID = @User_ID
	and Expl_Users.Deleted=0 

	set @UserBlocked= isnull(@UserBlocked,1)
	
	declare @res table (DBObject_ID bigint, Hide int)

	insert into @res
	select 
	DBObject_ID = temp.ID, 
	Hide = 	cast(case when isnull (Expl_UserGroup_Right.IsAssent,0) =0 then 0 else Expl_UserGroup_Right.IsAssent end as int) 
	from 
	@ModuleIds temp 
	 join Expl_Users_DBObjects on temp.ID= Expl_Users_DBObjects.Object_ID 
	--HideModule
	 join Expl_UserGroup_Right on Expl_UserGroup_Right.DBObject_ID=Expl_Users_DBObjects.ID 
	 join Expl_User_UserGroup on Expl_User_UserGroup.UserGroup_ID= Expl_UserGroup_Right.UserGroup_ID 	
	 where 
	 Expl_UserGroup_Right.RIGHT_ID='C3EA170F-18CD-47F3-B208-5FE8761C961D' --скрыть модуль
	 and Expl_UserGroup_Right.Deleted=0
	 and Expl_User_UserGroup.Deleted=0
	 and ObjectTypeName = 'ApplicationMenuItem'
	 and Expl_User_UserGroup.User_ID=@User_ID
	 	 
	--если пользователь заблокирован то скрываем все
	select MenuItemID=DBObject_ID, Hide=(cast(res.Hide as bit) | @UserBlocked)	
	from 
	( 	
		--явно скрытые модули
		select  * from @res
		union all
		--остальные отображаем
		select DBObject_ID=Id, Hide=cast(0 as int) from @ModuleIds
		where ID not in (select DBObject_ID from @res)
	)
	as res
	order by MenuItemID
		

END
GO


grant EXECUTE on dbo.usp2_CheckRight_ApplicationModulesVisibility to UserDeclarator
go
grant EXECUTE on dbo.usp2_CheckRight_ApplicationModulesVisibility to UserCalcService
go


 

