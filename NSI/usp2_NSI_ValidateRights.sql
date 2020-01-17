if exists (select 1
          from sysobjects
          where  id = object_id('usp2_NSI_ValidateRights')
          and type in ('P','PC'))
   drop procedure usp2_NSI_ValidateRights
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Карпов Денис
--
-- Дата создания:
--
--		Июнь, 2014
--
-- Описание:
--
--		Проверка прав пользователя на объект и его родителей
--
-- ======================================================================================

create proc [dbo].[usp2_NSI_ValidateRights]
	@User_ID varchar(255),
	@Right_ID varchar(255),
	@ObjectTypeName varchar(255),
	@DBObject_ID varchar(255) = null
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

declare 
@AdminRight_ID varchar(200)

declare @Result int
set @Result=0

set @Right_ID=isnull(@Right_ID,'') 
set @DBObject_ID=isnull(@DBObject_ID,'') 
set @ObjectTypeName=isnull(@ObjectTypeName,'') 
set @User_ID=isnull(@User_ID,'') 


set @AdminRight_ID='F7E018EE-C70B-4094-86F8-504057E7AF44'  --право admin

--все родительские объекты
--==================================================
declare
@ParentPS_DBObject_ID varchar(200),
@ParentPS_ObjectTypeName varchar(200),
@ParentH3_DBObject_ID varchar(200),
@ParentH3_ObjectTypeName varchar(200),
@ParentH2_DBObject_ID varchar(200),
@ParentH2_ObjectTypeName varchar(200),
@ParentH1_DBObject_ID varchar(200),
@ParentH1_ObjectTypeName varchar(200)
set @ParentPS_DBObject_ID=''
set @ParentPS_ObjectTypeName ='Dict_PS'
set @ParentH3_DBObject_ID =''
set @ParentH3_ObjectTypeName='Dict_HierLev3'
set @ParentH2_DBObject_ID =''
set @ParentH2_ObjectTypeName ='Dict_HierLev2_'
set @ParentH1_DBObject_ID=''
set @ParentH1_ObjectTypeName ='Dict_HierLev1_'

if @DBObject_ID is not null and @DBObject_ID not like '' begin
	if (@ObjectTypeName like 'Dict_PS%')
		select 
		@ParentH3_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev3.HierLev3_ID),
		@ParentH2_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID),
		@ParentH1_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)		
		from Dict_PS 
		join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Dict_PS.PS_ID= CONVERT(int, @DBObject_ID)
	
	else if (@ObjectTypeName like 'Dict_HierLev3')
		select 
		@ParentH2_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID),
		@ParentH1_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)		
		from 
		Dict_HierLev3 
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Dict_HierLev3.HierLev3_ID= CONVERT(int, @DBObject_ID)	
		

	else if (@ObjectTypeName like 'Dict_HierLev2_')
		select 
		@ParentH1_DBObject_ID=  CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)		
		from 
		Dict_HierLev2 
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Dict_HierLev2.HierLev2_ID= CONVERT(int, @DBObject_ID)	
		
	else if (@ObjectTypeName like 'Info_Section_List') begin
		--если принадлежит ПС
		select 
		@ParentPS_DBObject_ID= case when Dict_PS.PS_ID is null then '' else   CONVERT(varchar(200), Dict_PS.PS_ID) end,
		@ParentH3_DBObject_ID= case when Dict_HierLev3.HierLev3_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev3.HierLev3_ID) end,
		@ParentH2_DBObject_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@ParentH1_DBObject_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_PS on Info_Section_List.PS_ID= Dict_PS.PS_ID
		join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is not null
		
		--если принадлежит H3
		select 
		@ParentH3_DBObject_ID= case when Dict_HierLev3.HierLev3_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev3.HierLev3_ID) end,
		@ParentH2_DBObject_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@ParentH1_DBObject_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_HierLev3 on Info_Section_List.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is not null
				
		--если принадлежит H2
		select 
		@ParentH2_DBObject_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@ParentH1_DBObject_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_HierLev2 on Info_Section_List.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is null
		and Info_Section_List.HierLev2_ID is not null
		
		
		--если принадлежит H1
		select 
		@ParentH1_DBObject_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_HierLev1 on Info_Section_List.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is null
		and Info_Section_List.HierLev2_ID is null
		and Info_Section_List.HierLev1_ID is not null
	end
end


--==================================================


--если такого пользователя нет
if (not exists (select top 1 1 from Expl_Users where [User_ID]=@User_ID))
begin
	set @Result=0  
end 
--если пользователь роль - админ
else if  (exists (select top 1 1 from Expl_Users where [User_ID]=@User_ID and UserRole=1))
begin
	set @Result=1
end
--если пользователь состоит в группе с правом администратор без ограничения по иерархии
else if  
(
exists 
(
select top 1 1
from 
Expl_Users 
join Expl_User_UserGroup on Expl_Users.User_ID=Expl_User_UserGroup.User_ID
join Expl_UserGroup_Right on Expl_User_UserGroup.UserGroup_ID=Expl_UserGroup_Right.UserGroup_ID
where 
Expl_UserGroup_Right.RIGHT_ID=@AdminRight_ID
and Expl_Users.User_ID=@User_ID
and DBObject_ID is null
and isnull(Expl_UserGroup_Right.IsAssent,1)=1
)
)
begin
	set @Result=1	
end
--==================
--теперь конкретные права на объекты
ELSE
BEGIN
	if @Right_ID like ''  
		set @Result=0
	else
	begin
			--проверяем указанное право без ограничения по объектам
			--за искл своб иерархий - там должно быть право конкретно на объект?
			 if (exists 
					(select * 
						from 
						Expl_Users 
						join Expl_User_UserGroup on Expl_Users.User_ID=Expl_User_UserGroup.User_ID
						join Expl_UserGroup_Right on Expl_User_UserGroup.UserGroup_ID=Expl_UserGroup_Right.UserGroup_ID
						where 
						Expl_UserGroup_Right.RIGHT_ID=@Right_ID
						and Expl_Users.User_ID=@User_ID
						and Expl_UserGroup_Right.DBObject_ID is null
						and @ObjectTypeName not like 'Dict_FreeHierarchyTypes'
                        and isnull(Expl_UserGroup_Right.IsAssent,1)=1
					)
				)
			set @Result=1
			else 
			--проверяем указанное право c ограничением по конкретному объекту Или его родительским объектам
			 if (@DBObject_ID not like '' and 
				 exists 
					(select * 
						from 
						Expl_Users 
						join Expl_User_UserGroup on Expl_Users.User_ID=Expl_User_UserGroup.User_ID
						join Expl_UserGroup_Right on Expl_User_UserGroup.UserGroup_ID=Expl_UserGroup_Right.UserGroup_ID
						join Expl_Users_DBObjects on Expl_UserGroup_Right.DBObject_ID =Expl_Users_DBObjects.ID
						where 
						Expl_UserGroup_Right.RIGHT_ID=@Right_ID
						and Expl_Users.User_ID=@User_ID
						and Expl_UserGroup_Right.DBObject_ID is not null
                        and isnull(Expl_UserGroup_Right.IsAssent,1)=1
						and 
						(
						 (Expl_Users_DBObjects.ObjectTypeName like @ObjectTypeName+'%' and Expl_Users_DBObjects.Object_ID = @DBObject_ID)
						 or 
						 (isnull(@ParentPS_DBObject_ID,'') not like '' and (Expl_Users_DBObjects.ObjectTypeName like @ParentPS_ObjectTypeName+'%' and Expl_Users_DBObjects.Object_ID = @ParentPS_DBObject_ID))
						 or 
						 (isnull(@ParentH3_DBObject_ID,'') not like '' and (Expl_Users_DBObjects.ObjectTypeName like @ParentH3_ObjectTypeName+'%' and Expl_Users_DBObjects.Object_ID = @ParentH3_DBObject_ID))
						 or 
						 (isnull(@ParentH2_DBObject_ID,'') not like '' and (Expl_Users_DBObjects.ObjectTypeName like @ParentH2_ObjectTypeName+'%' and Expl_Users_DBObjects.Object_ID = @ParentH2_DBObject_ID))
						 or 
						 (isnull(@ParentH1_DBObject_ID,'') not like '' and (Expl_Users_DBObjects.ObjectTypeName like @ParentH1_ObjectTypeName+'%' and Expl_Users_DBObjects.Object_ID = @ParentH1_DBObject_ID))
						
						)		
					)
				)
			set @Result=1
	end
END
select @Result
end
go
   grant EXECUTE on usp2_NSI_ValidateRights to [UserCalcService]
go
   grant EXECUTE on usp2_NSI_ValidateRights to [UserDeclarator]
go
   grant EXECUTE on usp2_NSI_ValidateRights to [UserImportService]
go
   grant EXECUTE on usp2_NSI_ValidateRights to [UserExportService]
go