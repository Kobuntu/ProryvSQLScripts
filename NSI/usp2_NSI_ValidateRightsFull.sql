if exists (select 1
          from sysobjects
          where  id = object_id('usp2_NSI_ValidateRightsFull')
          and type in ('P','PC'))
   drop procedure usp2_NSI_ValidateRightsFull
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
--	Проверка прав пользователя на объект и его родителей
--	берем перечень родит объектов и сам объект, сортируем по старшинству и начиная с объекта ищем право - разрешено или нет
-- ======================================================================================


/*
передаем ИД дерева и ИД узла

если стандартное (<=0) 
1) проверяем по объекту и его родителям

если не стандартное то
--1) проверяем право на объект БД, если нет то проверяем на его родителей (как объекты БД)
--2) если не нашли то проверяем право на родительские узлы и на объекты родительских узлов?

*/


create proc [dbo].usp2_NSI_ValidateRightsFull
	@FreeHierTree_ID int,
	@FreeHierItem_ID int,
	@User_ID varchar(200),
	@Right_ID varchar(200),
	@DBObject_ID varchar(200),
	@ObjectTypeName varchar(200)
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

--declare 
--@FreeHierTree_ID int,
--@FreeHierItem_ID int,
--@User_ID varchar(200),
--@Right_ID varchar(200),
--@DBObject_ID varchar(200),
--@ObjectTypeName varchar(200)
--set @User_ID='80005SHJBS0X0ZNJQQVL6I'
--set @Right_ID='48BE4C3B-2C74-4EC0-B5C4-1897220F9F6D'
--set @DBObject_ID=1
--set @ObjectTypeName='Dict_HierLev3_'
--set @FreeHierTree_ID=-101
--set @FreeHierItem_ID=39

declare 
@AdminRight_ID varchar(200)
set @AdminRight_ID='F7E018EE-C70B-4094-86F8-504057E7AF44'  --право admin

declare @RightEditor nvarchar(200)='f14b985c-0ab9-4372-b185-87d0e5bbde84'

--право админ не дает права на рдедактор прав.. поэтому исключаем его таким образом...
if (@Right_ID like @RightEditor)
begin
	set @AdminRight_ID=@Right_ID
end

declare @Result int
set @Result=0
set @Right_ID=isnull(@Right_ID,'') 
set @DBObject_ID=isnull(@DBObject_ID,'') 
set @ObjectTypeName=isnull(@ObjectTypeName,'') 
set @User_ID=isnull(@User_ID,'') 


if (@ObjectTypeName) like 'Dict_PS%'
set @ObjectTypeName = 'Dict_PS_'
else if (@ObjectTypeName) like 'Dict_HierLev1%'
set @ObjectTypeName = 'Dict_HierLev1_'
else if (@ObjectTypeName) like 'Dict_HierLev2%'
set @ObjectTypeName = 'Dict_HierLev2_'
else if (@ObjectTypeName) like 'Dict_HierLev3%'
set @ObjectTypeName = 'Dict_HierLev3'


declare @SectionParent_PS_ID int, @SectionParent_H3_ID int, @SectionParent_H2_ID int, @SectionParent_H1_ID int

--ИД объекта в БД. тип объекта, ИД узла, название, ИД в Expl_Users_DBObjects
declare @tempParentObj table 
(ObjectrealID varchar(200), ObjectTypeName varchar(200), FreeHierItem_ID int, StringName varchar(200), ID varchar(200), Lvl int, IsAssent bit)

--для стандартных деревьев находим продительские объекты, для стандартной иерархии
IF (@DBObject_ID not like '' and @FreeHierTree_ID<=0)
begin

	insert into @tempParentObj
	--РОДИТЕЛИ ПС
	select  distinct
	CONVERT(varchar(200),Dict_HierLev3.HierLev3_ID ), 
	'Dict_HierLev3', 
	null,
	StringName=Dict_HierLev3.StringName, 
	null,3, null
	from Dict_PS 
			join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
			join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_PS.PS_ID= CONVERT(int, @DBObject_ID)
			and @ObjectTypeName like 'Dict_PS%'
	union		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev2.HierLev2_ID ), 
	'Dict_HierLev2_', null,
	StringName=Dict_HierLev2.StringName, 
	null,2, null
	from Dict_PS 
			join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
			join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_PS.PS_ID= CONVERT(int, @DBObject_ID)
			and @ObjectTypeName like 'Dict_PS%'
	union		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev1.HierLev1_ID ), 
	'Dict_HierLev1_', null,
	StringName=Dict_HierLev1.StringName, 
	null,1, null
	from Dict_PS 
			join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
			join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_PS.PS_ID= CONVERT(int, @DBObject_ID)		
			and @ObjectTypeName like 'Dict_PS%'
			
	--УРОВЕНЬ 3
	union		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev2.HierLev2_ID ), 
	'Dict_HierLev2_', null,
	StringName=Dict_HierLev2.StringName, 
	null,2, null
	from 
	Dict_HierLev3 
			join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_HierLev3.HierLev3_ID= CONVERT(int, @DBObject_ID)
			and @ObjectTypeName like 'Dict_HierLev3%'
	union		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev1.HierLev1_ID ), 
	'Dict_HierLev1_', null,
	StringName=Dict_HierLev1.StringName, 
	null,1, null
	from Dict_HierLev3 
			join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_HierLev3.HierLev3_ID= CONVERT(int, @DBObject_ID)
			and @ObjectTypeName like 'Dict_HierLev3%'
			
			
	--УРОВЕНЬ 2
	union		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev2.HierLev1_ID ), 
	'Dict_HierLev1_', null,
	StringName=Dict_HierLev1.StringName, 
	null,1, null
	from Dict_HierLev2 
			join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
			where Dict_HierLev2.HierLev2_ID= CONVERT(int, @DBObject_ID)	
			and @ObjectTypeName like 'Dict_HierLev2%'
		
	if (@ObjectTypeName like 'Info_Section_List')
	begin
		--если принадлежит ПС
		select 
		@SectionParent_PS_ID= case when Dict_PS.PS_ID is null then '' else   CONVERT(varchar(200), Dict_PS.PS_ID) end,
		@SectionParent_H3_ID= case when Dict_HierLev3.HierLev3_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev3.HierLev3_ID) end,
		@SectionParent_H2_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@SectionParent_H1_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_PS on Info_Section_List.PS_ID= Dict_PS.PS_ID
		join Dict_HierLev3 on Dict_PS.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is not null
		
		--если принадлежит H3
		select 
		@SectionParent_PS_ID= null,
		@SectionParent_H3_ID= case when Dict_HierLev3.HierLev3_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev3.HierLev3_ID) end,
		@SectionParent_H2_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@SectionParent_H1_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
			from Info_Section_List 
		join Dict_HierLev3 on Info_Section_List.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		join Dict_HierLev2 on Dict_HierLev3.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is not null
				
		--если принадлежит H2
		select 
		@SectionParent_PS_ID= null,
		@SectionParent_H3_ID= null,
		@SectionParent_H2_ID=  case when Dict_HierLev2.HierLev2_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev2.HierLev2_ID)end,
		@SectionParent_H1_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
		from Info_Section_List 
		join Dict_HierLev2 on Info_Section_List.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		join Dict_HierLev1 on Dict_HierLev2.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is null
		and Info_Section_List.HierLev2_ID is not null
		
		
		--если принадлежит H1
		select 
		@SectionParent_PS_ID= null,
		@SectionParent_H3_ID= null,
		@SectionParent_H2_ID= null,
		@SectionParent_H1_ID=  case when Dict_HierLev1.HierLev1_ID is null then '' else   CONVERT(varchar(200), Dict_HierLev1.HierLev1_ID)	end	
			from Info_Section_List 
		join Dict_HierLev1 on Info_Section_List.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		where Info_Section_List.Section_ID= CONVERT(int, @DBObject_ID)
		and Info_Section_List.PS_ID is null
		and Info_Section_List.HierLev3_ID is null
		and Info_Section_List.HierLev2_ID is null
		and Info_Section_List.HierLev1_ID is not null
		
		
			
	---добавляем родителей сечения
		
	Insert into @tempParentObj		 
	select  distinct
	CONVERT(varchar(200),Dict_HierLev1.HierLev1_ID ), 
	'Dict_HierLev1_', null,
	StringName=Dict_HierLev1.StringName, 
	null,1, null
	from Dict_HierLev1 			 
	where Dict_HierLev1.HierLev1_ID= @SectionParent_H1_ID and @SectionParent_H1_ID is not null
	
	union
	
	select  distinct
	CONVERT(varchar(200),Dict_HierLev2.HierLev2_ID ), 
	'Dict_HierLev2_', null,
	StringName=Dict_HierLev2.StringName, 
	null,2, null
	from Dict_HierLev2 			 
	where Dict_HierLev2.HierLev2_ID= @SectionParent_H2_ID and @SectionParent_H2_ID is not null
	
	union	
	
	select  distinct
	CONVERT(varchar(200),Dict_HierLev3.HierLev3_ID ), 
	'Dict_HierLev3', null,
	StringName=Dict_HierLev3.StringName	, 
	null,3, null
	from Dict_HierLev3 		 
	where Dict_HierLev3.HierLev3_ID= @SectionParent_H3_ID and @SectionParent_H3_ID is not null
	
	union	
	
	select  distinct
	CONVERT(varchar(200),Dict_PS.PS_ID ), 
	'Dict_PS_', null,
	StringName=Dict_PS.StringName, 
	null,4, null
	from Dict_PS 			 
	where Dict_PS.PS_ID= @SectionParent_PS_ID and @SectionParent_PS_ID is not null
	
	end						
end

--добавляем сам обеъект (lvl последний,тк будем проверят права с конца)
insert into @tempParentObj
values (@DBObject_ID, @ObjectTypeName, null, 'выбранный объект', null, 999, null)



--теперь добавляем родительские обекты из своб иерархий
if (@FreeHierTree_ID>0)
begin
print 'теперь добавляем родительские обекты из своб иерархий'

	declare @HierID varchar(200)
	
	select @HierID=HierID.ToString()from Dict_FreeHierarchyTree
	where FreeHierItem_ID=@FreeHierItem_ID

	insert into @tempParentObj	
	--родительские узлы
	select null,'Dict_FreeHierarchyTree', FreeHierItem_ID, StringName, null, HierLevel, null  from Dict_FreeHierarchyTree
	where hierarchyid::Parse(@HierID).IsDescendantOf(HierID) = 1
	and HierID<>@HierID
	and FreeHierTree_ID=@FreeHierTree_ID
	
	union
	--и самого себя
	select null,'Dict_FreeHierarchyTree', FreeHierItem_ID, StringName, null, HierLevel, null  from Dict_FreeHierarchyTree
	where 
	FreeHierItem_ID=@FreeHierItem_ID
	and FreeHierTree_ID=@FreeHierTree_ID
	
	union
	--родительские узлы если это объекты
	select 
	case when Dict_FreeHierarchyTree_Description.HierLev1_ID is not null then Dict_FreeHierarchyTree_Description.HierLev1_ID 
	when Dict_FreeHierarchyTree_Description.HierLev2_ID is not null then Dict_FreeHierarchyTree_Description.HierLev2_ID 
	when Dict_FreeHierarchyTree_Description.HierLev3_ID is not null then Dict_FreeHierarchyTree_Description.HierLev3_ID 
	when Dict_FreeHierarchyTree_Description.PS_ID is not null then Dict_FreeHierarchyTree_Description.PS_ID 
	when Dict_FreeHierarchyTree_Description.Section_ID is not null then Dict_FreeHierarchyTree_Description.Section_ID 
	else '-' end
	,
	case when Dict_FreeHierarchyTree_Description.HierLev1_ID is not null then 'Dict_HierLev1_'
	when Dict_FreeHierarchyTree_Description.HierLev2_ID is not null then 'Dict_HierLev2_'
	when Dict_FreeHierarchyTree_Description.HierLev3_ID is not null then 'Dict_HierLev3'
	when Dict_FreeHierarchyTree_Description.PS_ID is not null then 'Dict_PS_'
	when Dict_FreeHierarchyTree_Description.Section_ID is not null then 'Info_Section_List'
	else '-' end, 
	null, StringName, null  
	, HierLevel, null 
	from Dict_FreeHierarchyTree
	join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree_Description.FreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID 
	where hierarchyid::Parse(@HierID).IsDescendantOf(HierID) = 1
	and HierID<>@HierID
	and FreeHierTree_ID=@FreeHierTree_ID
	and Dict_FreeHierarchyTree.FreeHierItemType<>0
	and 
	(Dict_FreeHierarchyTree_Description.HierLev1_ID is not null
	or Dict_FreeHierarchyTree_Description.HierLev2_ID is not null
	or Dict_FreeHierarchyTree_Description.HierLev3_ID is not null
	or Dict_FreeHierarchyTree_Description.PS_ID is not null
	or Dict_FreeHierarchyTree_Description.Section_ID is not null	
	)
	
	union
	
	--и самого себя если это объект
	select 
	case when Dict_FreeHierarchyTree_Description.HierLev1_ID is not null then Dict_FreeHierarchyTree_Description.HierLev1_ID 
	when Dict_FreeHierarchyTree_Description.HierLev2_ID is not null then Dict_FreeHierarchyTree_Description.HierLev2_ID 
	when Dict_FreeHierarchyTree_Description.HierLev3_ID is not null then Dict_FreeHierarchyTree_Description.HierLev3_ID 
	when Dict_FreeHierarchyTree_Description.PS_ID is not null then Dict_FreeHierarchyTree_Description.PS_ID 
	when Dict_FreeHierarchyTree_Description.Section_ID is not null then Dict_FreeHierarchyTree_Description.Section_ID 
	else '-' end
	,
	case when Dict_FreeHierarchyTree_Description.HierLev1_ID is not null then 'Dict_HierLev1_'
	when Dict_FreeHierarchyTree_Description.HierLev2_ID is not null then 'Dict_HierLev2_'
	when Dict_FreeHierarchyTree_Description.HierLev3_ID is not null then 'Dict_HierLev3'
	when Dict_FreeHierarchyTree_Description.PS_ID is not null then 'Dict_PS_'
	when Dict_FreeHierarchyTree_Description.Section_ID is not null then 'Info_Section_List'
	else '-' end, 
	null, StringName, null  
	, HierLevel, null 
	from Dict_FreeHierarchyTree
	join Dict_FreeHierarchyTree_Description on Dict_FreeHierarchyTree_Description.FreeHierItem_ID=Dict_FreeHierarchyTree.FreeHierItem_ID 
	where 	
	Dict_FreeHierarchyTree.FreeHierItem_ID=@FreeHierItem_ID
	and Dict_FreeHierarchyTree.FreeHierTree_ID=@FreeHierTree_ID
	and Dict_FreeHierarchyTree.FreeHierItemType<>0
	and 
	(Dict_FreeHierarchyTree_Description.HierLev1_ID is not null
	or Dict_FreeHierarchyTree_Description.HierLev2_ID is not null
	or Dict_FreeHierarchyTree_Description.HierLev3_ID is not null
	or Dict_FreeHierarchyTree_Description.PS_ID is not null
	or Dict_FreeHierarchyTree_Description.Section_ID is not null	
	)
end
 

--insert into @tempParentObj
--values (null, @ObjectTypeName, @FreeHierItem_ID, 'выбранный объект в своб иерархиях', null)



--получаем Объекты прав  объекты
update @tempParentObj
set ID= Expl_Users_DBObjects.ID
from 
@tempParentObj temp join Expl_Users_DBObjects
on temp.ObjectTypeName like Expl_Users_DBObjects.ObjectTypeName
and isnull(temp.ObjectrealID,'') like isnull(Expl_Users_DBObjects.Object_ID,'-')
and temp.ObjectrealID is not null
and Expl_Users_DBObjects.ObjectTypeName not like 'Info_TI'


--получаем Объекты прав  узлы
update @tempParentObj
set ID= Expl_Users_DBObjects.ID
from 
@tempParentObj temp join Expl_Users_DBObjects
on temp.ObjectTypeName like Expl_Users_DBObjects.ObjectTypeName
and isnull(temp.FreeHierItem_ID,'') like isnull(Expl_Users_DBObjects.Object_ID,'-')
and temp.FreeHierItem_ID is not null
and Expl_Users_DBObjects.ObjectTypeName not like 'Info_TI'


--удаляем объекты и узлы не найденные в Expl_Users_DBObjects
delete from @tempParentObj
where ID is null


--ПРОВЕРКА ПРАВ
--если заблокирован или неподтвержден или удален - нет прав
if (exists (select 1 from Expl_Users where [User_ID]=@User_ID and (IsLockedOut=1 or IsApproved=0 or Deleted=1)))
begin
	set @Result=0  
end 

--если нет такого то нет прав
else if (not exists (select  1 from Expl_Users where [User_ID]=@User_ID))
begin
	set @Result=0  
end 

--если пользователь роль - админ то есть права
else if  (@Right_ID not like @RightEditor and   exists (select  1 from Expl_Users where [User_ID]=@User_ID and UserRole=1))
begin
	set @Result=1
end

--если пользователь состоит в группе с правом администратор без ограничения по иерархии
else if  
(
exists 
(
select * 
from 
Expl_Users 
join Expl_User_UserGroup on Expl_Users.User_ID=Expl_User_UserGroup.User_ID
join Expl_UserGroup_Right on Expl_User_UserGroup.UserGroup_ID=Expl_UserGroup_Right.UserGroup_ID
where 
Expl_UserGroup_Right.RIGHT_ID=@AdminRight_ID
and Expl_Users.User_ID=@User_ID
and DBObject_ID is null --только если объект не указан (старые права) в будущем убрать надо будет
and isnull(Expl_UserGroup_Right.IsAssent,1)=1
and Expl_User_UserGroup.Deleted=0
and Expl_UserGroup_Right.Deleted=0
and Expl_Users.Deleted=0
)
)
begin
	set @Result=1	
end

else 
begin
	
	update @tempParentObj
	set
	IsAssent=isnull(Expl_UserGroup_Right.IsAssent,1)
	from 
	Expl_User_UserGroup 
	join Expl_UserGroup_Right on Expl_UserGroup_Right.UserGroup_ID=Expl_User_UserGroup.UserGroup_ID 
	join Expl_Users_DBObjects on Expl_Users_DBObjects.ID= Expl_UserGroup_Right.DBObject_ID
	join  @tempParentObj temp on temp.ID=Expl_Users_DBObjects.ID
	where 
	Expl_User_UserGroup.Deleted=0 
	and Expl_User_UserGroup.[User_ID]= @User_ID
	and (Expl_UserGroup_Right.RIGHT_ID=@Right_ID or Expl_UserGroup_Right.RIGHT_ID=@AdminRight_ID) --указанное право или админ
	and Expl_User_UserGroup.Deleted=0
	and Expl_UserGroup_Right.Deleted=0
	and Expl_Users_DBObjects.Deleted=0
and Expl_Users_DBObjects.ObjectTypeName not like 'Info_TI'
	
	--оставляем только объекты на которые реально дано это право (с учетом старых версий isnull(Expl_UserGroup_Right.IsAssent,1))
	delete from @tempParentObj where IsAssent is null

	--выбираем право ближайшего объекта
	set @Result=isnull((select top 1 isnull(IsAssent,0) from @tempParentObj order by Lvl desc),0)

end


--для просмотра/ редактирования отчетов не проверяем объекты (эти права д.б. без ограничения)
if (@DBObject_ID='' or @DBObject_ID='0') and (@Right_ID='2EC883F2-470F-4f7b-8BA7-041281E6A5FC' or @Right_ID='5A88CCC3-08AA-454D-B150-F177A7E65EB8')
begin

select 
@Result=isnull(Expl_UserGroup_Right.IsAssent,1)
	from 
	Expl_User_UserGroup 
	join Expl_UserGroup_Right on Expl_UserGroup_Right.UserGroup_ID=Expl_User_UserGroup.UserGroup_ID 
	
	where 
	Expl_User_UserGroup.Deleted=0 
	and Expl_User_UserGroup.[User_ID]= @User_ID
	and (Expl_UserGroup_Right.RIGHT_ID=@Right_ID ) --указанное право или админ
	and Expl_User_UserGroup.Deleted=0
	and Expl_UserGroup_Right.Deleted=0
	and Expl_UserGroup_Right.DBObject_ID  is null
	
	
	set @Result=ISNULL(@Result,1)
end

select @Result


end
go

   grant EXECUTE on usp2_NSI_ValidateRightsFull to [UserCalcService]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull to [UserDeclarator]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull to [UserImportService]
go
   grant EXECUTE on usp2_NSI_ValidateRightsFull to [UserExportService]
go




--exec [dbo].usp2_NSI_ValidateRightsFull 
--33, --Tree_ID
--44, --FreeItem_ID 41-44
--'80005Y7AHL0Q1OXYI65PJS', --UserID
--'F34707DB-AFFF-4296-BEAF-B490F25BB71B',--'48BE4C3B-2C74-4ec0-B5C4-1897220F9F6D' , --Right EditStructure
--8101, --ObjectID
--'Dict_PS'-- 'Dict_FreeHierarchyTree' --objTypeName
 
 
 

--select * from Dict_FreeHierarchyTree where FreeHierTree_ID=33