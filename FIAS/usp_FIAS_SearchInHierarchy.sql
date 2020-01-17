
if exists (select 1
          from sysobjects
          where  id = object_id('dbo.usp_FIAS_SearchInHierarchy')
          and type in ('P','PC'))
   drop procedure dbo.usp_FIAS_SearchInHierarchy
go


create procedure dbo.usp_FIAS_SearchInHierarchy 
@SearchType int, @ItemLevel int,@ParentGUID int, @SearchStr nvarchar(400)
as
begin

/*
--@SearchType:
--0 - поиск по адресу
--1 - дочерние ноды - задаем ParentObjectID и level 
--  	- если ParentObjectID=Null  то верхний уровень, если нет то дочерние

--примеры
--поиск дочерних для 1 уровня ID=57
exec usp_SearchInHierarchy 1,3,57,null
--поиск по названию 
exec usp_SearchInHierarchy 0,0,0, 'дружб 1'
--поиск элементов верхнего уровня
exec usp_SearchInHierarchy 1,0,null,''
*/

--для улучшения поиска по включению...
SET @SearchStr= replace(@SearchStr,' ','%')
SET @SearchStr= replace(@SearchStr,',','%_%')
SET @SearchStr= replace(@SearchStr,'-','%_%')
SET @SearchStr= replace(@SearchStr,'.','%_%')


--название уровней из БД
DECLARE @HierLevl1StringName varchar(200), 	@HierLevl2StringName varchar(200), 	@HierLevl3StringName varchar(200), 	@HierLevl4StringName varchar(200)

SELECT 
@HierLevl1StringName=lower(StringName)
from Dict_Hier_Names
where HierLevel=1
SELECT 
@HierLevl2StringName=lower(StringName)
from Dict_Hier_Names
where HierLevel=2
SELECT 
@HierLevl3StringName=lower(StringName)
from Dict_Hier_Names
where HierLevel=3
SELECT 
@HierLevl4StringName=lower(StringName)
from Dict_Hier_Names
where HierLevel=4
 	
		SELECT TOP 1000
		4 as ItemLevel, 
		case when @HierLevl4StringName IS NOT NULL THEN @HierLevl1StringName
			 when @HierLevl4StringName IS  NULL and PSType=2 THEN 'дом '+' '
			 when @HierLevl4StringName IS  NULL and PSType=1 THEN 'тр. ПС '+' '
			 when @HierLevl4StringName IS  NULL and PSType=0 THEN 'ПС '+' '
			 else ' ' END as ItemType,
		Dict_PS.StringName as StringName, 
		@HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
		@HierLevl2StringName+' '+Dict_HierLev2.StringName+', '+
		@HierLevl3StringName+' '+Dict_HierLev3.StringName+', '+
		case when @HierLevl4StringName IS NOT NULL THEN @HierLevl1StringName
			 when @HierLevl4StringName IS  NULL and PSType=2 THEN 'дом '+' '
			 when @HierLevl4StringName IS  NULL and PSType=1 THEN 'тр. ПС '+' '
			 when @HierLevl4StringName IS  NULL and PSType=0 THEN 'ПС '+' '
			 else ' ' END+
		Dict_PS.StringName as ItemFullAddress,
		Dict_PS.PS_ID as ObjectGUID,
		Dict_PS.HierLev3_ID as ParentGUID,
		NULL as HierLev1_ID,
		NULL as HierLev2_ID,
		NULL as HierLev3_ID,
		Dict_PS.PS_ID as PS_ID,
		case when FIAS_FullAddressToHierarchy.FullAddress_ID is not null then 1 else 0 end  as IsFIASBindExists
		FROM 
		Dict_HierLev1
		join Dict_HierLev2 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID
		JOIN Dict_HierLev3 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
		JOIN Dict_PS on Dict_HierLev3.HierLev3_ID= Dict_PS.HierLev3_ID
		left join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.PS_ID=Dict_PS.PS_ID
		WHERE
		(@SearchType=0 
			and (Dict_PS.StringName LIKE '%'+@SearchStr+'%'
					or 	@HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
						@HierLevl2StringName+' '+Dict_HierLev2.StringName+', '+
						@HierLevl3StringName+' '+Dict_HierLev3.StringName+', '+
						case when @HierLevl4StringName IS NOT NULL THEN @HierLevl1StringName
							 when @HierLevl4StringName IS  NULL and PSType=2 THEN 'дом '+' '
							 when @HierLevl4StringName IS  NULL and PSType=1 THEN 'траснформаторная ПС '+' '
							 when @HierLevl4StringName IS  NULL and PSType=0 THEN 'подстанция '+' '
							 else ' ' END+
						Dict_PS.StringName LIKE '%'+@SearchStr+'%'))
		OR
		(
		@SearchType=1 and @ItemLevel=3 AND Dict_PS.HierLev3_ID=@ParentGUID 
		)

		union
		SELECT  TOP 1000
		3 as ItemLevel, 
		@HierLevl3StringName as ItemType ,
		Dict_HierLev3.StringName	as StringName , 
		@HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
		@HierLevl2StringName+' '+Dict_HierLev2.StringName+', '+
		@HierLevl3StringName+' '+Dict_HierLev3.StringName	as ItemFullAddress,
		Dict_HierLev3.HierLev3_ID as ObjectGUID ,
		Dict_HierLev3.HierLev2_ID as ParentGUID ,
		NULL as HierLev1_ID ,
		NULL as HierLev2_ID ,
		Dict_HierLev3.HierLev3_ID as HierLev3_ID ,
		NULL as PS_ID ,
		case when FIAS_FullAddressToHierarchy.FullAddress_ID is not null then 1 else 0 end  as IsFIASBindExists
		FROM 
		Dict_HierLev1
		join Dict_HierLev2 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID
		JOIN Dict_HierLev3 on Dict_HierLev2.HierLev2_ID= Dict_HierLev3.HierLev2_ID
		left join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.HierLev3_ID=Dict_HierLev3.HierLev3_ID
		WHERE
		(@SearchType=0 
					and (Dict_HierLev3.StringName LIKE '%'+@SearchStr+'%'
							or @HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
							@HierLevl2StringName+' '+Dict_HierLev2.StringName+', '+
							@HierLevl3StringName+' '+Dict_HierLev3.StringName LIKE '%'+@SearchStr+'%'))
		OR
		(
		@SearchType=1 and @ItemLevel=2 AND Dict_HierLev3.HierLev2_ID=@ParentGUID
		)

		union
		SELECT  TOP 1000
		2 as ItemLevel , 
		@HierLevl2StringName as ItemType ,
		Dict_HierLev2.StringName as StringName , 
		@HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
		@HierLevl2StringName+' '+Dict_HierLev2.StringName as ItemFullAddress ,
		Dict_HierLev2.HierLev2_ID as ObjectGUID ,
		Dict_HierLev2.HierLev1_ID as ParentGUID ,
		NULL as HierLev1_ID ,
		Dict_HierLev2.HierLev2_ID as HierLev2_ID ,
		NULL as HierLev3_ID ,
		NULL as PS_ID ,
		case when FIAS_FullAddressToHierarchy.FullAddress_ID is not null then 1 else 0 end  as IsFIASBindExists
		FROM 
		Dict_HierLev1
		join Dict_HierLev2 on Dict_HierLev1.HierLev1_ID= Dict_HierLev2.HierLev1_ID
		left join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.HierLev2_ID=Dict_HierLev2.HierLev2_ID
		WHERE
		(@SearchType=0 
				and (
					Dict_HierLev2.StringName LIKE '%'+@SearchStr+'%'
						or @HierLevl1StringName+' '+Dict_HierLev1.StringName+', '+
						@HierLevl2StringName+' '+Dict_HierLev2.StringName LIKE '%'+@SearchStr+'%'
						))
		OR
		(
		@SearchType=1 and @ItemLevel=1 AND Dict_HierLev2.HierLev1_ID=@ParentGUID
		)

		union
		SELECT  TOP 1000
		1 as ItemLevel , 
		@HierLevl1StringName as ItemType ,
		Dict_HierLev1.StringName as StringName , 
		@HierLevl1StringName+' '+Dict_HierLev1.StringName as ItemFullAddress,
		Dict_HierLev1.HierLev1_ID as ObjectGUID ,
		null as ParentGUID ,
		Dict_HierLev1.HierLev1_ID as HierLev1_ID ,
		NULL as HierLev2_ID ,
		NULL as HierLev3_ID ,
		NULL as PS_ID,
		case when FIAS_FullAddressToHierarchy.FullAddress_ID is not null then 1 else 0 end  as IsFIASBindExists
		FROM 
		Dict_HierLev1
		left join FIAS_FullAddressToHierarchy on FIAS_FullAddressToHierarchy.HierLev1_ID=Dict_HierLev1.HierLev1_ID
		WHERE
			(@SearchType=0 
				and (
					Dict_HierLev1.StringName LIKE '%'+@SearchStr+'%'
					or @HierLevl1StringName+' '+Dict_HierLev1.StringName LIKE '%'+@SearchStr+'%'))
			OR
			(
			@SearchType=1 and @ItemLevel=0
			)

end
go

grant EXECUTE on dbo.usp_FIAS_SearchInHierarchy to UserDeclarator
go