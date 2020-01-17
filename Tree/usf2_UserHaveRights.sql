set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_UserHaveRights')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_UserHaveRights
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2018
--
-- Описание:
--
--		Перечень прав, которые имеет пользователь
--
-- ======================================================================================
CREATE FUNCTION [dbo].[usf2_UserHaveRights]
(
@userId varchar(22),
@objectId varchar(255),
@objectTypeName varchar(255)
)
RETURNS nvarchar(max)
AS
BEGIN

declare @rights nvarchar(max)
set @rights = ''
select @rights = @rights + convert(nvarchar(50), r.RIGHT_ID)  + ',' + STR(r.IsAssent,1) + '|' from Expl_User_UserGroup g  
join Expl_UserGroup_Right r on r.Deleted<>1 and  r.UserGroup_ID = g.UserGroup_ID
left join Expl_Users_DBObjects o on o.Deleted <>1 and o.ObjectTypeName<>'Dict_FreeHierarchyTypes' and o.ID = r.DBObject_ID
where g.User_ID=@userId and g.Deleted <> 1 and o.Object_ID = @objectId and ObjectTypeName = @objectTypeName

RETURN @rights;

END
go
grant EXECUTE on usf2_UserHaveRights to [UserCalcService]
go