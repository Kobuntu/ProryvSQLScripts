if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteUserGlobalSet')
          and type in ('P','PC'))
   drop procedure usp2_WriteUserGlobalSet
go
/****** Object:  StoredProcedure [dbo].[usp2_InfoFormulaSelect]    Script Date: 10/02/2008 22:40:23 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Обновление таблиц Expl_User_Global_Set
-- ======================================================================================
create proc [dbo].[usp2_WriteUserGlobalSet]
	@User_ID varchar(22), --Идентификатор пользователя
	@StringName varchar(255), --Название набора
	@IsGlobal bit, --Набор глобальный
	@List ntext, --Список объектов
	@IsReadOnly bit, --Набор доступен для редактированию только создавшему его пользователю (для глобальных)
	@VersionNumber smallint, --Версия сериализации/сжатия @List
	@UseProtoSerializer bit = null, --Используется прото сериализатор
	@ModuleNameForUse tinyint = null, --Модуль в АРМЕ (если нужно)
	@UserGlobalSet_ID uniqueidentifier = null --Идентификатор записи (null - новая запись)
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

BEGIN TRY  BEGIN TRANSACTION

declare @croosNamedMsg nvarchar(56);
set @croosNamedMsg = N'Уже есть набор с таким же названием. Измените название';

if (@UserGlobalSet_ID is null) begin --Это новая запись

	if (@IsGlobal = 0) begin
	--Проверяем что нет набора с таким названием для этого же пользователя
		if (exists(select top 1 1 from Expl_User_Global_Sets where (User_ID = @User_ID OR IsGlobal = 1) 
				and StringName = @StringName and (@ModuleNameForUse is null or @ModuleNameForUse = ModuleNameForUse))) begin
			RAISERROR(@croosNamedMsg, 16, 1);
			RETURN;
		end
	end else begin 
	--Проверяем что нет набора с таким названием у другого пользователя (надо ли это проверять, не понятно)
		if (exists(select top 1 1 from Expl_User_Global_Sets where StringName = @StringName)) begin
			RAISERROR(@croosNamedMsg, 16, 1);
			RETURN;
		end		
	end

	set @UserGlobalSet_ID = NEWID()
	
end else begin --Это обновление старой

	--Проверяем что нет набора с таким названием для этого же пользователя
		if (exists(select top 1 1 from Expl_User_Global_Sets where (@IsGlobal=1 OR IsGlobal = 1 OR User_ID = @User_ID)  
			and StringName = @StringName and UserGlobalSet_ID <> @UserGlobalSet_ID)) begin
			RAISERROR(@croosNamedMsg, 16, 1);
			RETURN;
		end

end

--Добавляем/обновляем записи
MERGE [dbo].[Expl_User_Global_Sets] as a USING (select @UserGlobalSet_ID as UserGlobalSet_ID, @User_ID as User_ID, @StringName as StringName,
		@IsGlobal as IsGlobal, @List as List, @UseProtoSerializer as UseProtoSerializer, @ModuleNameForUse as ModuleNameForUse, 
		@IsReadOnly as IsReadOnly, @VersionNumber as VersionNumber) n
	ON a.UserGlobalSet_ID=n.UserGlobalSet_ID 
	WHEN MATCHED THEN UPDATE SET [StringName]=n.StringName,[IsGlobal]=n.IsGlobal,[List]=n.List,[UseProtoSerializer]=n.UseProtoSerializer,
		[ModuleNameForUse]=n.ModuleNameForUse,[IsReadOnly]=n.IsReadOnly,[VersionNumber]=n.VersionNumber, [User_ID]=ISNULL(n.[User_ID], a.[User_ID])
	WHEN NOT MATCHED THEN 
	INSERT ([UserGlobalSet_ID],[User_ID],[StringName],[IsGlobal],[List],[UseProtoSerializer],[ModuleNameForUse],[IsReadOnly],[VersionNumber])
	VALUES (n.[UserGlobalSet_ID],n.[User_ID],n.[StringName],n.[IsGlobal],n.[List],n.[UseProtoSerializer],n.[ModuleNameForUse],n.[IsReadOnly],n.[VersionNumber]);


--Запись в журнал о добавлении/обновлении
insert into Expl_User_Journal (ApplicationType, CommentString, CUS_ID, EventDateTime, EventString, User_ID, ObjectID, ObjectName)
values (1, @StringName, 0, GETDATE(), 'Изменение/добавление набора', @User_ID, @UserGlobalSet_ID, 'Expl_User_Global_Sets');

select @UserGlobalSet_ID --Возвращаем идентификатор записи

COMMIT	
END TRY
BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
end
go
   grant EXECUTE on usp2_WriteUserGlobalSet to [UserCalcService]
go