if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarm_WriteConfirmStatusesByAlarmIds')
          and type in ('P','PC'))
   drop procedure usp2_Alarm_WriteConfirmStatusesByAlarmIds
go


--Создаем тип, если его еще нет
IF  not EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'AlarmsConfirmStatus' AND ss.name = N'dbo')
CREATE TYPE [dbo].[AlarmsConfirmStatus]  AS TABLE (
	[Alarm_ID] [uniqueidentifier] NOT NULL,
	[AlarmConfirmStatus_UN] [uniqueidentifier] NULL,
	[User_ID] [varchar](22) NULL,
	[ConfirmStatusDateTime] [datetime] NULL,
	[AlarmConfirmStatusCategory_ID] [int] NULL,
	[Comment] [varchar](1024) NULL,
	[DateTime1] [datetime] NULL,
	[String1] [nvarchar](127) NULL)
go

grant EXECUTE on TYPE::AlarmsConfirmStatus to [UserCalcService]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2018
--
-- Описание:
--
--		Подтверждаем/изменяем статусы аварий
--
-- ======================================================================================
create proc [dbo].[usp2_Alarm_WriteConfirmStatusesByAlarmIds]
	@User_ID [dbo].[ABS_NUMBER_TYPE_2], --Пользователь
	@CUS_ID dbo.[CUS_ID_TYPE],
	@AlarmsConfirmStatuses AlarmsConfirmStatus READONLY --Аварии и статусы, которые квитируем/пишем
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare @nowDateTime DateTime;
set @nowDateTime = GETDATE();

BEGIN TRY BEGIN TRANSACTION

	--Пишем в таблицу Alarms_Archive_Confirm_Status, если AlarmConfirmStatus_UN != null
	MERGE Alarms_Archive_Confirm_Status AS a
	USING (select * from @AlarmsConfirmStatuses where AlarmConfirmStatus_UN is not null and AlarmConfirmStatus_UN <> '00000000-0000-0000-0000-000000000000') AS n 
	ON a.Alarm_ID = n.Alarm_ID and a.AlarmConfirmStatus_UN = n.AlarmConfirmStatus_UN
	WHEN MATCHED THEN 
		--Просто обновляем запись текущего статуса
		UPDATE SET [User_ID] = n.[User_ID], Comment = n.Comment, DateTime1 = n.DateTime1, String1 = n.String1 
	WHEN NOT MATCHED THEN 
		--Квитируем, с добавлением новой записи
		INSERT ([Alarm_ID],[AlarmConfirmStatus_UN],[User_ID],[ConfirmStatusDateTime]
			,[AlarmConfirmStatusCategory_ID],[Comment],[DateTime1],[String1])
		VALUES (n.[Alarm_ID],n.[AlarmConfirmStatus_UN],n.[User_ID],n.[ConfirmStatusDateTime]
			,n.[AlarmConfirmStatusCategory_ID],n.[Comment],n.[DateTime1],n.[String1]);


	---Это новые подтвержденные аварии
	create table #Confirmed
	(
		Alarm_ID uniqueidentifier NOT NULL,
		User_ID varchar(22) NULL,
	)

	--Подтверждаем в таблицу Alarms_Archive_Confirm
	MERGE Alarms_Archive_Confirm AS a
	USING @AlarmsConfirmStatuses AS n
	ON a.Alarm_ID = n.Alarm_ID
	WHEN MATCHED THEN 
		--Просто обновляем статус, больше ничего менять нельзя!
		UPDATE SET [AlarmConfirmStatus_UN] = case when n.[AlarmConfirmStatus_UN] = '00000000-0000-0000-0000-000000000000' then NULL else n.[AlarmConfirmStatus_UN] end
	WHEN NOT MATCHED THEN 
		--Квитируем, с добавлением новой записи
		INSERT ([Alarm_ID],[User_ID],[ConfirmDateTime],[CUS_ID],[AlarmConfirmStatus_UN])
		VALUES (n.[Alarm_ID],@User_ID,@nowDateTime,@CUS_ID,case when n.[AlarmConfirmStatus_UN] = '00000000-0000-0000-0000-000000000000' then NULL else n.[AlarmConfirmStatus_UN] end);
	--OUTPUT Inserted.Alarm_ID INTO #Confirmed;

	---Подтверждение
	--Обновляем Alarms_Archive поле Confirmed
	UPDATE [dbo].[Alarms_Archive] SET [Confirmed] = 1
	OUTPUT Inserted.Alarm_ID, Inserted.User_ID INTO #Confirmed
	where [USER_ID] = @User_ID and [Confirmed] <> 1 and [Alarm_ID] in (select [Alarm_ID] from @AlarmsConfirmStatuses)

	--Удаляем записи из таблицы Alarms_Current
	delete from [dbo].[Alarms_Current] 
	where [Alarm_ID] in (select [Alarm_ID] from #Confirmed)

	--Отправляем уведомление
	declare @alarmId nvarchar(50), @routing nvarchar(4000), @mqType nvarchar(100), @ID nvarchar(100), @alarmUserId [dbo].[ABS_NUMBER_TYPE_2];

	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select cast(Alarm_ID as nvarchar(50)), MQType, ID, [User_ID] from #Confirmed c
		outer apply [dbo].usf2_Alarm_ObjectInfo([c].Alarm_ID) o
    open t;
	FETCH NEXT FROM t into @alarmId,@mqType,@ID,@alarmUserId
	WHILE @@FETCH_STATUS = 0
	BEGIN

		set @routing =  @alarmUserId + '.Alarms_Archive_Confirm.' + @mqType + '.' + @ID + '.' + @alarmId

		exec spclr_MQ_TryPostMessage @alarmId, @routing, null

	FETCH NEXT FROM t into @alarmId,@mqType,@ID,@alarmUserId
	end;
	CLOSE t
	DEALLOCATE t

COMMIT
END TRY  
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK 
	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 16 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH

end

go
   grant EXECUTE on usp2_Alarm_WriteConfirmStatusesByAlarmIds to [UserCalcService]
go