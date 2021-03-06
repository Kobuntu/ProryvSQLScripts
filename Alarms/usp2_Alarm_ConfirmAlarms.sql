if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Alarm_ConfirmAlarms')
          and type in ('P','PC'))
   drop procedure usp2_Alarm_ConfirmAlarms
go

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
--		Февраль, 2013
--
-- Описание:
--
--		Подтверждение текущих тревог
--
-- ======================================================================================

create proc [dbo].[usp2_Alarm_ConfirmAlarms]
	@AlarmsArray varchar(4000),-- Идентификаторы аварий
	@User_ID varchar(22),
	@CUS_ID tinyint
as
begin

	set nocount on
	set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
	set numeric_roundabort off
	set transaction isolation level read uncommitted

	select Item as Alarm_ID
	into #Guids
	from dbo.usf2_Utils_SplitString(@AlarmsArray, ',')

	create table #UpdatedGuids
	(
		Alarm_ID uniqueidentifier
	)

	declare @dtNow DateTime;

	set @dtNow = GETDATE();
	
	BEGIN TRY  BEGIN TRANSACTION
		--Подтверждение в архиве
		if (@AlarmsArray is null or (@AlarmsArray is not null and len(@AlarmsArray)=0)) begin
			update Alarms_Archive set Confirmed = 1
			OUTPUT inserted.Alarm_ID into #UpdatedGuids
			where [User_ID] = @User_ID and Confirmed <> 1 
		end else begin
			update Alarms_Archive set Confirmed = 1
			OUTPUT inserted.Alarm_ID into #UpdatedGuids
			where Alarm_ID in (select Alarm_ID from #Guids)  and Confirmed <> 1
		end

		select * from #UpdatedGuids

		--Пишем в журнал подтверждений
		insert into Alarms_Archive_Confirm (Alarm_ID, ConfirmDateTime, CUS_ID, [User_ID])
		select Alarm_ID, @dtNow, @CUS_ID, @User_ID from #UpdatedGuids -- Выбираем только те что были подтверждены
		where Alarm_ID not in (select Alarm_ID from Alarms_Archive_Confirm)

		--Удаляем из журнала текущих
		delete from Alarms_Current
		where Alarm_ID in (select Alarm_ID from #UpdatedGuids)

	COMMIT	
END TRY
BEGIN CATCH
	--Ошибка, откатываем все изменения
	IF @@TRANCOUNT > 0 ROLLBACK 

	DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
	set @ErrMsg = ERROR_MESSAGE()
	set @ErrSeverity = 10 
	SELECT @ErrMsg 
	RAISERROR(@ErrMsg, @ErrSeverity, 1)
END CATCH
	
	drop table #Guids;
	drop table #UpdatedGuids;
end
go
   grant EXECUTE on usp2_Alarm_ConfirmAlarms to [UserCalcService]
go
