if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchTech_UpdateOrInsertDG')
          and type in ('P','PC'))
   drop procedure usp2_ArchTech_UpdateOrInsertDG
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
--		Январь, 2011
--
-- Описание:
--
--		Сохранение диспетчерского графика
--
-- ======================================================================================

create proc [dbo].[usp2_ArchTech_UpdateOrInsertDG]
	@Event_Array varchar(4000),-- 'EventDateTime|VAL|MAX_VAL|MIN_VAL;'
	@TP_ID   int, --Идентификатор ТИ
	@ChannelType tinyint, -- Номер канала
	@PredispatchSchedulerType tinyint,
	@CUS_ID tinyint
	

as
begin
select * into #t from usf2_Utils_iter_threefloatlist_to_table(@Event_Array)
--Вставляем то чего не хватает
insert into dbo.ArchTech_PredispatchSchedule (TP_ID, ChannelType, EventDateTime, PredispatchSchedulerType, VAL, MaxVal, MinVal, CUS_ID)
select @TP_ID, @ChannelType, #t.EventDate, @PredispatchSchedulerType, #t.Float1, #t.Float2, #t.Float3, @CUS_ID 
from #t
WHERE not exists (select * from dbo.ArchTech_PredispatchSchedule 
where 
ArchTech_PredispatchSchedule.TP_ID = @TP_ID 
and ArchTech_PredispatchSchedule.ChannelType = @ChannelType 
and ArchTech_PredispatchSchedule.EventDateTime = #t.EventDate
and ArchTech_PredispatchSchedule.PredispatchSchedulerType = @PredispatchSchedulerType
);

--Обновляем уже существующее
update dbo.ArchTech_PredispatchSchedule set
VAL = #t.Float1, MAXVAL = #t.Float2, MINVAL = #t.Float3, CUS_ID = @CUS_ID
from #t
where 
ArchTech_PredispatchSchedule.TP_ID = @TP_ID 
and ArchTech_PredispatchSchedule.ChannelType = @ChannelType 
and ArchTech_PredispatchSchedule.EventDateTime = #t.EventDate
and ArchTech_PredispatchSchedule.PredispatchSchedulerType = @PredispatchSchedulerType
and (IsLock is null);

drop table #t

end

go
   grant EXECUTE on usp2_ArchTech_UpdateOrInsertDG to [UserCalcService]
go