set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchTech_Get_LastDateTime')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchTech_Get_LastDateTime
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2010
--
-- Описание:
--
--		Возвращаем время последнего значения в таблице тех. архивов
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ArchTech_Get_LastDateTime] (
		@TI_ID   int, --Идентификатор ТИ
		@ChannelType tinyint, -- Номер канала
		@DTNearLook DateTime,
		@TechProfilePeriod int = null, --Профиль
		@iteration smallint = 0 --Итерация
)

 RETURNS DateTime 
 AS BEGIN
declare
@lastEventDateTime DateTime,
@DTStart DateTime,
@DTEnd DateTime

--Проверяем инвертированность каналов
set @ChannelType = (select dbo.usf2_ReverseTariffChannel(0, @ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, @DTNearLook, @DTNearLook) from Info_TI where TI_ID=@TI_ID)

--Если не задан профиль то определяемся с таблицей где лежат значения
	if (@TechProfilePeriod is null) begin
		set @TechProfilePeriod = (select top 1 TechProfilePeriod
		from 
		dbo.Info_Meters_TO_TI im
		join dbo.Hard_Meters hm on im.Meter_ID = hm.Meter_ID
		where ti_id = @ti_id and (@DTNearLook between StartDateTime and FinishDateTime) and TechProfilePeriod is not null 
		order by StartDateTime desc)
	end;

if (@TechProfilePeriod is null) return null;

set @DTStart = DateAdd(day,-3, @DTNearLook);
set @DTEnd = DateAdd(day,3, @DTNearLook);

if (@TechProfilePeriod = 1) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_1Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 2;
END ELSE if (@TechProfilePeriod = 2) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_2Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 3;
END ELSE  if (@TechProfilePeriod = 3) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_3Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 5;
END ELSE  if (@TechProfilePeriod = 5) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_5Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 10;
END ELSE if (@TechProfilePeriod = 10) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_10Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 15;
END ELSE if (@TechProfilePeriod = 15) BEGIN
	select @lastEventDateTime = Max(EventDate) from  dbo.ArchTech_15Min_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and (EventDate between @DTStart and @DTEnd)
	set @TechProfilePeriod = 1;
END;

--Не нашли в нужной ищем там где лежат
if (@lastEventDateTime is null) begin
	if (@iteration < 6) begin
		set @lastEventDateTime = (select dbo.usf2_ArchTech_Get_LastDateTime(@TI_ID, @ChannelType, @DTNearLook, @TechProfilePeriod, @iteration + 1))
	end
end;

RETURN @lastEventDateTime
END;

go
grant EXECUTE on usf2_ArchTech_Get_LastDateTime to [UserCalcService]
go