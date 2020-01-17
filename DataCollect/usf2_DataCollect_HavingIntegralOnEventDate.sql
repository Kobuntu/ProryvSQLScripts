set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_DataCollect_HavingIntegralOnEventDate')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_DataCollect_HavingIntegralOnEventDate
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель 2011
--
-- Описание:
--
--		Выбираем значения барабанов для модуля достоверности
--		0 - Данных нет за указанные сутки
--		48 - Данные есть
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_DataCollect_HavingIntegralOnEventDate]
(
	@TI_ID int,
	@TIType tinyint,
	@AbsentChannelsMask tinyint,
	@AIATSCode int,
	@AOATSCode int,
	@RIATSCode int,
	@ROATSCode int,
	@EventDate datetime
)
returns float 
AS
BEGIN

declare 
@ch tinyint,
@tariff_id int,
@result float,
@chCount float;

set @result = 0;

--Проверяем маску
if @AbsentChannelsMask is null OR (@AbsentChannelsMask & 1) = 0 set @ch = ISNULL(@AIATSCode, 1);
else if ((@AbsentChannelsMask / 2 ) & 1) = 0 set @ch = ISNULL(@AOATSCode, 2);
else set @ch = ISNULL(@AIATSCode, 1);

set @tariff_id = (select Tariff_ID from DictTariffs_ToTI 
where  TI_ID = @TI_ID and StartDateTime = (select MAX(StartDateTime) from DictTariffs_ToTI where TI_ID = @TI_ID and @EventDate between StartDateTime and FinishDateTime));

set @chCount = ISNULL((select COUNT(ChannelType1) from DictTariffs_Zones 
		where Tariff_ID = @tariff_id), 0) + 1;

if (@chCount > 4) set @chCount = 4

--Смотрим тип точки
if (@TIType = 11) begin

	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_1 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else if (@TIType = 12) begin
	--set @result = 48;
	
	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_2 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else if (@TIType = 13)begin

	--set @result = 48;
	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_3 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else if (@TIType = 14)begin

	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_4 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else if (@TIType = 15)begin

	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_5 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else if (@TIType = 16)begin

	set @result = (select COUNT(TI_ID) / @chCount * 48
	FROM dbo.ArchBit_Integrals_6 a
	join 
	(
		select case when @ch = 1 then ChannelType1 else ChannelType2 end as ChannelType from DictTariffs_Zones 
		where Tariff_ID = @tariff_id
		union all 
		select @ch as ChannelType
	) ch on a.ChannelType = ch.ChannelType
	where ti_id = @TI_ID and EventDateTime = @EventDate);
	
end else return @result;

RETURN @result;
END
go
grant EXECUTE on usf2_DataCollect_HavingIntegralOnEventDate to [UserCalcService]
go