if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_SectionPowerExcess')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_SectionPowerExcess
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2010
--
-- Описание:
--
-- Выбор уставок по мощности для прямых потребителей в сечении
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_SectionPowerExcess] (
			@Section_ID int, 
			@DTStart DateTime,
			@DTEnd DateTime
			
)	
	RETURNS TABLE 
AS
RETURN
(
		--Выбираем прямых потребителей из сечения
		select dbo.ArchComm_DirectConsumer_Power.DirectConsumer_ID
			,dbo.ArchComm_DirectConsumer_Power.StartDateTime
			,dbo.ArchComm_DirectConsumer_Power.FinishDateTime
			,dbo.ArchComm_DirectConsumer_Power.PowerLimit
			,dbo.ArchComm_DirectConsumer_Power.PowerLimitType
		from dbo.Info_Section_Description2
		join dbo.Info_TP2 
			on Info_Section_Description2.TP_ID = Info_TP2.TP_ID
		join dbo.ArchComm_DirectConsumer_Power 
			on ArchComm_DirectConsumer_Power.DirectConsumer_ID = Info_TP2.DirectConsumer_ID
			and ArchComm_DirectConsumer_Power.StartDateTime <= @DTEnd 
			and ArchComm_DirectConsumer_Power.FinishDateTime >= @DTStart
		where Info_Section_Description2.Section_ID = @Section_ID

		--Объединяем с потреблением от РСК
		union 
		select -1,StartDateTime,FinishDateTime,PowerLimit,PowerLimitType
		from dbo.ArchComm_Section_Power
		where Section_ID = @Section_ID
			and StartDateTime <= @DTEnd 
			and FinishDateTime >= @DTStart
);
GO

grant select on usf2_Info_SectionPowerExcess to [UserCalcService]
GO