if exists (select 1
          from sysobjects
          where  id = object_id('vw_ArchIntegrals')
          and type in ('V'))
   drop view vw_ArchIntegrals
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2019
--
-- Описание:
--
--		Журнал событий ТИ
--
-- ======================================================================================
CREATE view [dbo].[vw_ArchIntegrals] 
WITH SCHEMABINDING
AS
	select m.MeterSerialNumber, arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data
	from
	(
		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		[dbo].[ArchCalc_Integrals_Virtual] arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_1 arch with (nolock)
	
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_2 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_3 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_4 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_5 arch with (nolock)
		
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_6 arch with (nolock)
		
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_7 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_8 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_9 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_10 arch with (nolock)
		
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_11 arch with (nolock)
	
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_12 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_13 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_14 arch with (nolock)

		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_15 arch with (nolock)
		
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_16 arch with (nolock)
		
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_17 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_18 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_19 arch with (nolock)
				
		union all

		select arch.TI_ID, arch.EventDateTime, arch.[ChannelType], arch.Data, arch.IntegralType
		from 
		dbo.ArchCalcBit_Integrals_Virtual_20 arch with (nolock)
	) arch
	cross apply
	(
		select top 1 m.MeterSerialNumber from 
		dbo.Info_Meters_TO_TI im 
		join [dbo].[Hard_Meters] m on m.Meter_ID = im.METER_ID
		where im.TI_ID = arch.TI_ID and im.StartDateTime <= GETDATE() and im.FinishDateTime >= GETDATE()
		order by im.StartDateTime
	) m
	--where arch.IntegralType = 0
GO

   grant select on vw_ArchIntegrals to [UserCalcService]
go