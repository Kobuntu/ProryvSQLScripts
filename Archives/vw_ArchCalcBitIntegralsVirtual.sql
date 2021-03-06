if exists (select 1
          from sysobjects
          where  id = object_id('vw_ArchCalcBitIntegralsVirtual')
          and type in ('V'))
   drop view vw_ArchCalcBitIntegralsVirtual
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2019
--
-- Описание:
--
--		Интегральные значения ТИ
--
-- ======================================================================================
CREATE view [dbo].[vw_ArchCalcBitIntegralsVirtual] 
WITH SCHEMABINDING
AS
	select 0 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalc_Integrals_Virtual a with (nolock) 

	union all

	select 11 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_1 a with (nolock) 

	union all

	select 12 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_2 a with (nolock) 

	union all

	select 13 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_3 a with (nolock) 

	union all

	select 14 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_4 a with (nolock) 

	union all

	select 15 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_5 a with (nolock) 

	union all

	select 16 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_6 a with (nolock) 

	union all

	select 17 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_7 a with (nolock) 

	union all

	select 18 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_8 a with (nolock) 

	union all

	select 19 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_9 a with (nolock) 

	union all

	select 20 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_10 a with (nolock) 
	union all

	select 21 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_11 a with (nolock) 

	union all

	select 22 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_12 a with (nolock) 

	union all

	select 23 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_13 a with (nolock) 

	union all

	select 24 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_14 a with (nolock) 

	union all

	select 25 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_15 a with (nolock) 

	union all

	select 26 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_16 a with (nolock) 

	union all

	select 27 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_17 a with (nolock) 

	union all

	select 28 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_18 a with (nolock) 

	union all

	select 29 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_19 a with (nolock) 

	union all

	select 30 as TiType, TI_ID, ChannelType, DataSource_ID, EventDateTime, 
	[Data], [IntegralType], DispatchDateTime, [Status], ManualEnterData, IsUsedForFillHalfHours 
	from dbo.ArchCalcBit_Integrals_Virtual_20 a with (nolock) 

	
GO

   grant select on vw_ArchCalcBitIntegralsVirtual to [UserCalcService]
go