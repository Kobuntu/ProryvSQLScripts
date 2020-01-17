if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Rep_Info_TI')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Rep_Info_TI
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
--		Август 2012
--
-- Описание:
--
--		Выбираем информацию по ТИ для отчетов
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Rep_Info_TI]
(
	@PSArray varchar(4000),
	@TI_ID int = null
)
returns @tableTI TABLE
(
 TI_ID int,
 TIType tinyint,
 PS_ID int,
 HierLev3_ID int,
 HierLev2_ID int,
 HierLev1_ID int,
 TIName varchar(1024),
 
 TPName varchar(255),
 PSName varchar(255),
 SectionNumber int,
 H3Name varchar(255),
 H2Name varchar(255),
 H1Name varchar(255),
 
 MeterTypeName varchar(128),
 MeterSerialNumber varchar(255),
 
 AbonentCode varchar(128),
 AbonentName varchar(255),	
 
 MeterExtendedTypeName varchar (128),
 PowerLineName varchar (255),

 PRIMARY KEY CLUSTERED ([TIType], [ti_id])
		WITH (ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF)
)
AS
BEGIN
	insert into @tableTI 
	select ti.TI_ID, ti.TIType, ti.PS_ID, h3.HierLev3_ID, h2.HierLev2_ID, h1.HierLev1_ID, TIName, 
	-- Номер ТП
	case 
		when ps.PSType <> 2 then ps.StringName --Это ТП, просто отображаем название 
		else (select StringName from Dict_PS where PS_ID = dbo.usf2_Dict_PS_SearchPowerSupplyForPS(ps.PS_ID)) --Это дом, берем название питающей подстанции
	end as TPName, 
	-- Название дома
	case 
		when ps.PSType = 1 then '' 
		else ps.StringName --Отображаем название только если это дом
	end as PSName, 
	ti.SectionNumber, --Номер секции
	case when ps.PSType = 1 then '' else h3.StringName end as H3Name, -- Улица определена только для дома
	h2.StringName as H2Name,
	h1.StringName as H1Name,
	mt.MeterTypeName, hm.MeterSerialNumber,
		(select top 1 BitAbonentCode from dbo.InfoBit_Abonents_To_TI ati 
			join dbo.InfoBit_Abonents_List al on ati.BitAbonent_ID = al.BitAbonent_ID
			where ati.TI_ID = ti.TI_ID 			
			and ati.StartDateTime=(select MAX(StartDateTime) from dbo.InfoBit_Abonents_To_TI where TI_ID = ti.TI_ID)
			)as AbonentCode, 
		(select top 1 BitAbonentSurname + ' ' + REPLACE(SUBSTRING(BitAbonentName, 1, 1),'-','') + ' ' + SUBSTRING(BitAbonentMiddleName, 1, 1) from dbo.InfoBit_Abonents_To_TI ati 
			join dbo.InfoBit_Abonents_List al on ati.BitAbonent_ID = al.BitAbonent_ID
			where ati.TI_ID = ti.TI_ID 			
			and ati.StartDateTime=(select MAX(StartDateTime) from dbo.InfoBit_Abonents_To_TI where TI_ID = ti.TI_ID)
			) as AbonentName,
	met.MeterExtendedTypeName,
	pl.StringName as PowerLineName 
	from Info_TI ti
	join Dict_PS ps on ti.PS_ID = ps.PS_ID
	join Dict_HierLev3 h3 on h3.HierLev3_ID = ps.HierLev3_ID
	join Dict_HierLev2 h2 on h2.HierLev2_ID = h3.HierLev2_ID
	join Dict_HierLev1 h1 on h1.HierLev1_ID = h2.HierLev1_ID
	left join Info_Meters_TO_TI mti on mti.TI_ID = ti.TI_ID and StartDateTime = (
	select MAX(StartDateTime) from Info_Meters_TO_TI where TI_ID = ti.TI_ID)
	left join Hard_Meters hm on hm.Meter_ID = mti.METER_ID
	left join dbo.Dict_Meters_Types mt on mt.MeterType_ID = hm.MeterType_ID
	left join dbo.Dict_Meters_Extended_Types met on met.MeterExtendedType_ID = hm.MeterExtendedType_ID and met.MeterType_ID = hm.MeterType_ID
	left join Dict_PowerLines pl on pl.PLine_ID = ti.PLine_ID
	where ((@TI_ID is null and ti.PS_ID in (select * from usf2_Utils_Split(@PSArray, ','))) OR (@TI_ID is not null and ti.TI_ID = @TI_ID))
	and ISNULL(Deleted, 0) = 0 
	RETURN
END
go
   grant select on usf2_Rep_Info_TI to [UserCalcService]
go