if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Hard_Meters_Addx_RemoteDisplay')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Hard_Meters_Addx_RemoteDisplay
go
/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/18/2008 11:53:14 ******/
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
--		Март, 2017
--
-- Описание:
--
--		Читаем номер дисплея для счетчика матрица
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Hard_Meters_Addx_RemoteDisplay] (
		@MeterSerialNumber  varchar (255)--
)

RETURNS varchar (255)
AS BEGIN
	return @MeterSerialNumber + ISNULL((select top 1 ', №дисп.  ' + convert(varchar,AddxRemoteDisplay_ID) from  dbo.Hard_Meters_Addx_RemoteDisplay
	where  convert(varchar,AddxDevice_ID) like @MeterSerialNumber), '')
END;
go
   grant exec on usf2_Hard_Meters_Addx_RemoteDisplay to [UserCalcService]
go