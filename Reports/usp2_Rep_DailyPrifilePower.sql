if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Rep_DailyPrifilePower')
          and type in ('P','PC'))
   drop procedure usp2_Rep_DailyPrifilePower
go

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

---- ======================================================================================
---- Автор:
----
----		Малышев Игорь
----
---- Дата создания:
----
----		Сентябрь, 2012
----
---- Описание:
----
----		Суточный отчет (профиль) по счетчикам (1.11 и 1.12)
----
---- ======================================================================================
create proc [dbo].[usp2_Rep_DailyPrifilePower]

	@PSArray varchar(4000),
	@DTStart DateTime,
	@DTEnd DateTime,
	@DataSourceType tinyint = null,
	@ClosedPeriod_ID uniqueidentifier = null
as

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Информация по точкам
select distinct TI_ID, TIType, PS_ID,HierLev3_ID,HierLev2_ID,HierLev1_ID
,TIName,TPName,PSName,SectionNumber,H3Name,H2Name,H1Name
,MeterTypeName,MeterSerialNumber,AbonentCode,AbonentName 
from usf2_Rep_Info_TI(@PSArray, null);

go
  grant EXECUTE on usp2_Rep_DailyPrifilePower to [UserCalcService]
go