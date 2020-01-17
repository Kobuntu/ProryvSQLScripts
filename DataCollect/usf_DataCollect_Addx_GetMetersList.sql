set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf_DataCollect_Addx_GetMetersList')
          and type in ('IF', 'FN', 'TF'))
   drop function usf_DataCollect_Addx_GetMetersList
go


create FUNCTION [dbo].[usf_DataCollect_Addx_GetMetersList]
(
   @RootRouter_ID int,
   @AddxID bigint
)
RETURNS @ret 
TABLE
(
Meter_ID METER_ID_TYPE not null,
AddxDevice_ID ADDXDEVICE_ID_TYPE not null,
MeterType_ID int not null,
MeterExtendedType_ID METEREXTENDEDTYPE_ID_TYPE null
)
AS
begin

WITH subtree
AS
(
select AddxDevice_ID,ParentDevice_ID,DispatchDateTime
from JournalDataCollect_Addx_DeviceParams
where AddxDevice_ID = @AddxID and RootRouter_ID=@RootRouter_ID
and DispatchDateTime>DATEADD(dd,-3,GETDATE())
union all
select e.AddxDevice_ID, e.ParentDevice_ID,e.DispatchDateTime
from JournalDataCollect_Addx_DeviceParams
AS e join subtree on (subtree.AddxDevice_ID=e.ParentDevice_ID
and e.RootRouter_ID=@RootRouter_ID
and e.DispatchDateTime>DATEADD(dd,-3,GETDATE())))
insert into @ret
select Hard_Meters.meter_id,subtree.AddxDevice_ID,Hard_Meters.MeterType_ID,Hard_Meters.MeterExtendedType_ID
from subtree
join Hard_Meters on LTRIM(RTRIM(Hard_Meters.MeterSerialNumber))=cast(subtree.AddxDevice_ID as varchar)
join Hard_Meters_Addx_Info on subtree.AddxDevice_ID=Hard_Meters_Addx_Info.AddxDevice_ID
where Hard_Meters_Addx_Info.Producer is not null and Hard_Meters_Addx_Info.DevTypeID is not null
and Hard_Meters_Addx_Info.SubType1 is not null and Hard_Meters_Addx_Info.SubType2 is not null
return
END

go
grant SELECT  on dbo.usf_DataCollect_Addx_GetMetersList to UserCalcService
GO
grant SELECT  on dbo.usf_DataCollect_Addx_GetMetersList to UserDataCollectorService
GO