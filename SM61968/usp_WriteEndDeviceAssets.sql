if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteEndDeviceAssets')
          and type in ('P','PC'))
 drop procedure usp2_WriteEndDeviceAssets
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'EndDeviceAssetsTableType' AND ss.name = N'dbo')
DROP TYPE [dbo].[EndDeviceAssetsTableType]
-- Пересоздаем заново
CREATE TYPE [dbo].[EndDeviceAssetsTableType] AS TABLE(
	[MRID] varchar(max) NOT NULL,
	[serialNumber] varchar(255) NOT NULL
)
GO

grant EXECUTE on TYPE::EndDeviceAssetsTableType to [UserMaster61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2011
--
-- Описание:
--
--		Обновяляем таблицы 
--		Master61968_SlaveSystems_EndDeviceAssets
--		Master61968_SlaveSystems_EndDeviceAssets_Description
--		Master61968_SlaveSystems_EndDeviceAssets_To_Meters
--
--		Данные со слэйв системы
-- ======================================================================================

create proc [dbo].[usp2_WriteEndDeviceAssets]
	@slave61968System_ID int, --Идетификатор Slave
	@EndDeviceAssetsTable EndDeviceAssetsTableType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

--Здесь необходимо проверять соотношение serialNumber -> MRID
-- Связка м\у MeterSerialNumber и MRID строится так
-- Hard_Meters (MeterSerialNumber, Meter_ID) -> 
-- Master61968_SlaveSystems_EndDeviceAssets_To_Meters (Meter_ID, Slave61968EndDeviceAsset_ID) ->
-- Master61968_SlaveSystems_EndDeviceAssets_Description (Slave61968EndDeviceAsset_ID, MRID)
--Выбираем все Slave61968EndDeviceAsset_ID у которых не совпадают присланные MRID c теми MRID которые прописаны у нас в базе
select distinct d.Slave61968EndDeviceAsset_ID 
into #tblMeterForDelete --Таблица всех записей в которых mrid сменился
from @EndDeviceAssetsTable ea
join Hard_Meters h on h.MeterSerialNumber = ea.serialNumber
join Master61968_SlaveSystems_EndDeviceAssets_To_Meters etm on etm.Meter_ID = h.Meter_ID
join Master61968_SlaveSystems_EndDeviceAssets_Description d on d.Slave61968EndDeviceAsset_ID = etm.Slave61968EndDeviceAsset_ID and ea.MRID <> d.MRID
	where h.Meter_ID not in (select Meter_ID from dbo.Hard_MetersE422_Links)
	and h.Meter_ID not in (select Meter_ID from dbo.Hard_MetersUSPD_Links)

--Удалем эти записи
if (select COUNT(*) from #tblMeterForDelete) > 0 begin
	delete from Master61968_SlaveSystems_EndDeviceAssets
	where Slave61968System_ID = @slave61968System_ID and Slave61968EndDeviceAsset_ID in (select Slave61968EndDeviceAsset_ID from #tblMeterForDelete);
end;

drop table #tblMeterForDelete;

--Выбираем всех кто еще не прописан в таблицах, по этой выборке будем добавлять или обновлять
select p.*, NEWID() as Slave61968EndDeviceAsset_ID into #tmp
from
(
	select distinct MRID, serialNumber, h.Meter_ID
	from 
	(
		select  MRID, serialNumber from @EndDeviceAssetsTable
		where MRID not in 
		(
			select MRID from 
			dbo.Master61968_SlaveSystems_EndDeviceAssets_Description d
			join dbo.Master61968_SlaveSystems_EndDeviceAssets a
			on d.Slave61968EndDeviceAsset_ID = a.Slave61968EndDeviceAsset_ID
			where a.Slave61968System_ID = @slave61968System_ID
		)
	) u
	join Hard_Meters h on h.MeterSerialNumber = u.serialNumber
	where h.Meter_ID not in (select Meter_ID from dbo.Hard_MetersE422_Links)
	and h.Meter_ID not in (select Meter_ID from dbo.Hard_MetersUSPD_Links)
) p


--Соединяем недостающее
MERGE dbo.Master61968_SlaveSystems_EndDeviceAssets AS a
USING (SELECT * FROM #tmp) AS n
ON a.Slave61968EndDeviceAsset_ID = n.Slave61968EndDeviceAsset_ID and a.Slave61968System_ID = @slave61968System_ID
WHEN NOT MATCHED THEN 
    INSERT (Slave61968EndDeviceAsset_ID
      ,Slave61968System_ID)
    VALUES (n.Slave61968EndDeviceAsset_ID
      ,@slave61968System_ID);

MERGE dbo.Master61968_SlaveSystems_EndDeviceAssets_Description AS a
USING (SELECT * FROM #tmp) AS n
ON a.MRID = n.MRID
WHEN MATCHED THEN 
 UPDATE SET DispatchDateTime = GetDate()
WHEN NOT MATCHED THEN 
    INSERT (Slave61968EndDeviceAsset_ID
      ,[MRID]
      ,[EndDeviceAssetDescription]
      ,[EndDeviceAssetStringDescription]
      ,[CreateDateTime]
      ,[DispatchDateTime]
      ,[Deleted]
      ,[CUS_ID])
    VALUES (n.Slave61968EndDeviceAsset_ID
      ,n.[MRID]
      ,'<test/>'
      ,''
      ,GetDate()
      ,GetDate()
      ,0
      ,0);
      
MERGE dbo.Master61968_SlaveSystems_EndDeviceAssets_To_Meters AS a
USING (SELECT * FROM #tmp) AS n
ON a.Meter_ID = n.Meter_ID
WHEN NOT MATCHED THEN 
    INSERT (Slave61968EndDeviceAsset_ID
      ,Meter_ID)
    VALUES (n.Slave61968EndDeviceAsset_ID
      ,Meter_ID);      


drop table #tmp;


--удаляем опрашиваемые напрямую !!!!!!!!
if(
select count(*) from Master61968_SlaveSystems_EndDeviceAssets where 
Slave61968System_ID=@slave61968System_ID and
Slave61968EndDeviceAsset_ID in (
select Slave61968EndDeviceAsset_ID from Master61968_SlaveSystems_EndDeviceAssets_To_Meters 
where Meter_ID in (select Meter_ID from Hard_MetersUSPD_Links) OR
Meter_ID in (select Meter_ID from Hard_MetersE422_Links))
)>0
begin

	delete from Master61968_SlaveSystems_EndDeviceAssets where 
	Slave61968System_ID=@slave61968System_ID and
	Slave61968EndDeviceAsset_ID in (
	select Slave61968EndDeviceAsset_ID from Master61968_SlaveSystems_EndDeviceAssets_To_Meters 
	where Meter_ID in (select Meter_ID from Hard_MetersUSPD_Links) OR
	Meter_ID in (select Meter_ID from Hard_MetersE422_Links))

end;



end
go
   grant EXECUTE on usp2_WriteEndDeviceAssets to [UserCalcService]
go

grant EXECUTE on usp2_WriteEndDeviceAssets to [UserMaster61968Service]
go
