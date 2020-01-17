if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetSubscribedMeterReading')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetSubscribedMeterReading
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2008
--
-- Описание:
--
--		Выбираем подписанные мастером счетчики со слэйва, обновляем информацию о последних отосланных данных
--
-- ======================================================================================
create proc [dbo].[usp2_SM61968_GetSubscribedMeterReading]

@Master_ID tinyint,
@DateNow dateTime,
@LastTI_ID int = 0

as
declare
@table30Name nvarchar(100),
@tableIntegrName nvarchar(100),
@tableEventName nvarchar(100),
@ParmDefinition NVARCHAR(1000),
@SQLString NVARCHAR(4000),
@DateStart DateTime;

begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

set @DateStart = DATEADD(day, -1, @DateNow);

select top 10 ti.TI_ID, mr.Meter_ID,mr.MRID,ti.TIType, mt.StartDateTime, mt.FinishDateTime, ISNULL(mr.LastEventsDateTime,@DateStart) as LastEventsDateTime
, ISNULL(mr.LastIntegralsDateTime,@DateStart) as LastIntegralsDateTime, ISNULL(mr.LastProfilesDateTime,@DateStart) as LastProfilesDateTime
, ISNULL(mr.LastQualDateTime,@DateStart) as LastQualDateTime
, ti.AIATSCode, ti.AOATSCode, ti.RIATSCode, ti.ROATSCode
into #tmp
from dbo.Slave61968_Sheduled_MeterReadings mr
join Info_Meters_TO_TI mt on mt.METER_ID = mr.Meter_ID
join Info_TI ti on mt.TI_ID = ti.TI_ID
where Master61968System_ID = @Master_ID and ti.TI_ID > @LastTI_ID

declare
@TI_ID int, @Meter_ID int, @TIType tinyint, @StartDateTime DateTime, @MRID varchar(max),
@FinishDateTime DateTime, @LastEventsDateTime DateTime, @LastIntegralsDateTime DateTime, @LastProfilesDateTime DateTime, @LastQualDateTime  DateTime,
@AIATSCode int, @AOATSCode int, @RIATSCode int, @ROATSCode int

SET @ParmDefinition = N'@TI_ID int, @Meter_ID int,@TIType tinyint, @LastDateTime DateTime, @AIATSCode int, @AOATSCode int, @RIATSCode int, @ROATSCode int';

BEGIN TRY  BEGIN TRANSACTION

--Смотрим все Счетчики
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select MRID, TI_ID, Meter_ID, TIType, StartDateTime, FinishDateTime
			, LastEventsDateTime, LastIntegralsDateTime, LastProfilesDateTime, LastQualDateTime 
			, AIATSCode, AOATSCode, RIATSCode, ROATSCode
			from #tmp
	  open t;
		FETCH NEXT FROM t into @MRID, @TI_ID, @Meter_ID, @TIType, @StartDateTime, @FinishDateTime, @LastEventsDateTime
			, @LastIntegralsDateTime, @LastProfilesDateTime, @LastQualDateTime
			, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode
		WHILE @@FETCH_STATUS = 0
		BEGIN
		
		select @MRID as mRID, @TI_ID as TI_ID, @Meter_ID as Meter_ID
		
		--Смотрим на тип точки
		if (@TIType < 10) begin
			set @table30Name = 'ArchComm_30_Values'
			set @tableIntegrName = 'ArchComm_Integrals'
			set @tableEventName = 'ArchComm_Events_Journal_TI'
		end else begin
			set @table30Name = 'ArchBit_30_Values_' + ltrim(str(@TIType - 10,2));
			set @tableIntegrName = 'ArchBit_Integrals_' + ltrim(str(@TIType - 10,2));
			set @tableEventName = 'ArchBit_Events_Journal_' + ltrim(str(@TIType - 10,2));
		end;
		
			--Смотрим тарифы, тарифные зоны и каналы и что они обозначают
			select tz.TariffZone_ID,  tz.ChannelType1, tz.ChannelType2, tz.ChannelType3, tz.ChannelType4,  @TI_ID as TI_ID
			from dbo.DictTariffs_ToTI ttt 
			left join dbo.DictTariffs_Zones tz on tz.Tariff_ID = ttt.Tariff_ID
			where ttt.TI_ID = @TI_ID and ttt.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.DictTariffs_ToTI
				where DictTariffs_ToTI.TI_ID = @TI_ID
				and DictTariffs_ToTI.StartDateTime <= @DateNow and (FinishDateTime is null OR @DateNow <= DictTariffs_ToTI.FinishDateTime)
			)

			--Сначала смотрим 30 минутки
			SET @SQLString = N'select ChannelType,@TIType as TIType,EventDate,ValidStatus,DispatchDateTime,
					val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
					val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
					val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
					val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
					val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48 from ' + @table30Name 
					+ ' where TI_ID = @TI_ID and DispatchDateTime > @LastDateTime';
					
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @LastProfilesDateTime, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode;
			
			--Теперь смотрим интегральные значения
			SET @SQLString = N'select ChannelType,@TIType as TIType,EventDateTime,Data,IntegralType,DispatchDateTime,Status from ' + @tableIntegrName 
				+ ' where TI_ID = @TI_ID and DispatchDateTime > @LastDateTime';
			
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @LastIntegralsDateTime, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode;
			
			--insert into #resultArchIntegral				
			--select * from #resultArchIntegralTmp
			
			--Теперь смотрим журналы событий
			SET @SQLString = N'select @TIType as TIType,EventDateTime,arch.EventCode,DispatchDateTime,ExtendedEventCode
			,case when arch.EventCode >=1000 then arch.Event61968Domain_ID else d.Event61968Domain_ID end as Event61968Domain_ID
			,case when arch.EventCode >=1000 then arch.Event61968DomainPart_ID else d.Event61968DomainPart_ID end as Event61968DomainPart_ID
			,case when arch.EventCode >=1000 then arch.Event61968Type_ID else d.Event61968Type_ID end as Event61968Type_ID
			,case when arch.EventCode >=1000 then arch.Event61968Index_ID else d.Event61968Index_ID end as Event61968Index_ID
			,arch.Event61968Param 
			 from ' + @tableEventName + ' arch 
			 left join Dict_TI_Journal_Event_Codes d on arch.EventCode=d.EventCode
			 where TI_ID = @TI_ID and DispatchDateTime > @LastDateTime and arch.EventCode >=0';
			
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @LastEventsDateTime, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode;
			
			----Теперь определяем максимальные DispatchDateTime
			--select @LastProfilesDateTime = Max(DispatchDateTime) from #resultArch30;
			--select @LastIntegralsDateTime = Max(DispatchDateTime) from #resultArchIntegral;
			--select @LastEventsDateTime = Max(DispatchDateTime) from #resultArchEvent;
			
			--update dbo.Slave61968_Sheduled_MeterReadings 
			--set LastProfilesDateTime = case when @LastProfilesDateTime is null then LastProfilesDateTime  else @LastProfilesDateTime end,
			--LastIntegralsDateTime = case when @LastIntegralsDateTime is null then LastIntegralsDateTime  else @LastIntegralsDateTime end,
			--LastEventsDateTime = case when @LastEventsDateTime is null then LastEventsDateTime  else @LastEventsDateTime end
			--where Master61968System_ID = @Master_ID and Meter_ID = @Meter_ID
			
			FETCH NEXT FROM t into @MRID,@TI_ID,@Meter_ID,@TIType, @StartDateTime, @FinishDateTime
				, @LastEventsDateTime, @LastIntegralsDateTime, @LastProfilesDateTime, @LastQualDateTime
				, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode
		END;
		CLOSE t
		DEALLOCATE t

		

		COMMIT
	END TRY	
	BEGIN CATCH
		drop table #TMP
		IF @@TRANCOUNT > 0 ROLLBACK 
		DECLARE @ErrMsg nvarchar(4000), @ErrSeverity int
		set @ErrMsg = ERROR_MESSAGE()
		set @ErrSeverity = 10 
		SELECT @ErrMsg 
		RAISERROR(@ErrMsg, @ErrSeverity, 1)
	END CATCH

drop table #tmp;
--drop table #resultArch30
--drop table #resultArchIntegral
--drop table #resultArchEvent


end
go
   grant EXECUTE on usp2_SM61968_GetSubscribedMeterReading to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetSubscribedMeterReading to [UserSlave61968Service]
go
grant EXECUTE on usp2_SM61968_GetSubscribedMeterReading to [UserMaster61968Service]
go
