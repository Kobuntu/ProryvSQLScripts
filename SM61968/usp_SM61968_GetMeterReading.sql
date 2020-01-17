if exists (select 1
          from sysobjects
          where  id = object_id('usp2_SM61968_GetMeterReading')
          and type in ('P','PC'))
 drop procedure usp2_SM61968_GetMeterReading
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
--		Выбираем данные по одной точке за промежуток времени
--
-- ======================================================================================
create proc [dbo].[usp2_SM61968_GetMeterReading]

@Meter_ID int,
@DTStart dateTime,
@DTEnd dateTime

as
declare
@table30Name nvarchar(100),
@tableIntegrName nvarchar(100),
@tableEventName nvarchar(100),
@ParmDefinition NVARCHAR(1000),
@SQLString NVARCHAR(4000),
@DateStart DateTime;

begin

declare
@TI_ID int, @TIType tinyint, @StartDateTime DateTime, @FinishDateTime DateTime, 
@AIATSCode int, @AOATSCode int, @RIATSCode int, @ROATSCode int

--Выбираем все ТИ привязанные к данному счетчику
select ti.TI_ID, mt.Meter_ID, ti.TIType, mt.StartDateTime, mt.FinishDateTime
,ti.AIATSCode, ti.AOATSCode, ti.RIATSCode, ti.ROATSCode
into #tmp
from Info_Meters_TO_TI mt 
join Info_TI ti on mt.TI_ID = ti.TI_ID
where mt.Meter_ID = @Meter_ID and mt.StartDateTime <= @DTEnd and FinishDateTime >=@DTStart;

SET @ParmDefinition = N'@TI_ID int, @Meter_ID int,@TIType tinyint, @AIATSCode int, @AOATSCode int, @RIATSCode int, @ROATSCode int, @DTStart dateTime, @DTEnd dateTime';

select @Meter_ID as Meter_ID, COUNT(*) as [countTI] from #tmp;

declare 
@dt1 DateTime, 
@dt2 DateTime,
@dt3 DateTime;

--Смотрим все ТИ привязанные к этому счетчику
	declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID, Meter_ID, TIType, StartDateTime, FinishDateTime
						, AIATSCode, AOATSCode, RIATSCode, ROATSCode
			from #tmp
	  open t;
		FETCH NEXT FROM t into @TI_ID, @Meter_ID, @TIType, @StartDateTime, @FinishDateTime
			,@AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode
		WHILE @@FETCH_STATUS = 0
		BEGIN
		
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
		
		--Теперь смотрим дату, время привязки ТИ к счетчику 
		if (@StartDateTime > @DTStart) 	set  @dt1 = @StartDateTime;
		else set  @dt1 = @DTStart; 
		
		if (@FinishDateTime < @DTEnd) set  @dt2 = @FinishDateTime;
		else set  @dt2 = @DTEnd; 
			
			--Смотрим тарифы, тарифные зоны и каналы и что они обозначают
			select tz.TariffZone_ID,  tz.ChannelType1, tz.ChannelType2, tz.ChannelType3, tz.ChannelType4, @TI_ID as  TI_ID
			from dbo.DictTariffs_ToTI ttt 
			left join dbo.DictTariffs_Zones tz on tz.Tariff_ID = ttt.Tariff_ID
			where ttt.TI_ID = @TI_ID and ttt.StartDateTime = 
			(
				select max(StartDateTime)
				from dbo.DictTariffs_ToTI
				where DictTariffs_ToTI.TI_ID = @TI_ID
				and DictTariffs_ToTI.StartDateTime <= @dt2 and (FinishDateTime is null OR @dt1 <= DictTariffs_ToTI.FinishDateTime)
			)
			
			--Сначала смотрим 30 минутки
			SET @SQLString = N'select ChannelType,@TIType as TIType,EventDate,ValidStatus,DispatchDateTime,
					val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
					val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
					val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
					val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
					val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48 from ' + @table30Name 
					+ ' where TI_ID = @TI_ID and EventDate between @DTStart and @DTEnd';
			
			set @dt3 = Floor(Cast(@dt1 as float));
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode, @dt3, @dt2;
			
			--Теперь смотрим интегральные значения
			SET @SQLString = N'select ChannelType,@TIType as TIType,EventDateTime,Data,IntegralType,DispatchDateTime,Status from ' + @tableIntegrName 
				+ ' where TI_ID = @TI_ID and EventDateTime between @DTStart and @DTEnd';
			
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode, @dt1, @dt2;
			
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
			where TI_ID = @TI_ID and EventDateTime between @DTStart and @DTEnd and arch.EventCode >= 0';
			
			EXEC sp_executesql @SQLString, @ParmDefinition, @TI_ID, @Meter_ID, @TIType, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode, @dt1, @dt2;
			
			FETCH NEXT FROM t into @TI_ID, @Meter_ID, @TIType, @StartDateTime, @FinishDateTime
								, @AIATSCode, @AOATSCode, @RIATSCode, @ROATSCode
		END;
		CLOSE t
		DEALLOCATE t


drop table #tmp;


end
go
   grant EXECUTE on usp2_SM61968_GetMeterReading to [UserCalcService]
go
grant EXECUTE on usp2_SM61968_GetMeterReading to [UserSlave61968Service]
go
grant EXECUTE on usp2_SM61968_GetMeterReading to [UserMaster61968Service]
go
