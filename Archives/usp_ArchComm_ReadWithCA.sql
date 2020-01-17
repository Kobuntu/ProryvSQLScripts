if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_ReadWithCA')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_ReadWithCA
go

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
--		Январь, 2013
--
-- Описание:
--
--		Основная процедура чтения архивных данных по одной точке
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_ReadWithCA]

	@TI_ID int,  --Идентификатор ТИ
	@DateStart datetime, --Время начала выборки
	@DateEnd datetime, -- Время конца выборки
	@DataSourceType tinyint = null, -- Тип источника, если не указывается то в порядке приоритета
	@ClosedPeriod_ID uniqueidentifier = null, --Закрытый период, если не указан, читаем из таблицы расчетного профиля
	@isCoeffEnabled bit, -- 1 - Домножать на коэффициент трансформации, 0 - не домножать
	@IsCAEnabled bit, -- Брать ли значения КА (только для точек ФСК)
	@IsOVEnabled bit, -- Брать ли значения ОВ
	@ChannelType tinyint, -- Номер канала
	@IsCA bit, -- 1 - Эта точка является контрагентом,
	@isOVIntervalEnabled bit, -- Показывать ли время когда точка замещала другие точки
	@isValidateOtherDataSource bit = null, -- Нужны достоверности по остальным источникам
	@IsReadCalculatedValues bit, --Читаем расчетные данные
	@IsOVon bit,
	@TIType tinyint,
	@UseInactiveChannel bit = 0, -- Отображать отключенные каналы
	@excludedActUndercountUns varchar(max) = NULL, --Список идентификаторов актов недоучета, которые исключаем из чтения (для модуля ручного ввода по акту недоучета)
	@isReturnPreviousDispatchDateTime bit = 0, --Возвращать DispatchDateTime предыдущего поступления данных
	@IsChannelsInverted bit = 0, --Признак инвертированности каналов
	@UseActUndercount bit = 1, -- Получать данные по акту недочета
	@IsAbsentChannel bit = 0

as
begin

declare @ovT table
			(
				TI_ID int, 
				OV_ID int, 
				StartDateTime datetime,
				FinishDateTime datetime,
				TPCoef float null, 
				AIATSCode int null,
				AOATSCode int null,
				RIATSCode int null,
				ROATSCode int null, 
				IsCoeffTransformationDisabled bit,
				TIType tinyint,
				TIName nvarchar(1024)
			);

	--Продолжаем дальше смотреть только при наличии канала
	if (@UseInactiveChannel = 1 OR @IsAbsentChannel = 0) begin 
	
		--Берем список контрагентов
		if (@IsCAEnabled=1) and (@IsCA=0)   begin
		
			create table #caT(TP_Coef float, ContrTI_ID int, CA_Coef float);
			insert into  #caT (TP_Coef, ContrTI_ID, CA_Coef)
			select TP_Coef, ContrTI_ID, CA_Coef from
			(select TPCoefContr as TP_Coef, ContrTI_ID, Coef as CA_Coef, TP_ID2  from Info_Contr_TI ca
			right join (select TP_ID from info_TP2 where TP_ID=(select top 1 tp_id from Info_TI where ti_id = @ti_id)) tp
			on ca.TP_ID2=tp.TP_ID) tt
		
			declare 
			@ID int
			set @ID =(select top(1)ContrTI_ID from #caT) --КА может быть только один
		end
		--Берем список обходных выключателей 
		if (@IsOVEnabled=1) begin
			if (@IsCA=0)  begin --для точек ФСК
				insert into  @ovT (TI_ID, OV_ID, StartDateTime, FinishDateTime,TPCoef,AIATSCode,AOATSCode,RIATSCode,ROATSCode, IsCoeffTransformationDisabled, TIType, TIName)
				select ov.TI_ID, ovl.TI_ID as OV_ID, 
					case when sw.StartDateTime < @DateStart then @DateStart else sw.StartDateTime end as StartDateTime,
					case when sw.FinishDateTime > @DateEnd then @DateEnd else sw.FinishDateTime end as FinishDateTime,
					TPCoefOurSide,AIATSCode, AOATSCode, RIATSCode, ROATSCode, IsCoeffTransformationDisabled, TIType, TIName
					from Hard_OV_Positions_List ov
					join Hard_OV_List ovl on ov.OV_ID = ovl.OV_ID
					join ArchComm_OV_Switches sw WITH (NOLOCK) on ovl.OV_ID=sw.OV_ID and ov.OVPosition_ID=sw.OVPosition_ID
					join Info_TI on Info_TI.TI_ID = ovl.TI_ID
					where ov.TI_ID=@TI_ID and sw.StartDateTime <=  @DateEnd and sw.FinishDateTime > @DateStart
			end else begin -- для точек КА
				insert into  @ovT (TI_ID, OV_ID, StartDateTime, FinishDateTime,TPCoef,AIATSCode,AOATSCode,RIATSCode,ROATSCode, IsCoeffTransformationDisabled)
				exec usp2_Hard_OV_List_CA @TI_ID,@DateStart,@DateEnd
			end
		end
		--Если точка ОВ, надо вывести когда и кого замещала
		if (@IsOVon=1) begin
			if (@IsCA=0) begin

				select ovl.OV_ID,hopl.TI_ID,aw.StartDateTime,aw.FinishDateTime  from 
				(select OV_ID from Hard_OV_List  WITH (NOLOCK) where TI_ID = @TI_ID) ovl
				left join ArchComm_OV_Switches aw
				on ovl.OV_ID = aw.OV_ID and @DateStart <= FinishDateTime and @DateEnd >= StartDateTime
				left join dbo.Hard_OV_Positions_List hopl
				on hopl.OV_ID = ovl.OV_ID and aw.OVPosition_ID = hopl.OVPosition_ID

			end else exec usp2_Hard_TI_List_CA @TI_ID, @DateStart,@DateEnd
		end

		--Получасовки
		--if (@IsCA = 1) begin
		--	--Это контрагент, у него нет закрытого периода
		--	exec usp2_ArchComm_Contr_Select2 @TI_ID, @DateStart,@DateEnd,@ChannelType,@isCoeffEnabled,0,0 --TODO здесь надо доработать переворот канала
		--end else begin

					
					--Считываем значения по акту недоучета
					if (@IsReadCalculatedValues = 1 AND @UseActUndercount = 1) begin --Для основного профиля не нужны данные по актам недоучета
						declare @numbersMinutes float, @finishDate DateTime;
						set @finishDate = DateAdd(minute, 30, @DateEnd);
						--Читаем акт недоучета
						SELECT 
							case when @DateStart >= [StartDateTime] then cast(0 as float) 
								else cast(DATEDIFF(minute, @DateStart, [StartDateTime]) as float) / 30 end as StartIndex,
							(case when @finishDate < [FinishDateTime] then cast(DATEDIFF(minute, dbo.usf2_Utils_DateTimeRoundToHalfHour(@DateStart,1), dbo.usf2_Utils_DateTimeRoundToHalfHour(@finishDate, 1)) as float) / 30
								else cast(DATEDIFF(minute, @DateStart, [FinishDateTime]) as float) / 30 - (case when IsFinishDateTimeInclusive = 0 then 1 else 0 end) end) as FinishIndex,
								case when DATEDIFF(minute, StartDateTime, FinishDateTime) < 1 then AddedValue else
						[AddedValue] / cast(DATEDIFF(minute, StartDateTime, (case when IsFinishDateTimeInclusive = 0 then FinishDateTime else DATEADD(minute, 1, FinishDateTime) end)) as float) * 30 end  as HalfHourValue,--Значение добавляемое к каждой получаовке 
						ActMode, ActUndercount_UN, [IsCoeffTransformationEnabled], [IsLossesCoefficientEnabled]
						into #acts
						from
						(
							select StartDateTime, AddedValue,
							FinishDateTime, ActMode,ActUndercount_UN,
							[IsCoeffTransformationEnabled], [IsLossesCoefficientEnabled], IsFinishDateTimeInclusive
							from [dbo].[ArchCalc_Replace_ActUndercount]
							WHERE  [TI_ID] = @TI_ID AND [ChannelType]=@ChannelType AND StartDateTime < @finishDate AND FinishDateTime >= @DateStart
							AND (IsInactive is null OR IsInactive = 0)
							AND (@excludedActUndercountUns is null OR ActUndercount_UN not in (select distinct Item from usf2_Utils_SplitString(@excludedActUndercountUns, ',')))
						) a

						select * from #acts

						--Получасовки акта недоучета
						select [ActUndercount_UN], cast(cast(DATEDIFF(minute, @DateStart, [HalfhourDateTime]) as float) / 30 as int) as HalfhourIndex, [AddedValue] from [dbo].[ArchCalc_Replace_ActUndercount_Halfhours]
						where [ActUndercount_UN] in (select distinct [ActUndercount_UN] from #acts) 
						and HalfhourDateTime between @DateStart and @finishDate
						order by [ActUndercount_UN], [HalfhourDateTime]

						drop table #acts

					end;
					
					exec usp2_Arch_30_Select  @TI_ID, @DateStart,@DateEnd,@ChannelType,@TIType,@DataSourceType, @ClosedPeriod_ID,@isCoeffEnabled, @isValidateOtherDataSource, @IsReadCalculatedValues, 
					NULL, @isReturnPreviousDispatchDateTime, @IsChannelsInverted, @IsCA
				
		--end

		--Данные по КА для точки
		if (@IsCAEnabled=1) and (@IsCA=0) begin 
			select * from #caT
			
			--Берем значения контр агента
			--exec usp2_ArchComm_Contr_Select2 @ID, @DateStart, @DateEnd, @ChannelType, @isCoeffEnabled, 0, 0-- TODO надо переворачивать каналы из таблицы ArchCalc_Channel_InversionStatus

			exec usp2_Arch_30_Select  @ID, @DateStart,@DateEnd,@ChannelType,@TIType,@DataSourceType, null, @isCoeffEnabled, 0, @IsReadCalculatedValues, 
					NULL, @isReturnPreviousDispatchDateTime, 0, 1

			drop table #caT
		end 

		--Данные по обходному выключателю для точки
		if (@IsOVEnabled=1) begin 
				select ov.OV_ID, ov.TI_ID, ov.StartDateTime, ov.FinishDateTime, @IsCA as IsCA, ov.TPCoef, 
				ov.IsCoeffTransformationDisabled, ov.TIType, h.MeterSerialNumber, ov.TIName from @ovT ov
				cross apply 
				(
					select top 1 h.MeterSerialNumber from Info_Meters_TO_TI imtti 
					left join Hard_Meters h on h.Meter_ID = imtti.METER_ID
					where imtti.TI_ID = ov.OV_ID and @DateEnd >= StartDateTime and (FinishDateTime is null or @DateStart <= FinishDateTime)
					order by StartDateTime desc
				) h
				--Берем данные по обходным выключателям
				declare 
				@OV_ID int, @StartDateTime datetime,@FinishDateTime datetime,
				@AIATSCode_OV int,@AOATSCode_OV int,@RIATSCode_OV int,@ROATSCode_OV int
				declare c cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select TI_ID, OV_ID, dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime,1), FinishDateTime, AIATSCode, AOATSCode, RIATSCode, ROATSCode, TIType from @ovT
				open c;
					FETCH NEXT FROM c into @ID,@OV_ID, @StartDateTime,@FinishDateTime, @AIATSCode_OV, @AOATSCode_OV, @RIATSCode_OV, @ROATSCode_OV, @titype
					WHILE @@FETCH_STATUS = 0
					BEGIN

						if	(@IsCA = 1) begin
						
							exec usp2_Arch_30_Select ID, @DateStart,@DateEnd,@ChannelType,@TIType,@DataSourceType, null,@isCoeffEnabled, 0, @IsReadCalculatedValues, 
								NULL, @isReturnPreviousDispatchDateTime, 0, 1

								--exec usp2_ArchComm_Contr_Select2 @OV_ID, @StartDateTime,@FinishDateTime,@ChannelType,@isCoeffEnabled,0,0

						end else begin 
							--Выборка коэфф. трансформации
								if (@isCoeffEnabled = 1) begin
									if (@ClosedPeriod_ID is null) begin
										select ti_id, COEFU*COEFI as Coeff, 
										dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime < @DateStart then @DateStart else StartDateTime end, 1) as StartDateTime,
										case when ISNULL(FinishDateTime, '21000101') > @DateEnd then @DateEnd else ISNULL(FinishDateTime, '21000101') end as FinishDateTime, 
										null as ClosedPeriod_ID 
										from dbo.Info_Transformators where ti_id = @OV_ID and 
										dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
										order by ti_id, StartDateTime
									end else begin
										select ti_id, COEFU*COEFI as Coeff, 
										dbo.usf2_Utils_DateTimeRoundToHalfHour(case when StartDateTime < @DateStart then @DateStart else StartDateTime end, 1) as StartDateTime,
										case when ISNULL(FinishDateTime, '21000101') > @DateEnd then @DateEnd else ISNULL(FinishDateTime, '21000101') end as FinishDateTime, 
										@ClosedPeriod_ID as ClosedPeriod_ID 
										from dbo.Info_Transformators_Closed where ti_id = @OV_ID and ClosedPeriod_ID = @ClosedPeriod_ID and
										dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @DateEnd and ISNULL(FinishDateTime, '21000101') >= @DateStart	
										order by ti_id, StartDateTime
									end

									--Выборка периодов, когда коэфф. трансформации был заблокирован
									select distinct ti_id, IsCoeffTransformationDisabled, StartDateTime, ISNULL(FinishDateTime, '21000101') as FinishDateTime, null as ClosedPeriod_ID 
									from dbo.ArchCalc_CoeffTransformation_DisabledStatus where ti_id = @OV_ID
									and dbo.usf2_Utils_DateTimeRoundToHalfHour(StartDateTime, 1) <= @FinishDateTime and ISNULL(FinishDateTime, '21000101') >= @StartDateTime	
									order by ti_id, ClosedPeriod_ID, StartDateTime

								end;

							--Считываем значения по акту недоучета
								if (@IsReadCalculatedValues = 1 AND @UseActUndercount = 1) begin --Для основного профиля не нужны данные по актам недоучета
									set @finishDate = DateAdd(minute, 30, @FinishDateTime);
									--Читаем акт недоучета
									SELECT 
										case when @DateStart >= [StartDateTime] then cast(0 as float) 
											else cast(DATEDIFF(minute, @StartDateTime, [StartDateTime]) as float) / 30 end as StartIndex,
										(case when @finishDate < [FinishDateTime] then cast(DATEDIFF(minute, dbo.usf2_Utils_DateTimeRoundToHalfHour(@StartDateTime,1), dbo.usf2_Utils_DateTimeRoundToHalfHour(@finishDate, 1)) as float) / 30
											else cast(DATEDIFF(minute, @StartDateTime, [FinishDateTime]) as float) / 30  - (case when IsFinishDateTimeInclusive = 0 then 1 else 0 end) end) as FinishIndex,
										case when DATEDIFF(minute, StartDateTime, FinishDateTime) < 1 then AddedValue else
										[AddedValue] / cast(DATEDIFF(minute, StartDateTime, (case when IsFinishDateTimeInclusive = 0 then FinishDateTime else DATEADD(minute, 1, FinishDateTime) end)) as float) * 30 end  as HalfHourValue,--Значение добавляемое к каждой получаовке 
										ActMode, ActUndercount_UN, [IsCoeffTransformationEnabled], [IsLossesCoefficientEnabled]
									into #cacts
									from
									(
										select StartDateTime, AddedValue,
										FinishDateTime,
										ActMode, ActUndercount_UN,
										[IsCoeffTransformationEnabled], [IsLossesCoefficientEnabled], IsFinishDateTimeInclusive
										from [dbo].[ArchCalc_Replace_ActUndercount]
										WHERE [TI_ID] = @OV_ID AND [ChannelType]=@ChannelType AND StartDateTime < @finishDate AND FinishDateTime >= @StartDateTime
										AND (IsInactive is null OR IsInactive = 0)
									) a

									select * from #cacts

									--Получасовки акта недоучета
									select [ActUndercount_UN], cast(cast(DATEDIFF(minute, @StartDateTime, [HalfhourDateTime]) as float) / 30 as int) as HalfhourIndex, [AddedValue] from [dbo].[ArchCalc_Replace_ActUndercount_Halfhours]
									where [ActUndercount_UN] in (select distinct [ActUndercount_UN] from #cacts) 
									and HalfhourDateTime between @StartDateTime and @FinishDateTime
									order by [ActUndercount_UN], [HalfhourDateTime]

									drop table #cacts
								end;

								exec usp2_Arch_30_Select  @OV_ID, @StartDateTime,@FinishDateTime,@ChannelType,@titype,@DataSourceType,@ClosedPeriod_ID, @isCoeffEnabled, @isValidateOtherDataSource, 
									@IsReadCalculatedValues, null, 0, @IsChannelsInverted
							end
						FETCH NEXT FROM c into @ID,@OV_ID, @StartDateTime,@FinishDateTime, @AIATSCode_OV, @AOATSCode_OV, @RIATSCode_OV, @ROATSCode_OV, @titype
						end;
				CLOSE c
				DEALLOCATE c
		end
	end;
end;

go
   grant EXECUTE on usp2_ArchComm_ReadWithCA to [UserCalcService]
go

