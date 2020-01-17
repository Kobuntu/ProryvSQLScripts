if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_30_Values_Get_LastHalfHour')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_30_Values_Get_LastHalfHour
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2009
--
-- Описание:
--
--		Возвращаем последнюю получасовку для точки ФСК, по основному профилю
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ArchComm_30_Values_Get_LastHalfHour] (
		@TI_ID   int, 
		@ChannelType tinyint, -- Номер канала
		@isCoeffEnabled bit,
		@DiscreteType tinyint, --Период дискретизации
		-- 0  - последняя получасовка,
		-- 1  - последний полный час (с обеими получасовками)
		-- 47 - последние полные сутки (наличие первой и последней получасовки)
		@SummerOrWinter tinyint, --Зимнее (0) или летнее(1) время 
		@OffsetFromMoscow int --Смещение относительно Москвы в минутах
)
      RETURNS @tbl TABLE (
		[dt] [smalldatetime] NOT NULL,
		[ValidStatus] [bit] NULL,
		[Value] [float] NULL,
		[PS_ID] [int] null,
		[Coeff] [int] null,
		[Coef_tp] [float] null,
		[ContrReplaceStatus] [bigint] null,
		[ManualEnterStatus] [bigint] null,
		[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
		[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
		[TIType] [tinyint] NULL

) AS
   BEGIN
    DECLARE 
	@TIType tinyint,
	@ps_id int, 
	@TPCoefOurSide float,
	@DirectChannelType tinyint,
	@StartDate datetime,
	@ValidStatus bigint,
	@Valid bit,
	@Coeff int,
	@Value float,
	@VAL_01 float ,
	@VAL_02 float ,
	@VAL_03 float ,
	@VAL_04 float ,
	@VAL_05 float ,
	@VAL_06 float ,
	@VAL_07 float ,
	@VAL_08 float ,
	@VAL_09 float ,
	@VAL_10 float ,
	@VAL_11 float ,
	@VAL_12 float ,
	@VAL_13 float ,
	@VAL_14 float ,
	@VAL_15 float ,
	@VAL_16 float ,
	@VAL_17 float ,
	@VAL_18 float ,
	@VAL_19 float ,
	@VAL_20 float ,
	@VAL_21 float ,
	@VAL_22 float ,
	@VAL_23 float ,
	@VAL_24 float ,
	@VAL_25 float ,
	@VAL_26 float ,
	@VAL_27 float ,
	@VAL_28 float ,
	@VAL_29 float ,
	@VAL_30 float ,
	@VAL_31 float ,
	@VAL_32 float ,
	@VAL_33 float ,
	@VAL_34 float ,
	@VAL_35 float ,
	@VAL_36 float ,
	@VAL_37 float ,
	@VAL_38 float ,
	@VAL_39 float ,
	@VAL_40 float ,
	@VAL_41 float ,
	@VAL_42 float ,
	@VAL_43 float ,
	@VAL_44 float ,
	@VAL_45 float ,
	@VAL_46 float ,
	@VAL_47 float ,
	@VAL_48 float 
		
		set @DirectChannelType = @ChannelType;
		

		--Определяем тип таблицы для запроса
		--Проверяем достоверность каналов
		select	@TIType = TIType
			, @ChannelType = dbo.usf2_ReverseTariffChannel(0, @ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, GetDate(), GetDate())
			, @ps_id = ps_id
			, @TPCoefOurSide = TPCoefOurSide
		from Info_TI where TI_ID = @TI_ID

		set @StartDate = Floor(Cast(GetDate() as float))

		if (@DiscreteType = 0 OR @DiscreteType = 1) begin
			--Если это обычные архивные значения
			if (@TIType = 0 or @TIType > 2) begin
				set @StartDate = (select Max(EventDate) from ArchComm_30_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			--Если это точки импортируемые из XML
			END ELSE IF (@TIType = 1) BEGIN
				set @StartDate = (select Max(EventDate) from ArchComm_30_Import_From_XML where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			--Если это малые ТИ (распределяются на основе расхода за месяц)
			END ELSE IF (@TIType = 2) BEGIN
				set @StartDate = (select Max(EventDate) from ArchCalc_30_Month_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			END
			--Берем строку из таблицы с последними сутками
			select top 1
			@VAL_01 = arh.VAL_01,
			@VAL_02  = arh.VAL_02,
			@VAL_03  = arh.VAL_03,
			@VAL_04  = arh.VAL_04,
			@VAL_05  = arh.VAL_05,
			@VAL_06  = arh.VAL_06,
			@VAL_07  = arh.VAL_07,
			@VAL_08  = arh.VAL_08,
			@VAL_09  = arh.VAL_09,
			@VAL_10  = arh.VAL_10,
			@VAL_11  = arh.VAL_11,
			@VAL_12  = arh.VAL_12,
			@VAL_13  = arh.VAL_13,
			@VAL_14  = arh.VAL_14,
			@VAL_15  = arh.VAL_15,
			@VAL_16  = arh.VAL_16,
			@VAL_17  = arh.VAL_17,
			@VAL_18  = arh.VAL_18,
			@VAL_19  = arh.VAL_19,
			@VAL_20  = arh.VAL_20,
			@VAL_21  = arh.VAL_21,
			@VAL_22  = arh.VAL_22,
			@VAL_23  = arh.VAL_23,
			@VAL_24  = arh.VAL_24,
			@VAL_25  = arh.VAL_25,
			@VAL_26  = arh.VAL_26,
			@VAL_27  = arh.VAL_27,
			@VAL_28  = arh.VAL_28,
			@VAL_29  = arh.VAL_29,
			@VAL_30  = arh.VAL_30,
			@VAL_31  = arh.VAL_31,
			@VAL_32  = arh.VAL_32,
			@VAL_33  = arh.VAL_33,
			@VAL_34  = arh.VAL_34,
			@VAL_35  = arh.VAL_35,
			@VAL_36  = arh.VAL_36,
			@VAL_37  = arh.VAL_37,
			@VAL_38  = arh.VAL_38,
			@VAL_39  = arh.VAL_39,
			@VAL_40  = arh.VAL_40,
			@VAL_41  = arh.VAL_41,
			@VAL_42  = arh.VAL_42,
			@VAL_43  = arh.VAL_43,
			@VAL_44  = arh.VAL_44,
			@VAL_45  = arh.VAL_45,
			@VAL_46  = arh.VAL_46,
			@VAL_47  = arh.VAL_47,
			@VAL_48  = arh.VAL_48,
			@ValidStatus = ISNULL(arh.ValidStatus,0),
			@Coeff = arh.Coeff
			from usf2_ArchComm_30_Values_Get_Table_By_TIType (@TI_ID,@DirectChannelType,@StartDate,@StartDate,@isCoeffEnabled,0)  arh

			--Ищем последнюю получасовку
			if (@StartDate is null) begin 
				set @StartDate = Floor(Cast(GetDate() as float))
			end else if (not @VAL_48 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_48,@VAL_47,@DiscreteType,@StartDate,@ValidStatus,48)
			end else if (not @VAL_47 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_47
				set @StartDate = DateAdd(minute,1380, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,46)
			end else if (not @VAL_46 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_46,@VAL_45,@DiscreteType,@StartDate,@ValidStatus,46)
			end else if (not @VAL_45 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_45
				set @StartDate = DateAdd(minute,1310, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,44)
			end else if (not @VAL_44 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_44,@VAL_43,@DiscreteType,@StartDate,@ValidStatus,44)
			end  else if (not @VAL_43 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_43
				set @StartDate = DateAdd(minute,1260, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,42)
			end else if (not @VAL_42 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_42,@VAL_41,@DiscreteType,@StartDate,@ValidStatus,42)
			end else if (not @VAL_41 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_41
				set @StartDate = DateAdd(minute,1200, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,40)
			end else if (not @VAL_40 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_40,@VAL_39,@DiscreteType,@StartDate,@ValidStatus,40)
			end else if (not @VAL_39 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_39
				set @StartDate = DateAdd(minute,1140, @StartDate) 
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,38)
			end  else if (not @VAL_38 is null) begin 
			 select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_38,@VAL_37,@DiscreteType,@StartDate,@ValidStatus,38)
			end else if (not @VAL_37 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_37
				set @StartDate = DateAdd(minute,1080, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,36)
			end else if (not @VAL_36 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_36,@VAL_35,@DiscreteType,@StartDate,@ValidStatus,36)
			end else if (not @VAL_35 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_35
				set @StartDate = DateAdd(minute,1020, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,34)
			end else if (not @VAL_34 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_34,@VAL_33,@DiscreteType,@StartDate,@ValidStatus,34)
			end  else if (not @VAL_33 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_33
				set @StartDate = DateAdd(minute,960, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,32)
			end  else if (not @VAL_32 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_32,@VAL_31,@DiscreteType,@StartDate,@ValidStatus,32)
			end  else if (not @VAL_31 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_31
				set @StartDate = DateAdd(minute,900, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,30)
			end  else if (not @VAL_30 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_30,@VAL_29,@DiscreteType,@StartDate,@ValidStatus,30)
			end  else if (not @VAL_29 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_29
				set @StartDate = DateAdd(minute,840, @StartDate) 
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,28)
			end  else if (not @VAL_28 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_28,@VAL_27,@DiscreteType,@StartDate,@ValidStatus,28)
			end else if (not @VAL_27 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_27
				set @StartDate = DateAdd(minute,780, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,26)
			end else if (not @VAL_26 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_26,@VAL_25,@DiscreteType,@StartDate,@ValidStatus,26)
			end else if (not @VAL_25 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_25
				set @StartDate = DateAdd(minute,720, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,24)
			end else if (not @VAL_24 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_24,@VAL_23,@DiscreteType,@StartDate,@ValidStatus,24)
			end  else if (not @VAL_23 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_23
				set @StartDate = DateAdd(minute,660, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,22)
			end  else if (not @VAL_22 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_22,@VAL_21,@DiscreteType,@StartDate,@ValidStatus,22)
			end  else if (not @VAL_21 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_21
				set @StartDate = DateAdd(minute,600, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,20)
			end  else if (not @VAL_20 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_20,@VAL_19,@DiscreteType,@StartDate,@ValidStatus,20)
			end  else if (not @VAL_19 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_19
				set @StartDate = DateAdd(minute,540, @StartDate) 
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,18)
			end  else if (not @VAL_18 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_18,@VAL_17,@DiscreteType,@StartDate,@ValidStatus,18)
			end else if (not @VAL_17 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_17
				set @StartDate = DateAdd(minute,480, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,16)
			end else if (not @VAL_16 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_16,@VAL_15,@DiscreteType,@StartDate,@ValidStatus,16)
			end else if (not @VAL_15 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_15
				set @StartDate = DateAdd(minute,420, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,14)
			end else if (not @VAL_14 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_14,@VAL_13,@DiscreteType,@StartDate,@ValidStatus,14)
			end  else if (not @VAL_13 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_13
				set @StartDate = DateAdd(minute,360, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,12)
			end  else if (not @VAL_12 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_12,@VAL_11,@DiscreteType,@StartDate,@ValidStatus,12)
			end  else if (not @VAL_11 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_11
				set @StartDate = DateAdd(minute,300, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,10)
			end  else if (not @VAL_10 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_10,@VAL_09,@DiscreteType,@StartDate,@ValidStatus,10)
			end  else if (not @VAL_09 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_09
				set @StartDate = DateAdd(minute,240, @StartDate) 
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,8)
			end  else if (not @VAL_08 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_08,@VAL_07,@DiscreteType,@StartDate,@ValidStatus,8)
			end else if (not @VAL_07 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_07
				set @StartDate = DateAdd(minute,180, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,6)
			end else if (not @VAL_06 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_06,@VAL_05,@DiscreteType,@StartDate,@ValidStatus,6)
			end else if (not @VAL_05 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_05
				set @StartDate = DateAdd(minute,120, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,4)
			end else if (not @VAL_04 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_04,@VAL_03,@DiscreteType,@StartDate,@ValidStatus,4)
			end  else if (not @VAL_03 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_03
				set @StartDate = DateAdd(minute,60, @StartDate)
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,2)
			end  else if (not @VAL_02 is null) begin 
				select @Value = [Value], @Valid = [ValidStatus], @StartDate = [dt] from usf2_Utils_GetHalfHoursOrHours(@VAL_02,@VAL_01,@DiscreteType,@StartDate,@ValidStatus,2)
			end  else if (not @VAL_01 is null and @DiscreteType = 0) begin 
				set @Value = @VAL_01
				set @StartDate = @StartDate
				set @Valid = dbo.sfclr_Utils_BitOperations2(@ValidStatus,0)
			end  
		--Если берем данные за сутки
		end else begin
			declare
			@DateStart datetime,
			@DateEnd datetime

			if (@TIType = 0 or @TIType > 2) begin
				set @dateEnd = (select Max(EventDate) from ArchComm_30_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			--Если это точки импортируемые из XML
			END ELSE IF (@TIType = 1) BEGIN
				set @dateEnd = (select Max(EventDate) from ArchComm_30_Import_From_XML where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			--Если это малые ТИ (распределяются на основе расхода за месяц)
			END ELSE IF (@TIType = 2) BEGIN
				set @dateEnd = (select Max(EventDate) from ArchCalc_30_Month_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and not VAL_01 is null)
			END


			if (@dateEnd is null) begin 

				set @StartDate = Floor(Cast(GetDate() as float))

			end else begin

				set @dateStart = DateAdd(day,-2,@dateEnd)

				select @StartDate = ISNULL([Date],Floor(Cast(GetDate() as float))), @Value = [Vals], @Valid = 0 from 
					(
						select top 1 cast([Date] as DateTime) as [Date],Sum([Vals]) as [Vals], Count([Vals])  as [Count] from 
						(
						select @TI_ID as TI_ID,@ChannelType as ChannelType,
						-- корректируем время
						[Date] = floor(cast(dbo.usf2_Utils_CorrectDateTimeByDaylight(dt,@SummerOrWinter,@OffsetFromMoscow, cast(SUBSTRING([DateAndTime],5,2) as int) - 1) as float)),
						[Vals]
						 from 
						(
							select t1.TI_ID,t1.ChannelType,t1.ValidStatus,
									t1.[dt],
									(case when t1.val_01 < 0 then 0 else t1.val_01 end) as val_01,
									(case when t1.val_02 < 0 then 0 else t1.val_02 end) as val_02,
									(case when t1.val_03 < 0 then 0 else t1.val_03 end) as val_03,
									(case when t1.val_04 < 0 then 0 else t1.val_04 end) as val_04,
									(case when t1.val_05 < 0 then 0 else t1.val_05 end) as val_05,
									(case when t1.val_06 < 0 then 0 else t1.val_06 end) as val_06,
									(case when t1.val_07 < 0 then 0 else t1.val_07 end) as val_07,
									(case when t1.val_08 < 0 then 0 else t1.val_08 end) as val_08,
									(case when t1.val_09 < 0 then 0 else t1.val_09 end) as val_09,
									(case when t1.val_10 < 0 then 0 else t1.val_10 end) as val_10,
									(case when t1.val_11 < 0 then 0 else t1.val_11 end) as val_11,
									(case when t1.val_12 < 0 then 0 else t1.val_12 end) as val_12,
									(case when t1.val_13 < 0 then 0 else t1.val_13 end) as val_13,
									(case when t1.val_14 < 0 then 0 else t1.val_14 end) as val_14,
									(case when t1.val_15 < 0 then 0 else t1.val_15 end) as val_15,
									(case when t1.val_16 < 0 then 0 else t1.val_16 end) as val_16,
									(case when t1.val_17 < 0 then 0 else t1.val_17 end) as val_17,
									(case when t1.val_18 < 0 then 0 else t1.val_18 end) as val_18,
									(case when t1.val_19 < 0 then 0 else t1.val_19 end) as val_19,
									(case when t1.val_20 < 0 then 0 else t1.val_20 end) as val_20,
									(case when t1.val_21 < 0 then 0 else t1.val_21 end) as val_21,
									(case when t1.val_22 < 0 then 0 else t1.val_22 end) as val_22,
									(case when t1.val_23 < 0 then 0 else t1.val_23 end) as val_23,
									(case when t1.val_24 < 0 then 0 else t1.val_24 end) as val_24,
									(case when t1.val_25 < 0 then 0 else t1.val_25 end) as val_25,
									(case when t1.val_26 < 0 then 0 else t1.val_26 end) as val_26,
									(case when t1.val_27 < 0 then 0 else t1.val_27 end) as val_27,
									(case when t1.val_28 < 0 then 0 else t1.val_28 end) as val_28,
									(case when t1.val_29 < 0 then 0 else t1.val_29 end) as val_29,
									(case when t1.val_30 < 0 then 0 else t1.val_30 end) as val_30,
									(case when t1.val_31 < 0 then 0 else t1.val_31 end) as val_31,
									(case when t1.val_32 < 0 then 0 else t1.val_32 end) as val_32,
									(case when t1.val_33 < 0 then 0 else t1.val_33 end) as val_33,
									(case when t1.val_34 < 0 then 0 else t1.val_34 end) as val_34,
									(case when t1.val_35 < 0 then 0 else t1.val_35 end) as val_35,
									(case when t1.val_36 < 0 then 0 else t1.val_36 end) as val_36,
									(case when t1.val_37 < 0 then 0 else t1.val_37 end) as val_37,
									(case when t1.val_38 < 0 then 0 else t1.val_38 end) as val_38,
									(case when t1.val_39 < 0 then 0 else t1.val_39 end) as val_39,
									(case when t1.val_40 < 0 then 0 else t1.val_40 end) as val_40,
									(case when t1.val_41 < 0 then 0 else t1.val_41 end) as val_41,
									(case when t1.val_42 < 0 then 0 else t1.val_42 end) as val_42,
									(case when t1.val_43 < 0 then 0 else t1.val_43 end) as val_43,
									(case when t1.val_44 < 0 then 0 else t1.val_44 end) as val_44,
									(case when t1.val_45 < 0 then 0 else t1.val_45 end) as val_45,
									(case when t1.val_46 < 0 then 0 else t1.val_46 end) as val_46,
									(case when t1.val_47 < 0 then 0 else t1.val_47 end) as val_47,
									(case when t1.val_48 < 0 then 0 else t1.val_48 end) as val_48
								from usf2_ArchComm_30_Values_Get_Table_By_TIType(@TI_ID,@DirectChannelType,@dateStart, @dateEnd,@isCoeffEnabled,0) t1
							) arh
						unpivot ( [Vals] for [DateAndTime] in ([Val_01],[Val_02],[Val_03],[Val_04],[Val_05],[Val_06],[Val_07],[Val_08],[Val_09],[Val_10],[Val_11],[Val_12],[Val_13],[Val_14],[Val_15],[Val_16],[Val_17],[Val_18],[Val_19],[Val_20],[Val_21],[Val_22],[Val_23],[Val_24],[Val_25],[Val_26],[Val_27],[Val_28],[Val_29],[Val_30],[Val_31],[Val_32],[Val_33],[Val_34],[Val_35],[Val_36],[Val_37],[Val_38],[Val_39],[Val_40],[Val_41],[Val_42],[Val_43],[Val_44],[Val_45],[Val_46],[Val_47],[Val_48])) unp
						) arhives
						group by [Date]
						having Count([Vals]) = 48
						or Count([Vals]) = 50
						order by [Date] desc
					) a
			end -- if (@dateEnd is null) begin 
		end --Если берем данные за сутки
		--Пишем конечный результат
		insert @tbl ([dt],[TI_ID],[ChannelType],[ValidStatus], [Value], [PS_ID],[Coeff],[Coef_tp],[TIType]) 
		values (@StartDate, @TI_ID, @DirectChannelType, @Valid, @Value, @PS_ID, @Coeff, @TPCoefOurSide, @TIType)


      RETURN
   END


go
grant select on usf2_ArchComm_30_Values_Get_LastHalfHour to [UserCalcService]
go