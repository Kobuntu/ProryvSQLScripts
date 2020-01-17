if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_GetTPParams')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_GetTPParams
go

/****** Object:  UserDefinedFunction [dbo].[usf2_Info_GetFormulasNeededForMainFormula]    Script Date: 09/25/2008 12:50:13 ******/
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
--		Декабрь, 2009
--
-- Описание:
--
-- Возвращаем параметры ТП, выбираем параметры первой попавшейся точки в ТП
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_GetTPParams] (
			@TP_ID int,
			@ChannelType int = null,
			@datestart datetime = null,
			@dateend datetime = null,
			@IsOurSidebyBusRelation bit = 1, --Относительно какой стороны берем информацию (false(0) - относительно контрольной(нерасчетной))
			@MoneyOurSideMode tinyint = 0,
			@ClosedPeriod_ID uniqueidentifier = null --Закрытый период (если null читаем из открытого периода)
)	
	RETURNS @tbl TABLE 
(
		[IsMoneyOurSide] bit,
		[IsCA] bit,
		[VoltageLevel] tinyint,
		[EvalModeOurSide] tinyint,
		[EvalModeContr] tinyint,
		[TP_ID] Int,
		[TI_ID] int,
		[PS_ID] int,
		[Meter_ID] int,
		[PSProperty] int,

		[Voltage] float, 
		
		[MeterSerialNumber] varchar(255),
		[StringName] nvarchar(256),
		[PSName] nvarchar(255),
		[DirectConsumer_ID] int,
		[PSVoltage] float,
		[TPMode] tinyint,
		IsCoeffTransformationDisabled bit
)
AS
BEGIN
	declare
	@IsMoneyOurSide bit,
	@IsCA bit,
	@IsMoneyOurSidebyBusRelation bit,
	@EvalModeOurSide tinyint,
	@EvalModeContr tinyint,
	@Voltage float,
	@Meter_ID int,
	@MeterSerialNumber varchar(255),
	@TI_ID int, -- идентификатор первой найденой с ТП точкой от которой вытаскиваем параметры
	@TPName nvarchar(256),
	@PS_ID int,
	@PSName nvarchar(255),
	@PSProperty int,
	@VoltageLevel tinyint,
	@DirectConsumer_ID int,
	@PSVoltage float,
	@TPMode tinyint,
	@IsCoeffTransformationDisabled bit

	if (@datestart is null) set @datestart = DateAdd(day,-DatePart(day,DateAdd(month,-1,floor(cast(GetDate() as float))))+1,DateAdd(month,-1,floor(cast(GetDate() as float))));
	if (@dateend is null) set @dateend = DateAdd(minute, -30, DateAdd(month,1,DateAdd(day,-DatePart(day,DateAdd(month,-1,floor(cast(GetDate() as float))))+1,DateAdd(month,-1,floor(cast(GetDate() as float))))));
			

	select top 1 
			@IsMoneyOurSide = 
			case 
				when @MoneyOurSideMode = 0 then IsMoneyOurSide
				else IsMoneyOurSideMode2
			end,
			@EvalModeOurSide = EvalModeOurSide,@EvalModeContr = EvalModeContr,
			@TPName = StringName, @DirectConsumer_ID = DirectConsumer_ID, @Voltage = Voltage,
			@TPMode = TPMode
	from dbo.Info_TP2 tp
	where tp.TP_ID = @TP_ID;

	if (@ClosedPeriod_ID is null) set @VoltageLevel = (select top 1 VoltageLevel from Info_TP_VoltageLevel 
									where TP_ID = @TP_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') >= @datestart)
	else set @VoltageLevel = (select top 1 VoltageLevel from Info_TP_VoltageLevel_Closed
									where TP_ID = @TP_ID and ClosedPeriod_ID = @ClosedPeriod_ID and StartDateTime <= @dateend and ISNULL(FinishDateTime, '21000101') >= @datestart)

	--Проверяем по расчетной стороне берем или по расчетной
	if (@IsOurSidebyBusRelation = 0) begin
		set @IsMoneyOurSidebyBusRelation = ~@IsMoneyOurSide 
	end else begin
		set @IsMoneyOurSidebyBusRelation = @IsMoneyOurSide 
	end 
	

	if (@IsMoneyOurSidebyBusRelation=1) begin --Расчет по нашей стороне 
		
		if (@EvalModeOurSide = 0) begin --Расчет по коэфф

			select top 1 @TI_ID	= TI_ID, @PS_ID = ti.PS_ID
			,@PSName = ps.StringName, @PSProperty = PSProperty, @PSVoltage = ps.PSVoltage, @IsCA = 0, @IsCoeffTransformationDisabled = ti.IsCoeffTransformationDisabled
			from Info_TI ti
			left join Dict_PS ps
			on ti.PS_ID = ps.PS_ID
			where TP_ID = @TP_ID

		end else begin --Расчет по формуле
				
				if (@ClosedPeriod_ID is null) begin
					--Это чтение открытого периода
					select top 1 @TI_ID = TI_ID, @PS_ID = PS_ID, @PSName = StringName, @PSProperty = PSProperty, @PSVoltage = PSVoltage, @IsCA = 0
					,@IsCoeffTransformationDisabled = IsCoeffTransformationDisabled
					from
					(
						select tp.TI_ID, Info_TI.PS_ID, ps.StringName, PSProperty, ps.PSVoltage, 0 as IsCA, ForAutoUse, StringNumber, InnerLevel, Info_TI.IsCoeffTransformationDisabled
						from
						(
						--Прямые точки
						select  0 as InnerLevel,TI_ID, fl.ForAutoUse,StringNumber from dbo.Info_TP2_OurSide_Formula_List fl
						inner join 	dbo.Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
						where fl.TP_ID =@TP_ID and not fd.TI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1 
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						union
						--Вложенные через формулы
						select  InnerLevel,TI_ID, fl.ForAutoUse,StringNumber from dbo.Info_TP2_OurSide_Formula_List fl
						cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 0, null) ff
						where fl.TP_ID =@TP_ID and not ff.TI_ID is null 
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						) tp
						inner join Info_TI on Info_TI.TI_ID=tp.TI_ID 
						left join Dict_PS ps
						on Info_TI.PS_ID = ps.PS_ID
						union
						select tp.ContrTI_ID, Info_Contr_TI.Contr_PS_ID , ps.StringName, PSProperty, ps.PSVoltage, 1 as IsCA, ForAutoUse, StringNumber, InnerLevel, 0
						from
						(
						--Прямые точки
						select  0 as InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_OurSide_Formula_List fl
						inner join 	dbo.Info_TP2_OurSide_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
						where fl.TP_ID =@TP_ID and not fd.ContrTI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						union
						--Вложенные через формулы
						select  InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_OurSide_Formula_List fl
						cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 0, null) ff
						where fl.TP_ID =@TP_ID and not ff.ContrTI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						) tp
						inner join Info_Contr_TI 
						on Info_Contr_TI.ContrTI_ID=tp.ContrTI_ID 
						left join Dict_Contr_PS ps
						on Info_Contr_TI.Contr_PS_ID = ps.Contr_PS_ID
					) o
					order by ForAutoUse desc,InnerLevel,StringNumber
				end else begin
					--Копия чтения открытого периода только читаем из зарытого периода
					select top 1 @TI_ID = TI_ID, @PS_ID = PS_ID, @PSName = StringName, @PSProperty = PSProperty, @PSVoltage = PSVoltage, @IsCA = 0
					,@IsCoeffTransformationDisabled = IsCoeffTransformationDisabled
					from
					(
						select tp.TI_ID, Info_TI.PS_ID, ps.StringName, PSProperty, ps.PSVoltage, 0 as IsCA, ForAutoUse, StringNumber, InnerLevel, Info_TI.IsCoeffTransformationDisabled
						from
						(
						--Прямые точки
						select  0 as InnerLevel,TI_ID, fl.ForAutoUse,StringNumber from dbo.Info_TP2_OurSide_Formula_List_Closed fl
						inner join 	dbo.Info_TP2_OurSide_Formula_Description_Closed fd on fd.Formula_UN = fl.Formula_UN and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
						where fl.TP_ID =@TP_ID and not fd.TI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1 and fl.ClosedPeriod_ID = @ClosedPeriod_ID
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						union
						--Вложенные через формулы
						select  InnerLevel,TI_ID, fl.ForAutoUse,StringNumber from dbo.Info_TP2_OurSide_Formula_List_Closed fl
						cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 0, @ClosedPeriod_ID) ff
						where fl.TP_ID =@TP_ID and not ff.TI_ID is null and fl.ClosedPeriod_ID = @ClosedPeriod_ID
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						) tp
						inner join Info_TI on Info_TI.TI_ID=tp.TI_ID 
						left join Dict_PS ps
						on Info_TI.PS_ID = ps.PS_ID
						union
						select tp.ContrTI_ID, Info_Contr_TI.Contr_PS_ID , ps.StringName, PSProperty, ps.PSVoltage, 1 as IsCA, ForAutoUse, StringNumber, InnerLevel,0
						from
						(
						--Прямые точки
						select  0 as InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_OurSide_Formula_List_Closed fl
						inner join 	dbo.Info_TP2_OurSide_Formula_Description_Closed fd on fd.Formula_UN = fl.Formula_UN and fd.ClosedPeriod_ID = fl.ClosedPeriod_ID
						where fl.TP_ID =@TP_ID and not fd.ContrTI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1 and fl.ClosedPeriod_ID = @ClosedPeriod_ID
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						union
						--Вложенные через формулы
						select  InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_OurSide_Formula_List_Closed fl
						cross apply usf2_Info_GetFormulasNeededForMainFormulaOurSide(fl.Formula_UN, 0, @ClosedPeriod_ID) ff
						where fl.TP_ID =@TP_ID and not ff.ContrTI_ID is null
						and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
							or (@ChannelType is null))
						and fl.ForAutoUse = 1 and fl.ClosedPeriod_ID = @ClosedPeriod_ID
						and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') > @datestart -- ограничение на период действия ф-лы
						) tp
						inner join Info_Contr_TI 
						on Info_Contr_TI.ContrTI_ID=tp.ContrTI_ID 
						left join Dict_Contr_PS ps
						on Info_Contr_TI.Contr_PS_ID = ps.Contr_PS_ID
					) o
					order by ForAutoUse desc,InnerLevel,StringNumber
				end
		end

		--Номер и идентификатор счетчика
		select @METER_ID = mt.METER_ID, @MeterSerialNumber = MeterSerialNumber 
		from 
		(
			select top (1) * from dbo.Info_Meters_TO_TI mt
			where TI_ID = @TI_ID and StartDateTime <= @dateend
						and FinishDateTime > @datestart 
			order by StartDateTime desc
		) mt 
		left join  dbo.Hard_Meters hm
		on hm.Meter_ID = mt.Meter_ID

	
	end else begin --Расчет по стороне КА
		if (@EvalModeContr = 0) begin --Расчет по коэфф

			select top 1 @TI_ID	= ContrTI_ID, @PS_ID = ti.Contr_PS_ID, @PSName = ps.StringName, @PSProperty = PSProperty, @PSVoltage = ps.PSVoltage, @IsCA = 1
			from Info_Contr_TI  ti
			left join Dict_Contr_PS ps
			on ti.Contr_PS_ID = ps.Contr_PS_ID
			where TP_ID2 = @TP_ID

		end else begin --Расчет по формуле

				select top 1 @TI_ID = TI_ID, @PS_ID = PS_ID, @PSName = StringName, @PSProperty = PSProperty, @PSVoltage = PSVoltage, @IsCA = 1
				from
				(
					select tp.TI_ID, Info_TI.PS_ID, ps.StringName, PSProperty, ps.PSVoltage, 0 as IsCA, ForAutoUse, StringNumber, InnerLevel
					from
					(
					--Прямые точки
					select  0 as InnerLevel,TI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_Contr_Formula_List fl
					inner join 	dbo.Info_TP2_Contr_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
					where fl.TP_ID =@TP_ID and not fd.TI_ID is null
					and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
						or (@ChannelType is null))
					and fl.ForAutoUse = 1
					and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') >= @datestart -- ограничение на период действия ф-лы
					union
					--Вложенные через формулы
					select  InnerLevel,TI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_Contr_Formula_List fl
					cross apply usf2_Info_GetFormulasNeededForMainFormulaContr2(fl.Formula_UN, 0) ff
					where fl.TP_ID =@TP_ID and not ff.TI_ID is null 
					and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
						or (@ChannelType is null))
					and fl.ForAutoUse = 1
					and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') >= @datestart -- ограничение на период действия ф-лы
					) tp
					inner join Info_TI on Info_TI.TI_ID=tp.TI_ID 
					left join Dict_PS ps
					on Info_TI.PS_ID = ps.PS_ID
					union
					select tp.ContrTI_ID, Info_Contr_TI.Contr_PS_ID , ps.StringName, PSProperty, ps.PSVoltage, 1 as IsCA, ForAutoUse, StringNumber, InnerLevel
					from
					(
					--Прямые точки
					select  0 as InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_Contr_Formula_List fl
					inner join 	dbo.Info_TP2_Contr_Formula_Description fd on fd.Formula_UN = fl.Formula_UN
					where fl.TP_ID =@TP_ID and not fd.ContrTI_ID is null
					and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
						or (@ChannelType is null))
					and fl.ForAutoUse = 1
					and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') >= @datestart -- ограничение на период действия ф-лы
					union
					--Вложенные через формулы
					select  InnerLevel, ContrTI_ID, fl.ForAutoUse,StringNumber	from dbo.Info_TP2_Contr_Formula_List fl
					cross apply usf2_Info_GetFormulasNeededForMainFormulaContr2(fl.Formula_UN, 0) ff
					where fl.TP_ID =@TP_ID and not ff.ContrTI_ID is null
					and ((not @ChannelType is null and  fl.ChannelType = @ChannelType)	
						or (@ChannelType is null))
					and fl.ForAutoUse = 1
					and fl.StartDateTime<= @dateend and ISNULL(fl.FinishDateTime, '21000101') >= @datestart -- ограничение на период действия ф-лы
					) tp
					inner join Info_Contr_TI 
					on Info_Contr_TI.ContrTI_ID=tp.ContrTI_ID 
					left join Dict_Contr_PS ps
					on Info_Contr_TI.Contr_PS_ID = ps.Contr_PS_ID
				) o
				order by ForAutoUse desc,InnerLevel,StringNumber

		end
	end

	insert into @tbl
	select @IsMoneyOurSide, @IsCA, @VoltageLevel,@EvalModeOurSide,@EvalModeContr, @TP_ID, @TI_ID,@PS_ID,  @Meter_ID,@PSProperty, @Voltage,  @MeterSerialNumber, @TPName, @PSName, @DirectConsumer_ID, @PSVoltage, @TPMode, ISNULL(@IsCoeffTransformationDisabled,0)

	RETURN
END
go
   grant select on usf2_Info_GetTPParams to [UserCalcService]
go