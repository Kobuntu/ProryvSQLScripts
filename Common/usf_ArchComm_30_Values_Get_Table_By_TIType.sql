if exists (select 1
          from sysobjects
          where  id = object_id('usf2_ArchComm_30_Values_Get_Table_By_TIType')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_ArchComm_30_Values_Get_Table_By_TIType
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2009
--
-- Описание:
--
--		Возвращаем архивные данные в зависимости от типа таблицы
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_ArchComm_30_Values_Get_Table_By_TIType] (
		@TI_ID   int, 
		@ChannelType tinyint, -- Номер канала
		@StartDate DateTime, 
		@FinishDate DateTime,
		@isCoeffEnabled bit,
		@IsValidateOnly bit
)
      RETURNS @tbl TABLE (
		[dt] [datetime] NOT NULL,
		[ValidStatus] [bigint] NULL,
		[VAL_01] [float] NULL,
		[VAL_02] [float] NULL,
		[VAL_03] [float] NULL,
		[VAL_04] [float] NULL,
		[VAL_05] [float] NULL,
		[VAL_06] [float] NULL,
		[VAL_07] [float] NULL,
		[VAL_08] [float] NULL,
		[VAL_09] [float] NULL,
		[VAL_10] [float] NULL,
		[VAL_11] [float] NULL,
		[VAL_12] [float] NULL,
		[VAL_13] [float] NULL,
		[VAL_14] [float] NULL,
		[VAL_15] [float] NULL,
		[VAL_16] [float] NULL,
		[VAL_17] [float] NULL,
		[VAL_18] [float] NULL,
		[VAL_19] [float] NULL,
		[VAL_20] [float] NULL,
		[VAL_21] [float] NULL,
		[VAL_22] [float] NULL,
		[VAL_23] [float] NULL,
		[VAL_24] [float] NULL,
		[VAL_25] [float] NULL,
		[VAL_26] [float] NULL,
		[VAL_27] [float] NULL,
		[VAL_28] [float] NULL,
		[VAL_29] [float] NULL,
		[VAL_30] [float] NULL,
		[VAL_31] [float] NULL,
		[VAL_32] [float] NULL,
		[VAL_33] [float] NULL,
		[VAL_34] [float] NULL,
		[VAL_35] [float] NULL,
		[VAL_36] [float] NULL,
		[VAL_37] [float] NULL,
		[VAL_38] [float] NULL,
		[VAL_39] [float] NULL,
		[VAL_40] [float] NULL,
		[VAL_41] [float] NULL,
		[VAL_42] [float] NULL,
		[VAL_43] [float] NULL,
		[VAL_44] [float] NULL,
		[VAL_45] [float] NULL,
		[VAL_46] [float] NULL,
		[VAL_47] [float] NULL,
		[VAL_48] [float] NULL,
		[PS_ID] [int] null,
		[Coeff] [int] null,
		[Coef_tp] [float] null,
		[ContrReplaceStatus] [bigint] null,
		[ManualEnterStatus] [bigint] null,
		[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
		[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL

) AS
   BEGIN
    DECLARE 
	@TIType tinyint,
	@ps_id int, 
	@TPCoefOurSide float,
	@DirectChannelType tinyint,
	@IsCoeffTransformationDisabled bit
		
		set @DirectChannelType = @ChannelType;

		--Определяем тип таблицы для запроса
		select	@TIType = TIType, @IsCoeffTransformationDisabled = IsCoeffTransformationDisabled
				,@ChannelType = dbo.usf2_ReverseTariffChannel(0, @ChannelType, AIATSCode,AOATSCode,RIATSCode,ROATSCode, @TI_ID, @StartDate, @FinishDate)
				,@ps_id = ps_id,
				@TPCoefOurSide = TPCoefOurSide
		from Info_TI where TI_ID = @TI_ID

		--Если коэффициент трансформации заблокирован для данной точки
		set @IsCoeffTransformationDisabled = ~@IsCoeffTransformationDisabled & @isCoeffEnabled

		--Если это обычные архивные значения
		if (@TIType = 0 or @TIType > 2) insert @tbl ([dt],[TI_ID],[ChannelType],[ValidStatus]
								,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
								,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
								,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30]
								,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
								,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
								,[PS_ID],[Coeff],[Coef_tp])
						select v.dt,@TI_ID, @ChannelType, t1.ValidStatus,
								t1.val_01 * v.k1 as val_01,t1.val_02 * v.k2 as val_02,t1.val_03 * v.k3 as val_03 ,t1.val_04 * v.k4 as val_04,
								t1.val_05 * v.k5 as val_05,t1.val_06 * v.k6 as val_06,t1.val_07 * v.k7 as val_07 ,t1.val_08 * v.k8 as val_08,
								t1.val_09 * v.k9 as val_09,t1.val_10 * v.k10 as val_10,t1.val_11 * v.k11 as val_11 ,t1.val_12 * v.k12 as val_12,
								t1.val_13 * v.k13 as val_13,t1.val_14 * v.k14 as val_14,t1.val_15 * v.k15 as val_15 ,t1.val_16 * v.k16 as val_16,
								t1.val_17 * v.k17 as val_17,t1.val_18 * v.k18 as val_18,t1.val_19 * v.k19 as val_19 ,t1.val_20 * v.k20 as val_20,
								t1.val_21 * v.k21 as val_21,t1.val_22 * v.k22 as val_22,t1.val_23 * v.k23 as val_23 ,t1.val_24 * v.k24 as val_24,
								t1.val_25 * v.k25 as val_25,t1.val_26 * v.k26 as val_26,t1.val_27 * v.k27 as val_27 ,t1.val_28 * v.k28 as val_28,
								t1.val_29 * v.k29 as val_29,t1.val_30 * v.k30 as val_30,t1.val_31 * v.k31 as val_31 ,t1.val_32 * v.k32 as val_32,
								t1.val_33 * v.k33 as val_33,t1.val_34 * v.k34 as val_34,t1.val_35 * v.k35 as val_35 ,t1.val_36 * v.k36 as val_36,
								t1.val_37 * v.k37 as val_37,t1.val_38 * v.k38 as val_38,t1.val_39 * v.k39 as val_39 ,t1.val_40 * v.k40 as val_40,
								t1.val_41 * v.k41 as val_41,t1.val_42 * v.k42 as val_42,t1.val_43 * v.k43 as val_43 ,t1.val_44 * v.k44 as val_44,
								t1.val_45 * v.k45 as val_45,t1.val_46 * v.k46 as val_46,t1.val_47 * v.k47 as val_47 ,t1.val_48 * v.k48 as val_48,
								@PS_ID, v.Coeff as Coeff, @TPCoefOurSide as Coef_tp
						from  dbo.usf2_Info_Transformators(@TI_ID, @StartDate,@FinishDate,@IsCoeffTransformationDisabled) v
						left join 
						(select * from ArchComm_30_Values where TI_ID = @TI_ID and ChannelType=@ChannelType and cast(EventDate as DateTime) between floor(cast(@StartDate as float)) and floor(cast(@FinishDate as float))) t1
						on cast(t1.EventDate as DateTime) = v.dt
						order by v.dt 
		--Если это точки импортируемые из XML
		ELSE IF (@TIType = 1) insert @tbl ([dt],[TI_ID],[ChannelType],[ValidStatus]
								,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
								,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
								,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30]
								,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
								,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
								,[PS_ID],[Coeff],[Coef_tp])
						select v.dt,@TI_ID, @ChannelType, t1.ValidStatus,
								t1.val_01  as val_01,t1.val_02  as val_02,t1.val_03  as val_03 ,t1.val_04  as val_04,
								t1.val_05  as val_05,t1.val_06  as val_06,t1.val_07  as val_07 ,t1.val_08  as val_08,
								t1.val_09  as val_09,t1.val_10  as val_10,t1.val_11  as val_11 ,t1.val_12  as val_12,
								t1.val_13  as val_13,t1.val_14  as val_14,t1.val_15  as val_15 ,t1.val_16  as val_16,
								t1.val_17  as val_17,t1.val_18  as val_18,t1.val_19  as val_19 ,t1.val_20  as val_20,
								t1.val_21  as val_21,t1.val_22  as val_22,t1.val_23  as val_23 ,t1.val_24  as val_24,
								t1.val_25  as val_25,t1.val_26  as val_26,t1.val_27  as val_27 ,t1.val_28  as val_28,
								t1.val_29  as val_29,t1.val_30  as val_30,t1.val_31  as val_31 ,t1.val_32  as val_32,
								t1.val_33  as val_33,t1.val_34  as val_34,t1.val_35  as val_35 ,t1.val_36  as val_36,
								t1.val_37  as val_37,t1.val_38  as val_38,t1.val_39  as val_39 ,t1.val_40  as val_40,
								t1.val_41  as val_41,t1.val_42  as val_42,t1.val_43  as val_43 ,t1.val_44  as val_44,
								t1.val_45  as val_45,t1.val_46  as val_46,t1.val_47  as val_47 ,t1.val_48  as val_48,
								@PS_ID, v.Coeff as Coeff, @TPCoefOurSide as Coef_tp
						from  dbo.usf2_Info_Transformators(@TI_ID, @StartDate,@FinishDate,@isCoeffEnabled) v
						left join 
						(select * from ArchComm_30_Import_From_XML where TI_ID = @TI_ID and ChannelType=@ChannelType and cast(EventDate as DateTime) between floor(cast(@StartDate as float)) and floor(cast(@FinishDate as float))) t1
						on cast(t1.EventDate as DateTime)=v.dt 
						order by v.dt 
		--Если это малые ТИ (распределяются на основе расхода за месяц)
		ELSE IF (@TIType = 2) insert @tbl ([dt],[TI_ID],[ChannelType],[ValidStatus]
								,[VAL_01] ,[VAL_02] ,[VAL_03] ,[VAL_04] ,[VAL_05] ,[VAL_06] ,[VAL_07] ,[VAL_08] ,[VAL_09] ,[VAL_10]
								,[VAL_11] ,[VAL_12] ,[VAL_13] ,[VAL_14] ,[VAL_15] ,[VAL_16] ,[VAL_17] ,[VAL_18] ,[VAL_19] ,[VAL_20]
								,[VAL_21] ,[VAL_22] ,[VAL_23] ,[VAL_24] ,[VAL_25] ,[VAL_26] ,[VAL_27] ,[VAL_28] ,[VAL_29] ,[VAL_30]
								,[VAL_31] ,[VAL_32] ,[VAL_33] ,[VAL_34] ,[VAL_35] ,[VAL_36] ,[VAL_37] ,[VAL_38] ,[VAL_39] ,[VAL_40]
								,[VAL_41] ,[VAL_42] ,[VAL_43] ,[VAL_44] ,[VAL_45] ,[VAL_46] ,[VAL_47] ,[VAL_48]
								,[PS_ID],[Coeff],[Coef_tp])
						select v.dt,@TI_ID, @DirectChannelType, t1.ValidStatus,
								t1.val_01  as val_01,t1.val_02  as val_02,t1.val_03  as val_03 ,t1.val_04  as val_04,
								t1.val_05  as val_05,t1.val_06  as val_06,t1.val_07  as val_07 ,t1.val_08  as val_08,
								t1.val_09  as val_09,t1.val_10  as val_10,t1.val_11  as val_11 ,t1.val_12  as val_12,
								t1.val_13  as val_13,t1.val_14  as val_14,t1.val_15  as val_15 ,t1.val_16  as val_16,
								t1.val_17  as val_17,t1.val_18  as val_18,t1.val_19  as val_19 ,t1.val_20  as val_20,
								t1.val_21  as val_21,t1.val_22  as val_22,t1.val_23  as val_23 ,t1.val_24  as val_24,
								t1.val_25  as val_25,t1.val_26  as val_26,t1.val_27  as val_27 ,t1.val_28  as val_28,
								t1.val_29  as val_29,t1.val_30  as val_30,t1.val_31  as val_31 ,t1.val_32  as val_32,
								t1.val_33  as val_33,t1.val_34  as val_34,t1.val_35  as val_35 ,t1.val_36  as val_36,
								t1.val_37  as val_37,t1.val_38  as val_38,t1.val_39  as val_39 ,t1.val_40  as val_40,
								t1.val_41  as val_41,t1.val_42  as val_42,t1.val_43  as val_43 ,t1.val_44  as val_44,
								t1.val_45  as val_45,t1.val_46  as val_46,t1.val_47  as val_47 ,t1.val_48  as val_48,
								@PS_ID, v.Coeff as Coeff, @TPCoefOurSide as Coef_tp
						from  dbo.usf2_Info_Transformators(@TI_ID, @StartDate,@FinishDate,@isCoeffEnabled) v
						left join 
						(select * from ArchCalc_30_Month_Values where TI_ID = @TI_ID and cast(EventDate as DateTime) between floor(cast(@StartDate as float)) and floor(cast(@FinishDate as float)) and ChannelType=@DirectChannelType and PlanFact=1 ) t1
						on cast(t1.EventDate as DateTime) = v.dt
						order by v.dt
      RETURN
   END

go
grant select on usf2_ArchComm_30_Values_Get_Table_By_TIType to [UserCalcService]
go