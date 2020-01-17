if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchComm_Contr_Select2')
          and type in ('P','PC'))
   drop procedure usp2_ArchComm_Contr_Select2
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2008
--
-- Описание:
--
--		Выбирает получасовки для указанных параметров (работаем с интервалом 10 количества ТИ) (новый вариант)
--
-- ======================================================================================
create proc [dbo].[usp2_ArchComm_Contr_Select2]

	@TI_ID int,
	@DateStart datetime,
	@DateEnd datetime,
	@ChannelType tinyint,
	@isCoeffEnabled bit = null,
	@IsValidateOnly bit,
	@IsCAReverse bit


as
begin

	set @dateStart = floor(cast(@dateStart as float))
	set @dateEnd = floor(cast(@dateEnd as float))

	---Если канал описан как NULL или не найдена точка в Info_TI
	if (@ChannelType is null) begin
		select v.dt,cast(-1 as bigint) as ValidStatus,0 as VAL_01, 1 as Coeff,0 as ManualEnterStatus
		from usf2_Utils_HalfHoursByPeriod(@dateStart,@dateEnd) v
	---Обычный запрос
	end else begin

		if (@IsValidateOnly=1) begin  --Если запрашиваем только достоверность
				select v.dt as EventDate,t1.ValidStatus,
					t1.val_01,t1.val_02,t1.val_03,t1.val_04,t1.val_05,t1.val_06,t1.val_07,t1.val_08,t1.val_09,t1.val_10,
					t1.val_11,t1.val_12,t1.val_13,t1.val_14,t1.val_15,t1.val_16,t1.val_17,t1.val_18,t1.val_19,t1.val_20,
					t1.val_21,t1.val_22,t1.val_23,t1.val_24,t1.val_25,t1.val_26,t1.val_27,t1.val_28,t1.val_29,t1.val_30,
					t1.val_31,t1.val_32,t1.val_33,t1.val_34,t1.val_35,t1.val_36,t1.val_37,t1.val_38,t1.val_39,t1.val_40,
					t1.val_41,t1.val_42,t1.val_43,t1.val_44,t1.val_45,t1.val_46,t1.val_47,t1.val_48,ISNULL(ManualEnterStatus,0) as ManualEnterStatus, 0 as DataSource_ID, DispatchDateTime
				from usf2_Utils_HalfHoursByPeriod(@dateStart,@dateEnd) v 
				left join ArchComm_Contr_30_Import_From_XML  t1
				on t1.ContrTI_ID = @TI_ID  and t1.ChannelType=@ChannelType  and v.dt = t1.EventDate
				order by v.dt 

			end else begin
				select v.dt as EventDate,t1.ValidStatus,
					t1.val_01 as val_01,  t1.val_02  as val_02,t1.val_03  as val_03 ,t1.val_04  as val_04,
					t1.val_05 as val_05,  t1.val_06  as val_06,t1.val_07  as val_07 ,t1.val_08  as val_08,
					t1.val_09 as val_09,  t1.val_10  as val_10,t1.val_11  as val_11 ,t1.val_12  as val_12,
					t1.val_13  as val_13, t1.val_14  as val_14,t1.val_15  as val_15 ,t1.val_16  as val_16,
					t1.val_17  as val_17, t1.val_18  as val_18,t1.val_19  as val_19 ,t1.val_20  as val_20,
					t1.val_21  as val_21, t1.val_22  as val_22,t1.val_23  as val_23 ,t1.val_24  as val_24,
					t1.val_25  as val_25, t1.val_26  as val_26,t1.val_27  as val_27 ,t1.val_28  as val_28,
					t1.val_29  as val_29, t1.val_30  as val_30,t1.val_31  as val_31 ,t1.val_32  as val_32,
					t1.val_33  as val_33, t1.val_34  as val_34,t1.val_35  as val_35 ,t1.val_36  as val_36,
					t1.val_37  as val_37, t1.val_38  as val_38,t1.val_39  as val_39 ,t1.val_40  as val_40,
					t1.val_41  as val_41, t1.val_42  as val_42,t1.val_43  as val_43 ,t1.val_44  as val_44,
					t1.val_45  as val_45, t1.val_46  as val_46,t1.val_47  as val_47 ,t1.val_48  as val_48,
					1 as Coeff,ISNULL(ManualEnterStatus,0) as ManualEnterStatus, 0 as DataSource_ID, DispatchDateTime
				from usf2_Utils_HalfHoursByPeriod(@dateStart,@dateEnd) v 
				left join ArchComm_Contr_30_Import_From_XML  t1
				on t1.ContrTI_ID = @TI_ID and t1.ChannelType=@ChannelType  and v.dt = t1.EventDate
				order by v.dt 
		end
	end
end
go
   grant EXECUTE on usp2_ArchComm_Contr_Select2 to [UserCalcService]
go

 