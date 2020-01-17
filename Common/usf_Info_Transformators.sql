if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Info_Transformators')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Info_Transformators
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель, 2009
--
-- Описание:
--
-- Возврат коэфф трансформации по получасовкам
-- ======================================================================================
create FUNCTION [dbo].[usf2_Info_Transformators] (
			@TI_ID int,
			@DateStart DateTime,
			@DateEnd DateTime,
			@isCoeffEnabled bit
			
)	
	RETURNS @tbl TABLE 
(
		[dt] datetime,
		[k1] int,[k2] int,[k3] int,[k4] int,[k5] int,[k6] int,[k7] int,[k8] int,[k9] int,[k10] int,
		[k11] int,[k12] int,[k13] int,[k14] int,[k15] int,[k16] int,[k17] int,[k18] int,[k19] int,[k20] int,
		[k21] int,[k22] int,[k23] int,[k24] int,[k25] int,[k26] int,[k27] int,[k28] int,[k29] int,[k30] int,
		[k31] int,[k32] int,[k33] int,[k34] int,[k35] int,[k36] int,[k37] int,[k38] int,[k39] int,[k40] int,
		[k41] int,[k42] int,[k43] int,[k44] int,[k45] int,[k46] int,[k47] int,[k48] int, 
		[Coeff] int
)
AS
BEGIN
declare	 @k int

		--if (@isCoeffEnabled=1)
			set @k = (select top 1 COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and ISNULL(FinishDateTime, '21000101') >= @DateEnd and StartDateTime <= @DateStart)
		--else 
			--set @k=1



		if (@isCoeffEnabled=1 and @k is null) begin
			insert into @tbl 
			select dtf.dt,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k1,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,31,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k2,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,61,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k3,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,91,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k4,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,121,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k5,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,151,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k6,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,181,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k7,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,211,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k8,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,241,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k9,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,271,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k10,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,301,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k11,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,331,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k12,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,351,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k13,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,391,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k14,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,421,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k15,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,451,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k16,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,481,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k17,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,511,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k18,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,541,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k19,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,571,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k20,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,601,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k21,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,631,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k22,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,661,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k23,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,691,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k24,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,721,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k25,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,751,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k26,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,781,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k27,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,811,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k28,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,841,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k29,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,871,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k30,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,901,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k31,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,931,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k32,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,961,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k33,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,991,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k34,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1021,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k35,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1051,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k36,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1081,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k37,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1111,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k38,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1141,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k39,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1171,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k40,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1201,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k41,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1231,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k42,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1261,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k43,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1291,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k44,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1321,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k45,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1351,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k46,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1381,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k47,
				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1411,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as k48,

				ISNULL((select top (1) COEFU*COEFI as Coeff from Info_Transformators where TI_ID = @TI_ID and  DateAdd(n,1,dtf.dt) between StartDateTime and ISNULL(FinishDateTime, '21000101')),1) as Coeff
			from usf2_Utils_HalfHoursByPeriod (@DateStart,@dateEnd) dtf
		end else begin
			if (@isCoeffEnabled=1) begin 
				insert into @tbl
				select dtf.dt,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k,@k ,@k from usf2_Utils_HalfHoursByPeriod (@DateStart,@dateEnd) dtf
			end else begin
				insert into @tbl
				select dtf.dt,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,@k  from usf2_Utils_HalfHoursByPeriod (@DateStart,@dateEnd) dtf

			end

		end
		RETURN
END
   
go
grant select on usf2_Info_Transformators to [UserCalcService]
go