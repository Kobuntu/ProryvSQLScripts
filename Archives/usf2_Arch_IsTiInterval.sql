set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Arch_IsTiInterval')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Arch_IsTiInterval
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2015
--
-- Описание:
--
--		Определяем интервальная ТИ или нет (есть получасовки по точке за последние 3 месяца или нет)
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_Arch_IsTiInterval] (
		@TI_ID   int --Идентификатор ТИ
)

 RETURNS bit 
	AS BEGIN
	declare
	@isTiInterval bit,
	@titype tinyint,
	@dtNow DateTime;

	if (@TI_ID is null) return 0;

	select @titype = titype, @dtNow = DATEADD(month, -3, GETDATE()) from Info_TI where TI_ID = @TI_ID

	if (@titype = 2 or @titype = 1) return 1;

	if (@titype < 10) begin 
		if (exists(select top 1 1 from ArchCalc_30_Virtual where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 11) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_1 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 12) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_2 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 13) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_3 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 14) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_4 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 15) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_5 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 16) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_6 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 17) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_7 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 18) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_8 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 19) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_9 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 20) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_10 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 21) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_11 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 22) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_12 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 23) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_13 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 24) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_14 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 25) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_15 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 26) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_16 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 27) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_17 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 28) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_18 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 29) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_19 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 30) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_20 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 31) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_21 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 32) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_22 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 33) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_23 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 34) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_24 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;	
	end else if (@titype = 35) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_25 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 36) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_26 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 37) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_27 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 38) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_28 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 39) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_29 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;														
	end else if (@titype = 40) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_30 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 41) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_31 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 42) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_32 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 43) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_33 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 44) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_34 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;	
	end else if (@titype = 45) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_35 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 46) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_36 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 47) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_37 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 48) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_38 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 49) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_39 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;				
	end else if (@titype = 50) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_40 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 51) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_41 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 52) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_42 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 53) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_43 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 54) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_44 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;	
	end else if (@titype = 55) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_45 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 56) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_46 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 57) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_47 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 58) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_48 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;
	end else if (@titype = 59) begin
		if (exists(select top 1 1 from ArchCalcBit_30_Virtual_49 where TI_ID = @TI_ID and EventDate > @dtNow)) return 1;
		else return 0;				
	end

	RETURN 0;
END;
go
grant EXECUTE on usf2_Arch_IsTiInterval to [UserCalcService]
go