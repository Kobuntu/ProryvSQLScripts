set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_DataCollect_HavingHalfHoursOnEventDate')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_DataCollect_HavingHalfHoursOnEventDate
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Апрель 2011
--
-- Описание:
--
--		Выбираем признак наличия получасовок для модуля достоверности
--		0 - Отсутствуют данные за указанные сутки
--		48 - Полностью присутствуют данные
--		< 48 - Частично присутствуют данные
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_DataCollect_HavingHalfHoursOnEventDate]
(
	@TI_ID int,
	@TIType tinyint,
	@AbsentChannelsMask tinyint,
	@AIATSCode int,
	@AOATSCode int,
	@RIATSCode int,
	@ROATSCode int,
	@EventDate datetime
)
returns tinyint
AS
BEGIN

declare 
@result tinyint,
@ch tinyint;

set @result = 0;

--Проверяем маску
if @AbsentChannelsMask is null OR (@AbsentChannelsMask & 1) = 0 set @ch = ISNULL(@AIATSCode, 1);
else if ((@AbsentChannelsMask / 2 ) & 1) = 0 set @ch = ISNULL(@AOATSCode, 2);
else if ((@AbsentChannelsMask / 4 ) & 1) = 0 set @ch = ISNULL(@RIATSCode, 3);
else if ((@AbsentChannelsMask / 8 ) & 1) = 0 set @ch = ISNULL(@ROATSCode, 4);
else return @result;

--Смотрим тип точки
if (@TIType = 11) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	
	FROM dbo.ArchBit_30_Values_1 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end else if (@TIType = 12) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	FROM dbo.ArchBit_30_Values_2 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end else if (@TIType = 13) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	FROM dbo.ArchBit_30_Values_3 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end else if (@TIType = 14) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	FROM dbo.ArchBit_30_Values_4 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end else if (@TIType = 15) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	FROM dbo.ArchBit_30_Values_5 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end else if (@TIType = 16) begin
	select @result = 
	case when VAL_01 is null then 0 else 1 end +
	case when VAL_02 is null then 0 else 1 end +
	case when VAL_03 is null then 0 else 1 end +
	case when VAL_04 is null then 0 else 1 end +
	case when VAL_05 is null then 0 else 1 end +
	case when VAL_06 is null then 0 else 1 end +
	case when VAL_07 is null then 0 else 1 end +
	case when VAL_08 is null then 0 else 1 end +
	case when VAL_09 is null then 0 else 1 end +
	case when VAL_10 is null then 0 else 1 end +
	
	case when VAL_11 is null then 0 else 1 end +
	case when VAL_12 is null then 0 else 1 end +
	case when VAL_13 is null then 0 else 1 end +
	case when VAL_14 is null then 0 else 1 end +
	case when VAL_15 is null then 0 else 1 end +
	case when VAL_16 is null then 0 else 1 end +
	case when VAL_17 is null then 0 else 1 end +
	case when VAL_18 is null then 0 else 1 end +
	case when VAL_19 is null then 0 else 1 end +
	case when VAL_20 is null then 0 else 1 end +
	
	case when VAL_21 is null then 0 else 1 end +
	case when VAL_22 is null then 0 else 1 end +
	case when VAL_23 is null then 0 else 1 end +
	case when VAL_24 is null then 0 else 1 end +
	case when VAL_25 is null then 0 else 1 end +
	case when VAL_26 is null then 0 else 1 end +
	case when VAL_27 is null then 0 else 1 end +
	case when VAL_28 is null then 0 else 1 end +
	case when VAL_29 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_31 is null then 0 else 1 end +
	case when VAL_32 is null then 0 else 1 end +
	case when VAL_33 is null then 0 else 1 end +
	case when VAL_34 is null then 0 else 1 end +
	case when VAL_35 is null then 0 else 1 end +
	case when VAL_36 is null then 0 else 1 end +
	case when VAL_37 is null then 0 else 1 end +
	case when VAL_38 is null then 0 else 1 end +
	case when VAL_39 is null then 0 else 1 end +
	case when VAL_30 is null then 0 else 1 end +
	
	case when VAL_41 is null then 0 else 1 end +
	case when VAL_42 is null then 0 else 1 end +
	case when VAL_43 is null then 0 else 1 end +
	case when VAL_44 is null then 0 else 1 end +
	case when VAL_45 is null then 0 else 1 end +
	case when VAL_46 is null then 0 else 1 end +
	case when VAL_47 is null then 0 else 1 end +
	case when VAL_48 is null then 0 else 1 end
	FROM dbo.ArchBit_30_Values_6 
	where ti_id = @TI_ID and ChannelType = @ch and EventDate = @EventDate;
end;

--if (@result = 48) return 1;
--else if (@result = 0) return 0;
--else return 2;

RETURN @result;

END
go
grant EXECUTE on usf2_DataCollect_HavingHalfHoursOnEventDate to [UserCalcService]
go