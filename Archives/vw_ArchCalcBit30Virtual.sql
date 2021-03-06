if exists (select 1
          from sysobjects
          where  id = object_id('vw_ArchCalcBit30Virtual')
          and type in ('V'))
   drop view vw_ArchCalcBit30Virtual
go
-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Май, 2019
--
-- Описание:
--
--		Получасовые значения ТИ
--
-- ======================================================================================
CREATE view [dbo].[vw_ArchCalcBit30Virtual] 
WITH SCHEMABINDING
AS
	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	0 as TiType
	from dbo.ArchCalc_30_Virtual a with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, 0 as DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, cast(0 as bigint) as ContrReplaceStatus, cast(0 as bigint) as ManualEnterStatus, cast(0 as bigint) as ManualValidStatus,
	null as cal_01,null as cal_02,null as cal_03,null as cal_04,null as cal_05,null as cal_06,null as cal_07,null as cal_08,null as cal_09,null as cal_10,
	null as cal_11,null as cal_12,null as cal_13,null as cal_14,null as cal_15,null as cal_16,null as cal_17,null as cal_18,null as cal_19,null as cal_20,
	null as cal_21,null as cal_22,null as cal_23,null as cal_24,null as cal_25,null as cal_26,null as cal_27,null as cal_28,null as cal_29,null as cal_30,
	null as cal_31,null as cal_32,null as cal_33,null as cal_34,null as cal_35,null as cal_36,null as cal_37,null as cal_38,null as cal_39,null as cal_40,
	null as cal_41,null as cal_42,null as cal_43,null as cal_44,null as cal_45,null as cal_46,null as cal_47,null as cal_48,
	2 as TiType
	from dbo.ArchCalc_30_Month_Values a with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	11 as TiType
	from dbo.ArchCalcBit_30_Virtual_1 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	12 as TiType
	from dbo.ArchCalcBit_30_Virtual_2 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	13 as TiType
	from dbo.ArchCalcBit_30_Virtual_3 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	14 as TiType
	from dbo.ArchCalcBit_30_Virtual_4 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	15 as TiType
	from dbo.ArchCalcBit_30_Virtual_5 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	16 as TiType
	from dbo.ArchCalcBit_30_Virtual_6 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	17 as TiType
	from dbo.ArchCalcBit_30_Virtual_7 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	18 as TiType
	from dbo.ArchCalcBit_30_Virtual_8 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	19 as TiType
	from dbo.ArchCalcBit_30_Virtual_9 arch with (nolock) 

	union all

	select TI_ID, ChannelType, EventDate, DataSource_ID, 
	val_01,val_02,val_03,val_04,val_05,val_06,val_07,val_08,val_09,val_10,
	val_11,val_12,val_13,val_14,val_15,val_16,val_17,val_18,val_19,val_20,
	val_21,val_22,val_23,val_24,val_25,val_26,val_27,val_28,val_29,val_30,
	val_31,val_32,val_33,val_34,val_35,val_36,val_37,val_38,val_39,val_40,
	val_41,val_42,val_43,val_44,val_45,val_46,val_47,val_48, 
	ValidStatus, DispatchDateTime, Status, ContrReplaceStatus, ManualEnterStatus, ManualValidStatus,
	cal_01,cal_02,cal_03,cal_04,cal_05,cal_06,cal_07,cal_08,cal_09,cal_10,
	cal_11,cal_12,cal_13,cal_14,cal_15,cal_16,cal_17,cal_18,cal_19,cal_20,
	cal_21,cal_22,cal_23,cal_24,cal_25,cal_26,cal_27,cal_28,cal_29,cal_30,
	cal_31,cal_32,cal_33,cal_34,cal_35,cal_36,cal_37,cal_38,cal_39,cal_40,
	cal_41,cal_42,cal_43,cal_44,cal_45,cal_46,cal_47,cal_48,
	20 as TiType
	from dbo.ArchCalcBit_30_Virtual_10 arch with (nolock) 
	
GO

   grant select on vw_ArchCalcBit30Virtual to [UserCalcService]
go