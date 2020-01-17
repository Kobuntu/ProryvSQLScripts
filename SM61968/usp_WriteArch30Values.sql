if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteArch30Values')
          and type in ('P','PC'))
 drop procedure usp2_WriteArch30Values
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Arch30ValuesType' AND ss.name = N'dbo')
DROP TYPE [dbo].[Arch30ValuesType]
-- Пересоздаем заново
CREATE TYPE [dbo].[Arch30ValuesType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
	[TariffZone_ID] [int] NOT NULL,
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
	[ValidStatus] [bigint] NOT NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
	[Status] [int] NULL,
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDate] ASC,
	[ChannelType] ASC,
	[TariffZone_ID] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::Arch30ValuesType to [UserMaster61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Декабрь, 2011
--
-- Описание:
--
--		Пишем таблицу 30 минуток в базу (Устаоела, вместо этой процедуры надо использовать usp2_WriteArch30VirtualValues)
--
-- ======================================================================================

create proc [dbo].[usp2_WriteArch30Values]
	@TIType tinyint, --Тип точки 
	@Arch30ValuesTable Arch30ValuesType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@sqlcommand nvarchar(4000),
@tableName nvarchar(100);

if (@TIType < 10) begin
	set @tableName = 'ArchComm_30_Values'
end else begin
	set @tableName = 'ArchBit_30_Values_' + ltrim(str(@TIType - 10,2));
end;

set @sqlcommand = N'MERGE ' + @tableName + ' AS a
USING (SELECT *, dbo.usf2_Tariff_GetChannelByZoneID([ti_id], [EventDate], [ChannelType], [tariffZone_ID]) as Channel FROM @Arch30ValuesTable 
where dbo.usf2_Tariff_GetChannelByZoneID([ti_id], [EventDate], [ChannelType], [tariffZone_ID])>0) AS n
ON a.TI_ID = n.TI_ID and a.EventDate = n.EventDate and a.ChannelType = n.Channel
WHEN MATCHED THEN UPDATE SET ValidStatus = n.ValidStatus, DispatchDateTime=n.DispatchDateTime, CUS_ID=n.CUS_ID,[Status]=n.[Status]
                ,VAL_01 = ISNULL(n.VAL_01, a.VAL_01),VAL_02 = ISNULL(n.VAL_02, a.VAL_02)
                ,VAL_03 = ISNULL(n.VAL_03, a.VAL_03),VAL_04 = ISNULL(n.VAL_04, a.VAL_01)
                ,VAL_05 = ISNULL(n.VAL_05, a.VAL_05),VAL_06 = ISNULL(n.VAL_06, a.VAL_06)
                ,VAL_07 = ISNULL(n.VAL_07, a.VAL_07),VAL_08 = ISNULL(n.VAL_08, a.VAL_08)
                ,VAL_09 = ISNULL(n.VAL_09, a.VAL_09),VAL_10 = ISNULL(n.VAL_10, a.VAL_10)
                ,VAL_11 = ISNULL(n.VAL_11, a.VAL_11),VAL_12 = ISNULL(n.VAL_12, a.VAL_12)
                ,VAL_13 = ISNULL(n.VAL_13, a.VAL_13),VAL_14 = ISNULL(n.VAL_14, a.VAL_14)
                ,VAL_15 = ISNULL(n.VAL_15, a.VAL_15),VAL_16 = ISNULL(n.VAL_16, a.VAL_16)
                ,VAL_17 = ISNULL(n.VAL_17, a.VAL_17),VAL_18 = ISNULL(n.VAL_18, a.VAL_18)
                ,VAL_19 = ISNULL(n.VAL_19, a.VAL_19),VAL_20 = ISNULL(n.VAL_20, a.VAL_20)
                ,VAL_21 = ISNULL(n.VAL_21, a.VAL_21),VAL_22 = ISNULL(n.VAL_22, a.VAL_22)
                ,VAL_23 = ISNULL(n.VAL_23, a.VAL_23),VAL_24 = ISNULL(n.VAL_24, a.VAL_24)
                ,VAL_25 = ISNULL(n.VAL_25, a.VAL_25),VAL_26 = ISNULL(n.VAL_26, a.VAL_26)
                ,VAL_27 = ISNULL(n.VAL_27, a.VAL_27),VAL_28 = ISNULL(n.VAL_28, a.VAL_28)
                ,VAL_29 = ISNULL(n.VAL_29, a.VAL_29),VAL_30 = ISNULL(n.VAL_30, a.VAL_30)
                ,VAL_31 = ISNULL(n.VAL_31, a.VAL_31),VAL_32 = ISNULL(n.VAL_32, a.VAL_32)
                ,VAL_33 = ISNULL(n.VAL_33, a.VAL_33),VAL_34 = ISNULL(n.VAL_34, a.VAL_34)
                ,VAL_35 = ISNULL(n.VAL_35, a.VAL_35),VAL_36 = ISNULL(n.VAL_36, a.VAL_36)
                ,VAL_37 = ISNULL(n.VAL_37, a.VAL_37),VAL_38 = ISNULL(n.VAL_38, a.VAL_38)
                ,VAL_39 = ISNULL(n.VAL_39, a.VAL_39),VAL_40 = ISNULL(n.VAL_40, a.VAL_40)
                ,VAL_41 = ISNULL(n.VAL_41, a.VAL_41),VAL_42 = ISNULL(n.VAL_42, a.VAL_42)
                ,VAL_43 = ISNULL(n.VAL_43, a.VAL_43),VAL_44 = ISNULL(n.VAL_44, a.VAL_44)
                ,VAL_45 = ISNULL(n.VAL_45, a.VAL_45),VAL_46 = ISNULL(n.VAL_46, a.VAL_46)
                ,VAL_47 = ISNULL(n.VAL_47, a.VAL_47),VAL_48 = ISNULL(n.VAL_48, a.VAL_48)
WHEN NOT MATCHED THEN 
    INSERT (TI_ID,EventDate,ChannelType,VAL_01,VAL_02,VAL_03,VAL_04,VAL_05,VAL_06,VAL_07,VAL_08,VAL_09,VAL_10
			,VAL_11,VAL_12,VAL_13,VAL_14,VAL_15,VAL_16,VAL_17,VAL_18,VAL_19,VAL_20,VAL_21,VAL_22,VAL_23,VAL_24,VAL_25,VAL_26,VAL_27,VAL_28,VAL_29,VAL_30
			,VAL_31,VAL_32,VAL_33,VAL_34,VAL_35,VAL_36,VAL_37,VAL_38,VAL_39,VAL_40,VAL_41,VAL_42,VAL_43,VAL_44,VAL_45,VAL_46,VAL_47,VAL_48
           ,ValidStatus
           ,DispatchDateTime
           ,CUS_ID
           ,[Status])
    VALUES (n.TI_ID,n.EventDate,n.[Channel],n.VAL_01,n.VAL_02,n.VAL_03,n.VAL_04,n.VAL_05,n.VAL_06,n.VAL_07,n.VAL_08,n.VAL_09,n.VAL_10
			,n.VAL_11,n.VAL_12,n.VAL_13,n.VAL_14,n.VAL_15,n.VAL_16,n.VAL_17,n.VAL_18,n.VAL_19,n.VAL_20,n.VAL_21,n.VAL_22,n.VAL_23,n.VAL_24,n.VAL_25,n.VAL_26,n.VAL_27,n.VAL_28,n.VAL_29,n.VAL_30
			,n.VAL_31,n.VAL_32,n.VAL_33,n.VAL_34,n.VAL_35,n.VAL_36,n.VAL_37,n.VAL_38,n.VAL_39,n.VAL_40,n.VAL_41,n.VAL_42,n.VAL_43,n.VAL_44,n.VAL_45,n.VAL_46,n.VAL_47,n.VAL_48
           ,n.ValidStatus
           ,n.DispatchDateTime
           ,n.CUS_ID
           ,n.[Status]);'

EXEC sp_executesql @sqlcommand, 
N'@Arch30ValuesTable Arch30ValuesType READONLY', @Arch30ValuesTable

end
go
   grant EXECUTE on usp2_WriteArch30Values to [UserMaster61968Service]
go