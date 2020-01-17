if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteArch30VirtualValues')
          and type in ('P','PC'))
 drop procedure usp2_WriteArch30VirtualValues
go

if exists (select 1
          from sysobjects
          where  id = object_id('usp_DataCollect_WriteArch30Values')
          and type in ('P','PC'))
 drop procedure usp_DataCollect_WriteArch30Values
go

--if exists (select 1
--          from sysobjects
--          where  id = object_id('usp2_Expl_Import_Arch30Values')
--          and type in ('P','PC'))
-- drop procedure usp2_Expl_Import_Arch30Values
--GO

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_GetFirstSmallBitNumber')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_GetFirstSmallBitNumber
go

if exists (select 1
          from sysobjects
          where  id = object_id('usf2_Utils_GetLastBitNumber')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_Utils_GetLastBitNumber
go

--Обновляем тип
--Удаляем если есть
IF  NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'Arch30VirtualValuesType' AND ss.name = N'dbo')
--DROP TYPE [dbo].[Arch30VirtualValuesType]
-- Пересоздаем заново
CREATE TYPE [dbo].[Arch30VirtualValuesType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDate] [smalldatetime] NOT NULL,
	[ChannelType] [dbo].[TI_CHANNEL_TYPE] NOT NULL,
	[DataSourceType] tinyint NOT NULL,
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
	[Status] [int] NULL,
	[ExistValueMask] [bigint] NOT NULL,
	[ReasonsOfAbsentData] [tinyint] NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
	[IsDivOnCoeffTransformation] bit NULL, --Делить на коэфф. трансформации
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDate] ASC,
	[ChannelType] ASC,
	[DataSourceType]
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::Arch30VirtualValuesType to [UserCalcService]
go

grant EXECUTE on TYPE::Arch30VirtualValuesType to [UserDataCollectorService]
go


-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Возвращаем номер младшего взведенного бита
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Utils_GetFirstSmallBitNumber]
(	
	@mask bigint
)
RETURNS tinyint
AS
begin
declare @counter tinyint

set @counter = 0;
while @counter < 48
BEGIN
	
	if (dbo.sfclr_Utils_BitOperations2(@mask,@counter)=1) BREAK;
	set @counter = @counter + 1
END

return @counter;
end

go
grant EXECUTE on usf2_Utils_GetFirstSmallBitNumber to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Июль, 2018
--
-- Описание:
--
--		Возвращаем номер последнего взведенного бита
--
-- ======================================================================================

create FUNCTION [dbo].[usf2_Utils_GetLastBitNumber]
(	
	@mask bigint
)
RETURNS tinyint
AS
begin
declare @counter tinyint

set @counter = 48;
while @counter > 0
BEGIN
	
	if (dbo.sfclr_Utils_BitOperations2(@mask,@counter)=1) BREAK;
	set @counter = @counter - 1
END

return @counter;
end

go
grant EXECUTE on usf2_Utils_GetLastBitNumber to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2013
--
-- Описание:
--
--		Пишем таблицу 30 минуток расчетного профиля в базу
--
-- ======================================================================================

create proc [dbo].[usp2_WriteArch30VirtualValues]
	@IsCA bit, --Признак КА
	@EventParam tinyint, --Признак замещения
	@UserName varchar(64), --Имя пользователя
	@CommentString varchar(1000), --Строка комментариев
	@AddedValue float = null,
	@ReplaceSession uniqueidentifier = null,
	@OldDataSourceType tinyint = null, --Если смена источника данных, здесь старый источник из которого переносим данные 
	@Arch30VirtualValuesTable Arch30VirtualValuesType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare
@IsChangeDataSource bit = 0,
@DataSource_ID int,
@f nvarchar(1),
@sqlPerf nvarchar(max),
@sqlPerf1 nvarchar(max),
@sqlPerf2 nvarchar(max),
@sqlSufix nvarchar(max),
@sqlSufix1 nvarchar(max),
@sqlMiddle nvarchar(max),
@sqlexecuted nvarchar(max),
@sqlC1 nvarchar(1000),
@sqlC2 nvarchar(1000),
@sqlUpdate nvarchar(max),
@sqlUpdate1 nvarchar(max),
@sqlTable nvarchar(200)
--@reversedChannel nvarchar(300);

if (@OldDataSourceType is null) set @IsChangeDataSource = 0;
else set @IsChangeDataSource = 1;

--Сначала необходимо определить идентификатор пользователя
declare @ti_ID int, @ChannelType int, @User_ID varchar(22), @StartDate DateTime, @FinishDate DateTime;

set @User_ID = (select top 1 [USER_ID] from Expl_Users where [UserName] = @UserName);

if (@IsChangeDataSource = 1) set @f = 'V' else set @f = 'C'

set @sqlC1 = ',ContrReplaceStatus=case when (@EventParam=2 or @EventParam=3)  then ContrReplaceStatus | [ExistValueMask] else 0 end
				,ManualEnterStatus=case when (@EventParam<>2 and @EventParam<>131) then ManualEnterStatus | [ExistValueMask] else 0 end';

set @sqlPerf = N'ON a.TI_ID = n.TI_ID and a.EventDate = n.EventDate and a.ChannelType = n.Channel and a.DataSource_ID = n.DataSource_ID
				WHEN MATCHED THEN UPDATE SET '+@f+'AL_01=case when dbo.sfclr_Utils_BitOperations2(Mask,0) = 1 then n.VAL_01/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,0,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_01 end,
				'+@f+'AL_02=case when dbo.sfclr_Utils_BitOperations2(Mask,1)=1 then n.VAL_02/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,1,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_02 end,
				'+@f+'AL_03=case when dbo.sfclr_Utils_BitOperations2(Mask,2)=1 then n.VAL_03/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,2,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_03 end,
				'+@f+'AL_04=case when dbo.sfclr_Utils_BitOperations2(Mask,3)=1 then n.VAL_04/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,3,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_04 end,
				'+@f+'AL_05=case when dbo.sfclr_Utils_BitOperations2(Mask,4)=1 then n.VAL_05/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,4,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_05 end,
				'+@f+'AL_06=case when dbo.sfclr_Utils_BitOperations2(Mask,5)=1 then n.VAL_06/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,5,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_06 end,
				'+@f+'AL_07=case when dbo.sfclr_Utils_BitOperations2(Mask,6)=1 then n.VAL_07/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,6,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_07 end,
				'+@f+'AL_08=case when dbo.sfclr_Utils_BitOperations2(Mask,7)=1 then n.VAL_08/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,7,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_08 end,
				'+@f+'AL_09=case when dbo.sfclr_Utils_BitOperations2(Mask,8)=1 then n.VAL_09/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,8,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_09 end,
				'+@f+'AL_10=case when dbo.sfclr_Utils_BitOperations2(Mask,9)=1 then n.VAL_10/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,9,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_10 end,
				'+@f+'AL_11=case when dbo.sfclr_Utils_BitOperations2(Mask,10)=1 then n.VAL_11/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,10,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_11 end,
				'+@f+'AL_12=case when dbo.sfclr_Utils_BitOperations2(Mask,11)=1 then n.VAL_12/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,11,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_12 end,
				'+@f+'AL_13=case when dbo.sfclr_Utils_BitOperations2(Mask,12)=1 then n.VAL_13/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,12,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_13 end,
				'+@f+'AL_14=case when dbo.sfclr_Utils_BitOperations2(Mask,13)=1 then n.VAL_14/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,13,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_14 end,
				'+@f+'AL_15=case when dbo.sfclr_Utils_BitOperations2(Mask,14)=1 then n.VAL_15/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,14,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_15 end,
				'+@f+'AL_16=case when dbo.sfclr_Utils_BitOperations2(Mask,15)=1 then n.VAL_16/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,15,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_16 end,'
set @sqlPerf1=N''+@f+'AL_17=case when dbo.sfclr_Utils_BitOperations2(Mask,16)=1 then n.VAL_17/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,16,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_17 end,
				'+@f+'AL_18=case when dbo.sfclr_Utils_BitOperations2(Mask,17)=1 then n.VAL_18/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,17,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_18 end,
				'+@f+'AL_19=case when dbo.sfclr_Utils_BitOperations2(Mask,18)=1 then n.VAL_19/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,18,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_19 end,
				'+@f+'AL_20=case when dbo.sfclr_Utils_BitOperations2(Mask,19)=1 then n.VAL_20/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,19,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_20 end,
				'+@f+'AL_21=case when dbo.sfclr_Utils_BitOperations2(Mask,20)=1 then n.VAL_21/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,20,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_21 end,
				'+@f+'AL_22=case when dbo.sfclr_Utils_BitOperations2(Mask,21)=1 then n.VAL_22/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,21,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_22 end,
				'+@f+'AL_23=case when dbo.sfclr_Utils_BitOperations2(Mask,22)=1 then n.VAL_23/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,22,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_23 end,
				'+@f+'AL_24=case when dbo.sfclr_Utils_BitOperations2(Mask,23)=1 then n.VAL_24/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,23,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_24 end,
				'+@f+'AL_25=case when dbo.sfclr_Utils_BitOperations2(Mask,24)=1 then n.VAL_25/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,24,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_25 end,
				'+@f+'AL_26=case when dbo.sfclr_Utils_BitOperations2(Mask,25)=1 then n.VAL_26/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,25,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_26 end,
				'+@f+'AL_27=case when dbo.sfclr_Utils_BitOperations2(Mask,26)=1 then n.VAL_27/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,26,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_27 end,
				'+@f+'AL_28=case when dbo.sfclr_Utils_BitOperations2(Mask,27)=1 then n.VAL_28/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,27,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_28 end,
				'+@f+'AL_29=case when dbo.sfclr_Utils_BitOperations2(Mask,28)=1 then n.VAL_29/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,28,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_29 end,
				'+@f+'AL_30=case when dbo.sfclr_Utils_BitOperations2(Mask,29)=1 then n.VAL_30/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,29,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_30 end,
				'+@f+'AL_31=case when dbo.sfclr_Utils_BitOperations2(Mask,30)=1 then n.VAL_31/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,30,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_31 end,
				'+@f+'AL_32=case when dbo.sfclr_Utils_BitOperations2(Mask,31)=1 then n.VAL_32/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,31,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_32 end,
				'+@f+'AL_33=case when dbo.sfclr_Utils_BitOperations2(Mask,32)=1 then n.VAL_33/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,32,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_33 end,
				'+@f+'AL_34=case when dbo.sfclr_Utils_BitOperations2(Mask,33)=1 then n.VAL_34/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,33,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_34 end,
				'+@f+'AL_35=case when dbo.sfclr_Utils_BitOperations2(Mask,34)=1 then n.VAL_35/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,34,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_35 end, 
				'+@f+'AL_36=case when dbo.sfclr_Utils_BitOperations2(Mask,35)=1 then n.VAL_36/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,35,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_36 end,'
set @sqlPerf2=N''+@f+'AL_37=case when dbo.sfclr_Utils_BitOperations2(Mask,36)=1 then n.VAL_37/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,36,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_37 end,
				'+@f+'AL_38=case when dbo.sfclr_Utils_BitOperations2(Mask,37)=1 then n.VAL_38/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,37,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_38 end,
				'+@f+'AL_39=case when dbo.sfclr_Utils_BitOperations2(Mask,38)=1 then n.VAL_39/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,38,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_39 end,
				'+@f+'AL_40=case when dbo.sfclr_Utils_BitOperations2(Mask,39)=1 then n.VAL_40/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,39,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_40 end,
				'+@f+'AL_41=case when dbo.sfclr_Utils_BitOperations2(Mask,40)=1 then n.VAL_41/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,40,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_41 end,
				'+@f+'AL_42=case when dbo.sfclr_Utils_BitOperations2(Mask,41)=1 then n.VAL_42/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,41,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_42 end,
				'+@f+'AL_43=case when dbo.sfclr_Utils_BitOperations2(Mask,42)=1 then n.VAL_43/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,42,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_43 end,
				'+@f+'AL_44=case when dbo.sfclr_Utils_BitOperations2(Mask,43)=1 then n.VAL_44/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,43,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_44 end,
				'+@f+'AL_45=case when dbo.sfclr_Utils_BitOperations2(Mask,44)=1 then n.VAL_45/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,44,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_45 end,
				'+@f+'AL_46=case when dbo.sfclr_Utils_BitOperations2(Mask,45)=1 then n.VAL_46/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,45,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_46 end,
				'+@f+'AL_47=case when dbo.sfclr_Utils_BitOperations2(Mask,46)=1 then n.VAL_47/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,46,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_47 end,
				'+@f+'AL_48=case when dbo.sfclr_Utils_BitOperations2(Mask,47)=1 then n.VAL_48/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,47,n.IsDivOnCoeffTransformation) else a.'+@f+'AL_48 end,
				ManualEnterDateTime=n.DispatchDateTime, CUS_ID=n.CUS_ID'
set @sqlMiddle = ',ValidStatus = (a.ValidStatus & (~[ExistValueMask])) | n.ValidStatus
				WHEN NOT MATCHED THEN 
				INSERT (TI_ID,EventDate,ChannelType,DataSource_ID,'+@f+'AL_01,'+@f+'AL_02,'+@f+'AL_03,'+@f+'AL_04,'+@f+'AL_05,'+@f+'AL_06,'+@f+'AL_07,'+@f+'AL_08,'+@f+'AL_09,'+@f+'AL_10
				,'+@f+'AL_11,'+@f+'AL_12,'+@f+'AL_13,'+@f+'AL_14,'+@f+'AL_15,'+@f+'AL_16,'+@f+'AL_17,'+@f+'AL_18,'+@f+'AL_19,'+@f+'AL_20,'+@f+'AL_21,'+@f+'AL_22,'+@f+'AL_23,'+@f+'AL_24,'+@f+'AL_25,'+@f+'AL_26,'+@f+'AL_27,'+@f+'AL_28,'+@f+'AL_29,'+@f+'AL_30
				,'+@f+'AL_31,'+@f+'AL_32,'+@f+'AL_33,'+@f+'AL_34,'+@f+'AL_35,'+@f+'AL_36,'+@f+'AL_37,'+@f+'AL_38,'+@f+'AL_39,'+@f+'AL_40,'+@f+'AL_41,'+@f+'AL_42,'+@f+'AL_43,'+@f+'AL_44,'+@f+'AL_45,'+@f+'AL_46,'+@f+'AL_47,'+@f+'AL_48
				,ValidStatus,ManualEnterDateTime,CUS_ID,[Status],DispatchDateTime'
 set @sqlSufix = N')
			VALUES (n.TI_ID,n.EventDate,n.Channel,n.DataSource_ID,
				case when dbo.sfclr_Utils_BitOperations2(Mask,0)=1 then n.VAL_01/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,0,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,1)=1 then n.VAL_02/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,1,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,2)=1 then n.VAL_03/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,2,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,3)=1 then n.VAL_04/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,3,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,4)=1 then n.VAL_05/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,4,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,5)=1 then n.VAL_06/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,5,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,6)=1 then n.VAL_07/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,6,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,7)=1 then n.VAL_08/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,7,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,8)=1 then n.VAL_09/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,8,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,9)=1 then n.VAL_10/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,9,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,10)=1 then n.VAL_11/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,10,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,11)=1 then n.VAL_12/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,11,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,12)=1 then n.VAL_13/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,12,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,13)=1 then n.VAL_14/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,13,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,14)=1 then n.VAL_15/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,14,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,15)=1 then n.VAL_16/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,15,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,16)=1 then n.VAL_17/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,16,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,17)=1 then n.VAL_18/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,17,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,18)=1 then n.VAL_19/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,18,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,19)=1 then n.VAL_20/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,19,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,20)=1 then n.VAL_21/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,20,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,21)=1 then n.VAL_22/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,21,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,22)=1 then n.VAL_23/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,22,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,23)=1 then n.VAL_24/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,23,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,24)=1 then n.VAL_25/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,24,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,25)=1 then n.VAL_26/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,25,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,26)=1 then n.VAL_27/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,26,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,27)=1 then n.VAL_28/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,27,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,28)=1 then n.VAL_29/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,28,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,29)=1 then n.VAL_30/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,29,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,30)=1 then n.VAL_31/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,30,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,31)=1 then n.VAL_32/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,31,n.IsDivOnCoeffTransformation) else NULL end,'
set @sqlSufix1=N'case when dbo.sfclr_Utils_BitOperations2(Mask,32)=1 then n.VAL_33/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,32,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,33)=1 then n.VAL_34/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,33,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,34)=1 then n.VAL_35/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,34,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,35)=1 then n.VAL_36/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,35,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,36)=1 then n.VAL_37/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,36,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,37)=1 then n.VAL_38/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,37,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,38)=1 then n.VAL_39/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,38,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,39)=1 then n.VAL_40/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,39,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,40)=1 then n.VAL_41/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,40,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,41)=1 then n.VAL_42/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,41,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,42)=1 then n.VAL_43/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,42,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,43)=1 then n.VAL_44/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,43,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,44)=1 then n.VAL_45/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,44,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,45)=1 then n.VAL_46/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,45,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,46)=1 then n.VAL_47/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,46,n.IsDivOnCoeffTransformation) else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,47)=1 then n.VAL_48/dbo.usf2_Info_CoeffTransformators(n.TI_ID,n.EventDate,47,n.IsDivOnCoeffTransformation) else NULL end,
			n.ValidStatus,n.DispatchDateTime,n.CUS_ID,n.[Status],n.DispatchDateTime'

set @sqlC2 = ',case when (@EventParam=2 or @EventParam=3) then [ExistValueMask] else 0 end,case when @EventParam<>2 then [Mask] else 0 end);'

--Для удаления старых данных
if (@IsChangeDataSource = 1) begin
	set @DataSource_ID = (select top 1 DataSource_ID from Expl_DataSource_List where DataSourceType = @OldDataSourceType)
	if (@DataSource_ID is null) set @IsChangeDataSource = 0;
	set @sqlUpdate = N'update <table> set 
		VAL_01=(case when dbo.sfclr_Utils_BitOperations2(Mask,0)=0 or a.VAL_01 is null then t.VAL_01 else null end),   
		VAL_02=(case when dbo.sfclr_Utils_BitOperations2(Mask,1)=0 or a.VAL_02 is null then t.VAL_02 else null end),   
		VAL_03=(case when dbo.sfclr_Utils_BitOperations2(Mask,2)=0 or a.VAL_03 is null then t.VAL_03 else null end),   
		VAL_04=(case when dbo.sfclr_Utils_BitOperations2(Mask,3)=0 or a.VAL_04 is null then t.VAL_04 else null end),   
		VAL_05=(case when dbo.sfclr_Utils_BitOperations2(Mask,4)=0 or a.VAL_05 is null then t.VAL_05 else null end), 
		VAL_06=(case when dbo.sfclr_Utils_BitOperations2(Mask,5)=0 or a.VAL_06 is null then t.VAL_06 else null end),
		VAL_07=(case when dbo.sfclr_Utils_BitOperations2(Mask,6)=0 or a.VAL_07 is null then t.VAL_07 else null end),
		VAL_08=(case when dbo.sfclr_Utils_BitOperations2(Mask,7)=0 or a.VAL_08 is null then t.VAL_08 else null end),
		VAL_09=(case when dbo.sfclr_Utils_BitOperations2(Mask,8)=0 or a.VAL_09 is null then t.VAL_09 else null end),
		VAL_10=(case when dbo.sfclr_Utils_BitOperations2(Mask,9)=0 or a.VAL_10 is null then t.VAL_10 else null end),    
		VAL_11=(case when dbo.sfclr_Utils_BitOperations2(Mask,10)=0 or a.VAL_11 is null then t.VAL_11 else null end),   
		VAL_12=(case when dbo.sfclr_Utils_BitOperations2(Mask,11)=0 or a.VAL_12 is null then t.VAL_12 else null end),   
		VAL_13=(case when dbo.sfclr_Utils_BitOperations2(Mask,12)=0 or a.VAL_13 is null then t.VAL_13 else null end),   
		VAL_14=(case when dbo.sfclr_Utils_BitOperations2(Mask,13)=0 or a.VAL_14 is null then t.VAL_14 else null end),   
		VAL_15=(case when dbo.sfclr_Utils_BitOperations2(Mask,14)=0 or a.VAL_15 is null then t.VAL_15 else null end),  
		VAL_16=(case when dbo.sfclr_Utils_BitOperations2(Mask,15)=0 or a.VAL_16 is null then t.VAL_16 else null end),   
		VAL_17=(case when dbo.sfclr_Utils_BitOperations2(Mask,16)=0 or a.VAL_17 is null then t.VAL_17 else null end),   
		VAL_18=(case when dbo.sfclr_Utils_BitOperations2(Mask,17)=0 or a.VAL_18 is null then t.VAL_18 else null end),   
		VAL_19=(case when dbo.sfclr_Utils_BitOperations2(Mask,18)=0 or a.VAL_19 is null then t.VAL_19 else null end),   
		VAL_20=(case when dbo.sfclr_Utils_BitOperations2(Mask,19)=0 or a.VAL_20 is null then t.VAL_20 else null end),    
		VAL_21=(case when dbo.sfclr_Utils_BitOperations2(Mask,20)=0 or a.VAL_21 is null then t.VAL_21 else null end),   
		VAL_22=(case when dbo.sfclr_Utils_BitOperations2(Mask,21)=0 or a.VAL_22 is null then t.VAL_22 else null end),   
		VAL_23=(case when dbo.sfclr_Utils_BitOperations2(Mask,22)=0 or a.VAL_23 is null then t.VAL_23 else null end),   
		VAL_24=(case when dbo.sfclr_Utils_BitOperations2(Mask,23)=0 or a.VAL_24 is null then t.VAL_24 else null end),   
		VAL_25=(case when dbo.sfclr_Utils_BitOperations2(Mask,24)=0 or a.VAL_25 is null then t.VAL_25 else null end),   
		VAL_26=(case when dbo.sfclr_Utils_BitOperations2(Mask,25)=0 or a.VAL_26 is null then t.VAL_26 else null end),   
		VAL_27=(case when dbo.sfclr_Utils_BitOperations2(Mask,26)=0 or a.VAL_27 is null then t.VAL_27 else null end),   
		VAL_28=(case when dbo.sfclr_Utils_BitOperations2(Mask,27)=0 or a.VAL_28 is null then t.VAL_28 else null end),   
		VAL_29=(case when dbo.sfclr_Utils_BitOperations2(Mask,28)=0 or a.VAL_29 is null then t.VAL_29 else null end),
		VAL_30=(case when dbo.sfclr_Utils_BitOperations2(Mask,29)=0 or a.VAL_30 is null then t.VAL_30 else null end),   
		VAL_31=(case when dbo.sfclr_Utils_BitOperations2(Mask,30)=0 or a.VAL_31 is null then t.VAL_31 else null end),   
		VAL_32=(case when dbo.sfclr_Utils_BitOperations2(Mask,31)=0 or a.VAL_32 is null then t.VAL_32 else null end),   
		VAL_33=(case when dbo.sfclr_Utils_BitOperations2(Mask,32)=0 or a.VAL_33 is null then t.VAL_33 else null end),'   
set @sqlUpdate1 = N'VAL_34=(case when dbo.sfclr_Utils_BitOperations2(Mask,33)=0 or a.VAL_34 is null then t.VAL_34 else null end),   
		VAL_35=(case when dbo.sfclr_Utils_BitOperations2(Mask,34)=0 or a.VAL_35 is null then t.VAL_35 else null end),
		VAL_36=(case when dbo.sfclr_Utils_BitOperations2(Mask,35)=0 or a.VAL_36 is null then t.VAL_36 else null end),   
		VAL_37=(case when dbo.sfclr_Utils_BitOperations2(Mask,36)=0 or a.VAL_37 is null then t.VAL_37 else null end),   
		VAL_38=(case when dbo.sfclr_Utils_BitOperations2(Mask,37)=0 or a.VAL_38 is null then t.VAL_38 else null end),   
		VAL_39=(case when dbo.sfclr_Utils_BitOperations2(Mask,38)=0 or a.VAL_39 is null then t.VAL_39 else null end),   
		VAL_40=(case when dbo.sfclr_Utils_BitOperations2(Mask,39)=0 or a.VAL_40 is null then t.VAL_40 else null end),   
		VAL_41=(case when dbo.sfclr_Utils_BitOperations2(Mask,40)=0 or a.VAL_41 is null then t.VAL_41 else null end),   
		VAL_42=(case when dbo.sfclr_Utils_BitOperations2(Mask,41)=0 or a.VAL_42 is null then t.VAL_42 else null end),   
		VAL_43=(case when dbo.sfclr_Utils_BitOperations2(Mask,42)=0 or a.VAL_43 is null then t.VAL_43 else null end),   
		VAL_44=(case when dbo.sfclr_Utils_BitOperations2(Mask,43)=0 or a.VAL_44 is null then t.VAL_44 else null end),   
		VAL_45=(case when dbo.sfclr_Utils_BitOperations2(Mask,44)=0 or a.VAL_45 is null then t.VAL_45 else null end),   
		VAL_46=(case when dbo.sfclr_Utils_BitOperations2(Mask,45)=0 or a.VAL_46 is null then t.VAL_46 else null end),   
		VAL_47=(case when dbo.sfclr_Utils_BitOperations2(Mask,46)=0 or a.VAL_47 is null then t.VAL_47 else null end),   
		VAL_48=(case when dbo.sfclr_Utils_BitOperations2(Mask,47)=0 or a.VAL_48 is null then t.VAL_48 else null end)
		from <table> t
		join 
		(
			select a.* , --Данные, который обнуляем
			dbo.usf2_Utils_BitRange(cast(DATEDIFF(minute, a.eventDate, c.StartChannelStatus) / 30 as tinyint), cast(DATEDIFF(minute, a.EventDate, c.FinishChannelStatus) / 30 as tinyint), 0) & ExistValueMask as Mask, --Маска получсаовок по которой обнуляем
			c.ChannelType as Channel, --Канал с учетом переворота, по которому смотрим

			ISNULL((select top 1 DataSource_ID 
				from Expl_DataSource_List 
				where DataSourceType = a.DataSourceType), 
				(select top 1 p.DataSource_ID 
				from Expl_DataSource_PriorityList p
				where p.Year = Year(a.EventDate) and p.Month = MONTH(a.EventDate)
				order by Priority desc)) as DataSource_ID,

			ti.TIType

			from @Arch30ValuesTable a
			join Info_TI ti on ti.TI_ID = a.TI_ID 
			outer apply dbo.usf2_ArchCalcChannelInversionStatus(a.TI_ID, a.ChannelType, a.EventDate, DATEADD(minute, 1439, a.EventDate), case when ti.AIATSCode=2 then 1 else 0 end) c
			
		) a on t.TI_ID = a.TI_ID and t.EventDate = a.EventDate and t.ChannelType = a.Channel and t.DataSource_ID = @DataSource_ID
		where titype = @titype';
end;

if (@IsCA = 1) begin
	set @sqlexecuted = 'MERGE usp2_ArchComm_Update_Contr_30_Import_From_XML AS a USING (select a.*, dbo.usf2_ReverseTariffChannel(1, a.[ChannelType], ti.AOATSCode,ti.AIATSCode,ti.ROATSCode,ti.RIATSCode, a.TI_ID, a.[EventDate], a.[EventDate]) as Channel from @Arch30ValuesTable a join Info_Contr_TI ti on ti.ContrTI_ID = a.TI_ID) AS n ' 
	+ @sqlPerf + @sqlPerf1 + @sqlPerf2 + @sqlC1	+ @sqlMiddle + ',ContrReplaceStatus,ManualEnterStatus'+ @sqlSufix+ @sqlSufix1 + @sqlC2;
	EXEC sp_executesql @sqlexecuted, N'@EventParam tinyint, @Arch30ValuesTable Arch30VirtualValuesType READONLY', @EventParam, @Arch30VirtualValuesTable
end else begin

	declare @TIType tinyint;
	declare t cursor local FAST_FORWARD for select distinct TIType 	from Info_TI where ti_id in (select distinct ti_id from @Arch30VirtualValuesTable)

	open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN

		--Не обрабатываются малые точки
		if (@TIType < 10) begin
			 set @sqlTable = 'ArchCalc_30_Virtual';
		end else begin
			set @sqlTable = 'ArchCalcBit_30_Virtual_' + ltrim(str(@TIType - 10,2));
		end;
	
		set @sqlexecuted = 'MERGE '+@sqlTable+' AS a USING (select a.*, ti.titype,' +
			--Здесь выбираем приоритетный источник
			' ISNULL((select top 1 DataSource_ID 
			from Expl_DataSource_List 
			where DataSourceType = a.DataSourceType), 
			(select top 1 p.DataSource_ID 
			from Expl_DataSource_PriorityList p
			where p.Year = Year(a.EventDate) and p.Month = MONTH(a.EventDate)
			order by Priority desc)) as DataSource_ID,
			c.ChannelType as Channel,  dbo.usf2_Utils_BitRange(cast(DATEDIFF(minute, a.eventDate, c.StartChannelStatus) / 30 as tinyint), cast(DATEDIFF(minute, a.EventDate, c.FinishChannelStatus) / 30 as tinyint), 0) & ExistValueMask as Mask 
			from @Arch30ValuesTable a join Info_TI ti on ti.TI_ID = a.TI_ID 
			outer apply dbo.usf2_ArchCalcChannelInversionStatus(a.TI_ID, a.ChannelType, a.EventDate, DATEADD(minute, 1439, a.EventDate), case when ti.AIATSCode=2 then 1 else 0 end) c 
			where titype = @titype and dbo.usf2_Utils_BitRange(cast(DATEDIFF(minute, a.eventDate, c.StartChannelStatus) / 30 as tinyint), cast(DATEDIFF(minute, a.EventDate, c.FinishChannelStatus) / 30 as tinyint), 0) & ExistValueMask > 0) AS n '
			 + @sqlPerf + @sqlPerf1 + @sqlPerf2+ @sqlC1 + @sqlMiddle + ',ContrReplaceStatus,ManualEnterStatus' + @sqlSufix+ @sqlSufix1 + @sqlC2;
		
		--select @sqlexecuted
		EXEC sp_executesql @sqlexecuted, N'@titype tinyint, @EventParam tinyint, @Arch30ValuesTable Arch30VirtualValuesType READONLY', @TIType, @EventParam, @Arch30VirtualValuesTable

		if (@IsChangeDataSource = 1) begin
			--Пишем NULL в данные по старому источнику,  саму запись не удаляем
			set @sqlexecuted = REPLACE(@sqlUpdate + @sqlUpdate1, '<table>', @sqlTable);
			EXEC sp_executesql @sqlexecuted, N'@titype tinyint,@DataSource_ID tinyint, @Arch30ValuesTable Arch30VirtualValuesType READONLY', @TIType, @DataSource_ID, @Arch30VirtualValuesTable;
			--select @sqlexecuted
		END;
	FETCH NEXT FROM t into @TIType
	end;
	CLOSE t
	DEALLOCATE t
end;

	--Пишем в таблицу флагов с причинами замещения
	MERGE [dbo].[ArchComm_ReasonsOfAbsentData] as a  USING
	(
		select 	ti_id, Channel as ChannelType, DispatchDateTime, a.CUS_ID, ReasonsOfAbsentData, Min(EventDate) as StartDate, Max(EventDate) as FinishDate
		from
		(
			select a.*, c.ChannelType as Channel
			from @Arch30VirtualValuesTable a
			join Info_TI ti on a.TI_ID = ti.TI_ID
			outer apply dbo.usf2_ArchCalcChannelInversionStatus(a.TI_ID, a.ChannelType, a.EventDate, DATEADD(minute, 1439, a.EventDate), case when ti.AIATSCode=2 then 1 else 0 end) c
			where ReasonsOfAbsentData is not null
		) a
		group by ti_id, Channel, DispatchDateTime,CUS_ID,ReasonsOfAbsentData
	) n
	ON a.TI_ID = n.TI_ID and a.ChannelType = n.ChannelType and a.[StartDateTime] = n.StartDate
	WHEN MATCHED THEN UPDATE SET [FinishDateTime] = n.FinishDate, a.[Reason] = n.ReasonsOfAbsentData, a.[DispatchDateTime] = n.[DispatchDateTime], a.CUS_ID = n.CUS_ID
	WHEN NOT MATCHED THEN 
    INSERT (ti_id, ChannelType, StartDateTime, FinishDateTime, Reason, DispatchDateTime, CUS_ID)
	VALUES (ti_id, ChannelType, StartDate, FinishDate, ReasonsOfAbsentData, DispatchDateTime, CUS_ID);

	declare 
	@localSession uniqueidentifier,
	@DispatchDateTime DateTime,
	@CUS_ID tinyint;

	----Пишем в журнал событий-------------------------------------
	IF Cursor_Status('global', 'c_virtual') > -3 begin 
		CLOSE c_virtual
		DEALLOCATE c_virtual
	end

	declare c_virtual cursor for select ti_id, ChannelType, DispatchDateTime, t.CUS_ID, 
		DateAdd(n, dbo.usf2_Utils_GetFirstSmallBitNumber((select top 1 ExistValueMask from @Arch30VirtualValuesTable where TI_ID = t.TI_ID and ChannelType = t.ChannelType and EventDate = t.StartDate)) * 30, t.StartDate) as StartDate, 
		DateAdd(n, dbo.usf2_Utils_GetLastBitNumber((select top 1 ExistValueMask from @Arch30VirtualValuesTable where TI_ID = t.TI_ID and ChannelType = t.ChannelType and EventDate = t.FinishDate)) * 30 + 29, t.FinishDate) as FinishDate
	from 
	(
		select ti_id, ChannelType, DispatchDateTime, CUS_ID, Min(EventDate) as StartDate, Max(EventDate) as FinishDate
		from @Arch30VirtualValuesTable
		group by ti_id, ChannelType,DispatchDateTime,CUS_ID
	) t
		
	open c_virtual;
	FETCH NEXT FROM c_virtual into @ti_ID, @ChannelType, @DispatchDateTime, @CUS_ID, @StartDate, @FinishDate
	WHILE @@FETCH_STATUS = 0
	BEGIN
		--Пишем или обновляем запись в журнале замещений пользователей
		if not exists (select TI_ID,ChannelType,EventDate,[User_ID],EventDateTime from dbo.Expl_User_Journal_Replace_30_Virtual vv 
		where vv.ti_id = @ti_id and vv.ChannelType = @ChannelType and vv.EventDate = @StartDate and vv.[User_ID] = @User_ID 
		and vv.EventDateTime = @DispatchDateTime) begin 

			insert dbo.Expl_User_Journal_Replace_30_Virtual (TI_ID, ChannelType, EventDate, [User_ID], EventDateTime, EventParam, CommentString, CUS_ID, ZamerDateTime)
			select @ti_ID, @ChannelType,@StartDate as EventDate, @User_ID, @DispatchDateTime, @EventParam, @CommentString, @CUS_ID, @FinishDate as  ZamerDateTime
		end else begin 
			update dbo.Expl_User_Journal_Replace_30_Virtual
			set  ti_ID = @ti_ID, ChannelType = @ChannelType,EventDate = @StartDate, [User_ID] = @User_ID, EventDateTime =@DispatchDateTime, EventParam = @EventParam, CommentString = @CommentString, CUS_ID = @CUS_ID, ZamerDateTime = @FinishDate 
			where Expl_User_Journal_Replace_30_Virtual.ti_id = @ti_id and Expl_User_Journal_Replace_30_Virtual.ChannelType = @ChannelType and Expl_User_Journal_Replace_30_Virtual.EventDate = @StartDate
				and Expl_User_Journal_Replace_30_Virtual.[User_ID] = @User_ID and Expl_User_Journal_Replace_30_Virtual.EventDateTime = @DispatchDateTime 
		end
			
		--Пишем или обновляем запись в таблице замещений по акту недоучета
		if (@EventParam = 9 and @AddedValue is not null and not exists (select * from dbo.ArchComm_Replace_ActUndercount 
		where ti_id = @ti_id and ChannelType = @channelType and ReplaceSession = @ReplaceSession)) begin 
				
			set @localSession = (select top 1 ReplaceSession from dbo.ArchComm_Replace_ActUndercount 
			where ti_id = @ti_id and ChannelType = @channelType and EventDateTime = @StartDate);
				
			if (@localSession is null)  begin
				insert dbo.ArchComm_Replace_ActUndercount(TI_ID, ChannelType, EventDateTime, AddedValue, CommentString, CUS_ID, ReplaceSession)
				values(@TI_ID, @ChannelType, @StartDate, @AddedValue, @CommentString, @CUS_ID, @ReplaceSession)
			end else if (@localSession <> @ReplaceSession) begin
				update dbo.ArchComm_Replace_ActUndercount
				set AddedValue = @AddedValue, CommentString = @CommentString, CUS_ID = @CUS_ID, ReplaceSession = @ReplaceSession
				where TI_ID = @ti_id and ChannelType = @ChannelType and EventDateTime = @StartDate
			end;
				
			set @localSession = null;
		end;		
		FETCH NEXT FROM c_virtual into @ti_ID, @ChannelType, @DispatchDateTime, @CUS_ID, @StartDate, @FinishDate
	END
	CLOSE c_virtual
	DEALLOCATE c_virtual

end
go
   grant EXECUTE on usp2_WriteArch30VirtualValues to [UserCalcService]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Февраль, 2013
--
-- Описание:
--
--		Пишем таблицу 30 минуток расчетного профиля в базу
--
-- ======================================================================================
create proc [dbo].[usp_DataCollect_WriteArch30Values]
	@Arch30VirtualValuesTable Arch30VirtualValuesType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off

declare
@DataSource_ID int,
@f nvarchar(1),
@sqlPerf nvarchar(max),
@sqlPerf1 nvarchar(max),
@sqlPerf2 nvarchar(max),
@sqlSufix nvarchar(max),
@sqlMiddle nvarchar(max),
@sqlexecuted nvarchar(max),
@sqlUpdate nvarchar(max),
@sqlUpdate1 nvarchar(max),
@sqlTable nvarchar(200)
--@reversedChannel nvarchar(300);

--Сначала необходимо определить идентификатор пользователя
declare @ti_ID int, @ChannelType int, @StartDate DateTime, @FinishDate DateTime;

set @sqlPerf = N'ON a.TI_ID = n.TI_ID and a.EventDate = n.EventDate and a.ChannelType = n.ChannelType 
				WHEN MATCHED THEN UPDATE SET 
				VAL_01=case when dbo.sfclr_Utils_BitOperations2(Mask,0)=1 then n.VAL_01 else a.VAL_01 end,
				VAL_02=case when dbo.sfclr_Utils_BitOperations2(Mask,1)=1 then n.VAL_02 else a.VAL_02 end,
				VAL_03=case when dbo.sfclr_Utils_BitOperations2(Mask,2)=1 then n.VAL_03 else a.VAL_03 end,
				VAL_04=case when dbo.sfclr_Utils_BitOperations2(Mask,3)=1 then n.VAL_04 else a.VAL_04 end,
				VAL_05=case when dbo.sfclr_Utils_BitOperations2(Mask,4)=1 then n.VAL_05 else a.VAL_05 end,
				VAL_06=case when dbo.sfclr_Utils_BitOperations2(Mask,5)=1 then n.VAL_06 else a.VAL_06 end,
				VAL_07=case when dbo.sfclr_Utils_BitOperations2(Mask,6)=1 then n.VAL_07 else a.VAL_07 end,
				VAL_08=case when dbo.sfclr_Utils_BitOperations2(Mask,7)=1 then n.VAL_08 else a.VAL_08 end,
				VAL_09=case when dbo.sfclr_Utils_BitOperations2(Mask,8)=1 then n.VAL_09 else a.VAL_09 end,
				VAL_10=case when dbo.sfclr_Utils_BitOperations2(Mask,9)=1 then n.VAL_10 else a.VAL_10 end,
				VAL_11=case when dbo.sfclr_Utils_BitOperations2(Mask,10)=1 then n.VAL_11 else a.VAL_11 end,
				VAL_12=case when dbo.sfclr_Utils_BitOperations2(Mask,11)=1 then n.VAL_12 else a.VAL_12 end,
				VAL_13=case when dbo.sfclr_Utils_BitOperations2(Mask,12)=1 then n.VAL_13 else a.VAL_13 end,
				VAL_14=case when dbo.sfclr_Utils_BitOperations2(Mask,13)=1 then n.VAL_14 else a.VAL_14 end,
				VAL_15=case when dbo.sfclr_Utils_BitOperations2(Mask,14)=1 then n.VAL_15 else a.VAL_15 end,
				VAL_16=case when dbo.sfclr_Utils_BitOperations2(Mask,15)=1 then n.VAL_16 else a.VAL_16 end,'
set @sqlPerf1=N'VAL_17=case when dbo.sfclr_Utils_BitOperations2(Mask,16)=1 then n.VAL_17 else a.VAL_17 end,
				VAL_18=case when dbo.sfclr_Utils_BitOperations2(Mask,17)=1 then n.VAL_18 else a.VAL_18 end,
				VAL_19=case when dbo.sfclr_Utils_BitOperations2(Mask,18)=1 then n.VAL_19 else a.VAL_19 end,
				VAL_20=case when dbo.sfclr_Utils_BitOperations2(Mask,19)=1 then n.VAL_20 else a.VAL_20 end,
				VAL_21=case when dbo.sfclr_Utils_BitOperations2(Mask,20)=1 then n.VAL_21 else a.VAL_21 end,
				VAL_22=case when dbo.sfclr_Utils_BitOperations2(Mask,21)=1 then n.VAL_22 else a.VAL_22 end,
				VAL_23=case when dbo.sfclr_Utils_BitOperations2(Mask,22)=1 then n.VAL_23 else a.VAL_23 end,
				VAL_24=case when dbo.sfclr_Utils_BitOperations2(Mask,23)=1 then n.VAL_24 else a.VAL_24 end,
				VAL_25=case when dbo.sfclr_Utils_BitOperations2(Mask,24)=1 then n.VAL_25 else a.VAL_25 end,
				VAL_26=case when dbo.sfclr_Utils_BitOperations2(Mask,25)=1 then n.VAL_26 else a.VAL_26 end,
				VAL_27=case when dbo.sfclr_Utils_BitOperations2(Mask,26)=1 then n.VAL_27 else a.VAL_27 end,
				VAL_28=case when dbo.sfclr_Utils_BitOperations2(Mask,27)=1 then n.VAL_28 else a.VAL_28 end,
				VAL_29=case when dbo.sfclr_Utils_BitOperations2(Mask,28)=1 then n.VAL_29 else a.VAL_29 end,
				VAL_30=case when dbo.sfclr_Utils_BitOperations2(Mask,29)=1 then n.VAL_30 else a.VAL_30 end,
				VAL_31=case when dbo.sfclr_Utils_BitOperations2(Mask,30)=1 then n.VAL_31 else a.VAL_31 end,
				VAL_32=case when dbo.sfclr_Utils_BitOperations2(Mask,31)=1 then n.VAL_32 else a.VAL_32 end,
				VAL_33=case when dbo.sfclr_Utils_BitOperations2(Mask,32)=1 then n.VAL_33 else a.VAL_33 end,
				VAL_34=case when dbo.sfclr_Utils_BitOperations2(Mask,33)=1 then n.VAL_34 else a.VAL_34 end,
				VAL_35=case when dbo.sfclr_Utils_BitOperations2(Mask,34)=1 then n.VAL_35 else a.VAL_35 end, 
				VAL_36=case when dbo.sfclr_Utils_BitOperations2(Mask,35)=1 then n.VAL_36 else a.VAL_36 end,'
set @sqlPerf2=N'VAL_37=case when dbo.sfclr_Utils_BitOperations2(Mask,36)=1 then n.VAL_37 else a.VAL_37 end,
				VAL_38=case when dbo.sfclr_Utils_BitOperations2(Mask,37)=1 then n.VAL_38 else a.VAL_38 end,
				VAL_39=case when dbo.sfclr_Utils_BitOperations2(Mask,38)=1 then n.VAL_39 else a.VAL_39 end,
				VAL_40=case when dbo.sfclr_Utils_BitOperations2(Mask,39)=1 then n.VAL_40 else a.VAL_40 end,
				VAL_41=case when dbo.sfclr_Utils_BitOperations2(Mask,40)=1 then n.VAL_41 else a.VAL_41 end,
				VAL_42=case when dbo.sfclr_Utils_BitOperations2(Mask,41)=1 then n.VAL_42 else a.VAL_42 end,
				VAL_43=case when dbo.sfclr_Utils_BitOperations2(Mask,42)=1 then n.VAL_43 else a.VAL_43 end,
				VAL_44=case when dbo.sfclr_Utils_BitOperations2(Mask,43)=1 then n.VAL_44 else a.VAL_44 end,
				VAL_45=case when dbo.sfclr_Utils_BitOperations2(Mask,44)=1 then n.VAL_45 else a.VAL_45 end,
				VAL_46=case when dbo.sfclr_Utils_BitOperations2(Mask,45)=1 then n.VAL_46 else a.VAL_46 end,
				VAL_47=case when dbo.sfclr_Utils_BitOperations2(Mask,46)=1 then n.VAL_47 else a.VAL_47 end,
				VAL_48=case when dbo.sfclr_Utils_BitOperations2(Mask,47)=1 then n.VAL_48 else a.VAL_48 end,
				CUS_ID=n.CUS_ID'
set @sqlMiddle = ',ValidStatus = (a.ValidStatus & (~[ExistValueMask])) | n.ValidStatus
				WHEN NOT MATCHED THEN 
				INSERT (TI_ID,EventDate,ChannelType,VAL_01,VAL_02,VAL_03,VAL_04,VAL_05,VAL_06,VAL_07,VAL_08,VAL_09,VAL_10
				,VAL_11,VAL_12,VAL_13,VAL_14,VAL_15,VAL_16,VAL_17,VAL_18,VAL_19,VAL_20,VAL_21,VAL_22,VAL_23,VAL_24,VAL_25,VAL_26,VAL_27,VAL_28,VAL_29,VAL_30
				,VAL_31,VAL_32,VAL_33,VAL_34,VAL_35,VAL_36,VAL_37,VAL_38,VAL_39,VAL_40,VAL_41,VAL_42,VAL_43,VAL_44,VAL_45,VAL_46,VAL_47,VAL_48
				,ValidStatus,DispatchDateTime,CUS_ID,[Status]'
 set @sqlSufix = N')
			VALUES (n.TI_ID,n.EventDate,n.ChannelType,
				case when dbo.sfclr_Utils_BitOperations2(Mask,0)=1 then n.VAL_01 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,1)=1 then n.VAL_02 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,2)=1 then n.VAL_03 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,3)=1 then n.VAL_04 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,4)=1 then n.VAL_05 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,5)=1 then n.VAL_06 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,6)=1 then n.VAL_07 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,7)=1 then n.VAL_08 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,8)=1 then n.VAL_09 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,9)=1 then n.VAL_10 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,10)=1 then n.VAL_11 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,11)=1 then n.VAL_12 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,12)=1 then n.VAL_13 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,13)=1 then n.VAL_14 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,14)=1 then n.VAL_15 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,15)=1 then n.VAL_16 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,16)=1 then n.VAL_17 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,17)=1 then n.VAL_18 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,18)=1 then n.VAL_19 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,19)=1 then n.VAL_20 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,20)=1 then n.VAL_21 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,21)=1 then n.VAL_22 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,22)=1 then n.VAL_23 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,23)=1 then n.VAL_24 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,24)=1 then n.VAL_25 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,25)=1 then n.VAL_26 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,26)=1 then n.VAL_27 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,27)=1 then n.VAL_28 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,28)=1 then n.VAL_29 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,29)=1 then n.VAL_30 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,30)=1 then n.VAL_31 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,31)=1 then n.VAL_32 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,32)=1 then n.VAL_33 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,33)=1 then n.VAL_34 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,34)=1 then n.VAL_35 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,35)=1 then n.VAL_36 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,36)=1 then n.VAL_37 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,37)=1 then n.VAL_38 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,38)=1 then n.VAL_39 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,39)=1 then n.VAL_40 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,40)=1 then n.VAL_41 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,41)=1 then n.VAL_42 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,42)=1 then n.VAL_43 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,43)=1 then n.VAL_44 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,44)=1 then n.VAL_45 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,45)=1 then n.VAL_46 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,46)=1 then n.VAL_47 else NULL end,
				case when dbo.sfclr_Utils_BitOperations2(Mask,47)=1 then n.VAL_48 else NULL end,
			n.ValidStatus,n.DispatchDateTime,n.CUS_ID,n.[Status]);'

	declare @TIType tinyint;
	declare t cursor local FAST_FORWARD for select distinct TIType 	from Info_TI where ti_id in (select distinct ti_id from @Arch30VirtualValuesTable)

	open t;
	FETCH NEXT FROM t into @TIType
	WHILE @@FETCH_STATUS = 0
	BEGIN

		--Не обрабатываются малые точки
		if (@TIType < 10) begin
			 set @sqlTable = 'ArchComm_30_Values';
		end else begin
			set @sqlTable = 'ArchBit_30_Values_' + ltrim(str(@TIType - 10,2));
		end;
	
		set @sqlexecuted = 'MERGE '+@sqlTable+' AS a USING (select a.*, ti.titype,	ExistValueMask as Mask 
			from @Arch30ValuesTable a join Info_TI ti on ti.TI_ID = a.TI_ID 
			where titype = @titype and ExistValueMask > 0) AS n '
			 + @sqlPerf + @sqlPerf1 + @sqlPerf2+ @sqlMiddle + @sqlSufix;
		
		--select @sqlexecuted
		EXEC sp_executesql @sqlexecuted, N'@titype tinyint, @Arch30ValuesTable Arch30VirtualValuesType READONLY', @TIType,  @Arch30VirtualValuesTable

	
		FETCH NEXT FROM t into @TIType
	END;
	CLOSE t
	DEALLOCATE t
end

go
   grant EXECUTE on usp_DataCollect_WriteArch30Values to [UserCalcService]
go
	grant EXECUTE on usp_DataCollect_WriteArch30Values to [UserDataCollectorService]
go