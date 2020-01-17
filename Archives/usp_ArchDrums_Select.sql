if exists (select 1
          from sysobjects
          where  id = object_id('usp2_ArchDrums_Select')
          and type in ('P','PC'))
   drop procedure usp2_ArchDrums_Select
go

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
--		Июнь, 2011
--
-- Описание:
--
--		Выбираем интегральные значения на начало и на конец интервала для группы точек
--
-- ======================================================================================

create proc [dbo].[usp2_ArchDrums_Select]

	@TI_Array varchar(4000),
	@DateStart datetime,
	@DateEnd datetime
	

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
	@ti_id int, 
	@channeltype tinyint,
	@typeArchTable tinyint,
	@titype tinyint,
	@channel tinyint,
	@ps_id int,
	@tpcoeff float,
	@AIATSCode tinyint,@AOATSCode tinyint,@RIATSCode tinyint,@ROATSCode tinyint,
	@tableName varchar(255);

create table #tmp2
(
	ti_id int, 
	channelType tinyint,
	DirectChannelType tinyint, 
	PS_ID int, 
	TPCoeff float,
	TIType tinyint,
	EventDateTime DateTime, 
	[Data] float, 
	[Row] int
);


DECLARE @ParmDefinition NVARCHAR(1000);
SET @ParmDefinition = N'@ti_id int,@ChannelType tinyint,@channel tinyint, @titype tinyint,@ps_id int, @DateStart datetime, @DateEnd datetime, @tpcoeff float'
DECLARE @SQLString NVARCHAR(4000);

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select distinct TInumber,CHnumber, IsCAorOsOV as typeArchTable, TIType, PS_ID, TPCoefOurSide,AIATSCode,AOATSCode,RIATSCode,ROATSCode
from usf2_Utils_iterCA_intlist_to_table(@TI_Array) a
left join Info_TI ti on ti.TI_ID = TInumber

open t;
FETCH NEXT FROM t into @ti_id,@channeltype,@typeArchTable,@titype,@ps_id,@tpcoeff,@AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode 
WHILE @@FETCH_STATUS = 0
BEGIN
	
	--Определяемся с названием таблицы
	if (@TypeArchTable = 0) begin ----Основной профиль
		IF @titype>10 BEGIN
			set @tableName = 'dbo.ArchBit_Integrals_' + + ltrim(str(@TIType - 10,2));
		END ELSE BEGIN
			set @tableName = 'dbo.ArchComm_Integrals';
		END;
	END ELSE BEGIN ----Расчетный профиль
		IF @titype>10 BEGIN
			set @tableName = 'dbo.ArchCalcBit_Integrals_Virtual_' + + ltrim(str(@TIType - 10,2));
		END ELSE BEGIN
			set @tableName = 'dbo.ArchComm_Integrals';
		END;
	END;
	
	--Смотрим инверсию канала
	set @channel = dbo.usf2_ReverseTariffChannel(0, @channeltype, @AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode, @ti_id, @DateStart, @DateEnd)
	
	SET @SQLString = 'insert into #tmp2
	select p.ti_id, p.channel, p.channeltype, p.ps_id, p.tpcoeff, p.titype, EventDateTime, [Data], row_number() over (order by p.ti_id, p.channelType, arh.EventDateTime) as [Row] 
	from 
	(	
		select @ti_id as ti_id, @channel as channel, @channeltype as channeltype, @ps_id as ps_id, @tpcoeff as tpcoeff, @titype as titype
	) p
	left join ' + @tableName + ' arh
	on arh.TI_ID = p.TI_ID and arh.IntegralType = 0 and arh.ChannelType = p.channel and arh.EventDateTime between @DateStart and @DateEnd
				where (arh.EventDateTime is null or (DatePart(n,arh.EventDateTime) = 0 or DatePart(n,arh.EventDateTime) = 30))'
				
	EXEC sp_executesql @SQLString, @ParmDefinition, @ti_id ,@ChannelType,@channel,@titype ,@ps_id , @DateStart , @DateEnd, @tpcoeff;
	
	FETCH NEXT FROM t into @ti_id,@channeltype,@typeArchTable,@titype,@ps_id,@tpcoeff,@AIATSCode,@AOATSCode,@RIATSCode,@ROATSCode 
END;
CLOSE t
DEALLOCATE t;	

with cte as
(
	select * from #tmp2
)

select c1.TI_ID, c1.DirectChannelType as ChannelType, c1.PS_ID, c1.TPCoeff, c1.TIType, c1.EventDateTime as dtStart, c2.EventDateTime  as dtEnd
	,c1.[Data] as valStart, c2.[Data] as valEnd
	,dbo.usf2_Info_CoeffTransformators(c1.TI_ID, c1.EventDateTime, 0, 1) as coeffStart, dbo.usf2_Info_CoeffTransformators(c2.TI_ID, c2.EventDateTime, 0, 1) as coeffEnd
	from cte c1
	left join  cte c2 
	on c1.[TI_ID] = c2.[TI_ID]
	 and c1.[ChannelType] = c2.[ChannelType]
	 and c1.[Row] = c2.[Row] - 1 
	order by c1.TI_ID, c1.ChannelType, c1.EventDateTime

drop table #tmp2

end
go
   grant EXECUTE on usp2_ArchDrums_Select to [UserCalcService]
go