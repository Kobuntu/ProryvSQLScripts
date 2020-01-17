if exists (select 1
          from sysobjects
          where  id = object_id('usp2_Report_Ведомость_опроса_счетчиков')
          and type in ('P','PC'))
   drop procedure usp2_Report_Ведомость_опроса_счетчиков
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
--		Октябрь, 2013
--
-- Описание:
--
--		Отчет ведомость опроса счетчиков
--
-- ======================================================================================
create proc [dbo].[usp2_Report_Ведомость_опроса_счетчиков]

@dtStart DateTime,
@dtEnd DateTime	

as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

create table #result
(
	TI_ID int,
	TIName varchar(1024),
	TIType tinyint,
	PS_ID int,
	AIATSCode int,
	Сумма float,
	Пиковая float,
	Полупиковая float,
	Ночная float,
	EventDateTime DateTime
)

declare 
@TI_ID int, 
@AIATSCode int,
@TIName varchar(1024),
	@TIType tinyint,
	@PS_ID int,
@Сумма float,
	@Пиковая float,
	@Полупиковая float,
	@Ночная float,
	@EventDateTimeStart dateTime,
	@ManualEnterDataStart float,
	@CoeffStart int, 
	@DataSourceTypeStart tinyint,
	@StatusStart int, @channel tinyint,
	@limitDays int;

set @dtEnd = DATEADD(hour, 2,  @dtEnd);
set @limitDays = ceiling(cast(DATEDIFF(MINUTE, @dtStart, @dtEnd) as float) / 1440);

declare t cursor LOCAL STATIC FORWARD_ONLY READ_ONLY for select top 60000 TI_ID, AIATSCode, TIName, TIType, PS_ID from Info_TI where NOT TIType IN (0, 10)
    open t;
	FETCH NEXT FROM t into @TI_ID, @AIATSCode, @TIName, @TIType, @PS_ID
	WHILE @@FETCH_STATUS = 0
	BEGIN

	set @Сумма = null;

	set @channel = ISNULL(@AIATSCode, 1);

	exec usp2_ArchBit_LastIntegral @TI_ID, @dtEnd, @channel , @limitDays, null, null, 0, 
	@Сумма output, @EventDateTimeStart output, @ManualEnterDataStart output, @CoeffStart output, @DataSourceTypeStart output, @StatusStart output;

	if (@Сумма is not null) begin
		set @channel = 50 + ISNULL(@AIATSCode, 1);
		exec usp2_ArchBit_LastIntegral @TI_ID, @dtEnd, @channel , @limitDays, null, null, 0, 
		@Пиковая output, @EventDateTimeStart output, @ManualEnterDataStart output, @CoeffStart output, @DataSourceTypeStart output, @StatusStart output;

		set @channel = 60 + ISNULL(@AIATSCode, 1);
		exec usp2_ArchBit_LastIntegral @TI_ID, @dtEnd, @channel , @limitDays, null, null, 0, 
		@Полупиковая output, @EventDateTimeStart output, @ManualEnterDataStart output, @CoeffStart output, @DataSourceTypeStart output, @StatusStart output;

		set @channel = 70 + ISNULL(@AIATSCode, 1);
		exec usp2_ArchBit_LastIntegral @TI_ID, @dtEnd, @channel , @limitDays, null, null, 0, 
		@Ночная output, @EventDateTimeStart output, @ManualEnterDataStart output, @CoeffStart output, @DataSourceTypeStart output, @StatusStart output;

		insert into #result values (@TI_ID, @TIName, @TIType, @PS_ID, @AIATSCode, @Сумма / 1000, @Пиковая / 1000, @Полупиковая / 1000, @Ночная / 1000, @EventDateTimeStart);
	end

	FETCH NEXT FROM t into @TI_ID, @AIATSCode, @TIName, @TIType, @PS_ID
	end;
	CLOSE t
	DEALLOCATE t

SELECT r.TI_ID as Номер_ТИ, r.TIName as Имя_ТИ, ps.StringName AS ИмяПС,
  al.BitAbonentCode as Код_Абонента, al.BitAbonentSurname as Фамилия, al.BitAbonentName as Имя,
  al.BitAbonentMiddleName as Отчество, hm.MeterSerialNumber as Серийный_номер_счетчика,
  mtti.StartDateTime as Дата_Установки_Счетчика, mtti.FinishDateTime as Дата_демонтажа_счетчика,
  r.TIType as Тип_Потребителя, r.PS_ID as Номер_ПС, ps.PSType as Тип_ПС, r.AIATSCode as Код_Канала_АП, Сумма, Пиковая
  , Полупиковая, Ночная, r.EventDateTime as ДатаВремяПоказаний
FROM #result r 
LEFT JOIN  Dict_PS ps ON r.PS_ID = ps.PS_ID
LEFT JOIN  InfoBit_Abonents_To_TI atti ON r.TI_ID = atti.TI_ID
LEFT JOIN InfoBit_Abonents_List al ON atti.BitAbonent_ID = al.BitAbonent_ID
LEFT JOIN Info_Meters_TO_TI mtti ON r.TI_ID =mtti.TI_ID and mtti.StartDateTime < GetDate() AND ISNULL(mtti.FinishDateTime, '21000101') > GetDate()
LEFT JOIN Hard_Meters hm ON hm.Meter_ID = mtti.METER_ID
ORDER BY ps.StringName, r.TIName
drop table #result
end
go
   grant EXECUTE on usp2_Report_Ведомость_опроса_счетчиков to [UserCalcService]
go



