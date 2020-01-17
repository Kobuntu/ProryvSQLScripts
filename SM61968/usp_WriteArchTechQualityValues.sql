if exists (select 1
          from sysobjects
          where  id = object_id('usp2_WriteArchTechQualityValues')
          and type in ('P','PC'))
 drop procedure usp2_WriteArchTechQualityValues
go

--Обновляем тип
--Удаляем если есть
IF  EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE st.name = N'ArchTechQualityValuesType' AND ss.name = N'dbo')
DROP TYPE [dbo].[ArchTechQualityValuesType]
-- Пересоздаем заново
CREATE TYPE [dbo].[ArchTechQualityValuesType] AS TABLE(
	[TI_ID] [dbo].[TI_ID_TYPE] NOT NULL,
	[EventDateTime] [datetime] NOT NULL,
	[MeterEventDateTime] [datetime] NULL,
	[VAL_100] [float] NULL,
	[VAL_101] [float] NULL,
	[VAL_102] [float] NULL,
	[VAL_103] [float] NULL,
	[VAL_104] [float] NULL,
	[VAL_105] [float] NULL,
	[VAL_110] [float] NULL,
	[VAL_111] [float] NULL,
	[VAL_112] [float] NULL,
	[VAL_113] [float] NULL,
	[VAL_114] [float] NULL,
	[VAL_115] [float] NULL,
	[VAL_120] [float] NULL,
	[VAL_121] [float] NULL,
	[VAL_122] [float] NULL,
	[VAL_123] [float] NULL,
	[VAL_124] [float] NULL,
	[VAL_125] [float] NULL,
	[VAL_130] [float] NULL,
	[VAL_131] [float] NULL,
	[VAL_132] [float] NULL,
	[VAL_133] [float] NULL,
	[VAL_134] [float] NULL,
	[VAL_135] [float] NULL,
	[VAL_140] [float] NULL,
	[VAL_141] [float] NULL,
	[VAL_142] [float] NULL,
	[VAL_143] [float] NULL,
	[VAL_144] [float] NULL,
	[VAL_145] [float] NULL,
	[VAL_150] [float] NULL,
	[VAL_151] [float] NULL,
	[VAL_152] [float] NULL,
	[VAL_153] [float] NULL,
	[VAL_154] [float] NULL,
	[VAL_155] [float] NULL,
	[VAL_160] [float] NULL,
	[VAL_161] [float] NULL,
	[VAL_162] [float] NULL,
	[VAL_163] [float] NULL,
	[VAL_164] [float] NULL,
	[VAL_165] [float] NULL,
	[VAL_170] [float] NULL,
	[VAL_171] [float] NULL,
	[VAL_172] [float] NULL,
	[VAL_173] [float] NULL,
	[VAL_174] [float] NULL,
	[VAL_175] [float] NULL,
	[VAL_180] [float] NULL,
	[VAL_181] [float] NULL,
	[VAL_182] [float] NULL,
	[VAL_183] [float] NULL,
	[VAL_184] [float] NULL,
	[VAL_185] [float] NULL,
	[VAL_190] [float] NULL,
	[VAL_191] [float] NULL,
	[VAL_192] [float] NULL,
	[VAL_193] [float] NULL,
	[VAL_194] [float] NULL,
	[VAL_195] [float] NULL,
	[VAL_200] [float] NULL,
	[VAL_201] [float] NULL,
	[VAL_202] [float] NULL,
	[VAL_203] [float] NULL,
	[VAL_204] [float] NULL,
	[VAL_205] [float] NULL,
	[VAL_210] [float] NULL,
	[VAL_211] [float] NULL,
	[VAL_212] [float] NULL,
	[VAL_213] [float] NULL,
	[VAL_214] [float] NULL,
	[VAL_215] [float] NULL,
	[VAL_220] [float] NULL,
	[VAL_225] [float] NULL,
	[VAL_226] [float] NULL,
	[VAL_227] [float] NULL,
	[VAL_230] [float] NULL,
	[VAL_231] [float] NULL,
	[VAL_232] [float] NULL,
	[VAL_235] [float] NULL,
	[VAL_236] [float] NULL,
	[VAL_237] [float] NULL,
	[VAL_240] [float] NULL,
	[VAL_241] [float] NULL,
	[VAL_242] [float] NULL,
	[DispatchDateTime] [datetime] NOT NULL,
	[CUS_ID] [dbo].[CUS_ID_TYPE] NOT NULL,
	[Status] [int] NOT NULL,
	[VAL_250] [float] NULL,
	[VAL_251] [float] NULL,
	[VAL_252] [float] NULL,
	[VAL_253] [float] NULL,
	[VAL_254] [float] NULL,
	[VAL_255] [float] NULL,
	PRIMARY KEY CLUSTERED 
(
	[TI_ID] ASC,
	[EventDateTime] ASC
)WITH (IGNORE_DUP_KEY = OFF)
)
GO

grant EXECUTE on TYPE::ArchTechQualityValuesType to [UserMaster61968Service]
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Март, 2012
--
-- Описание:
--
--		Пишем таблицу мгновенных в базу
--
-- ======================================================================================

create proc [dbo].[usp2_WriteArchTechQualityValues]
	@TIType tinyint, --Тип точки 
	@ArchTechQualityValuesTable ArchTechQualityValuesType READONLY --Таблицу которую пишем в базу данных
	
as
begin

set nocount on
set quoted_identifier, ansi_nulls, ansi_warnings, arithabort, concat_null_yields_null, ansi_padding on
set numeric_roundabort off
set transaction isolation level read uncommitted

declare
@str1 nvarchar(max),
@str2 nvarchar(max),
@str3 nvarchar(max),
@sqlcommand nvarchar(4000),
@tableName nvarchar(100);

if (@TIType < 10) begin
	set @tableName = 'ArchTech_Quality_Values'
end else begin
	set @tableName = 'ArchBit_Quality_Values_' + ltrim(str(@TIType - 10,2));
end;

--Используем временную таблицу
select * 
into #ArchTechQualityValuesTable
from @ArchTechQualityValuesTable

--Дробим на несколько переменных из за того что максимальная длина переменной ограничена 4000 символами
set @str1 = 'MERGE ' + @tableName + ' AS a
USING (SELECT * FROM #ArchTechQualityValuesTable) AS n
ON a.TI_ID = n.TI_ID and a.EventDateTime = n.EventDateTime
WHEN MATCHED THEN UPDATE SET VAL_100 = ISNULL(n.VAL_100, a.VAL_100),VAL_101 = ISNULL(n.VAL_101, a.VAL_101)
                ,VAL_102 = ISNULL(n.VAL_102, a.VAL_102),VAL_103 = ISNULL(n.VAL_103, a.VAL_103)
                ,VAL_104 = ISNULL(n.VAL_104, a.VAL_104),VAL_105 = ISNULL(n.VAL_105, a.VAL_105)
                ,VAL_110 = ISNULL(n.VAL_110, a.VAL_110),VAL_111 = ISNULL(n.VAL_111, a.VAL_111)
                ,VAL_112 = ISNULL(n.VAL_112, a.VAL_112),VAL_113 = ISNULL(n.VAL_113, a.VAL_113)
                ,VAL_114 = ISNULL(n.VAL_114, a.VAL_114),VAL_115 = ISNULL(n.VAL_115, a.VAL_115)
                ,VAL_120 = ISNULL(n.VAL_120, a.VAL_120),VAL_121 = ISNULL(n.VAL_121, a.VAL_121)
                ,VAL_122 = ISNULL(n.VAL_122, a.VAL_122),VAL_123 = ISNULL(n.VAL_123, a.VAL_123)
                ,VAL_124 = ISNULL(n.VAL_124, a.VAL_124),VAL_125 = ISNULL(n.VAL_125, a.VAL_125)
                ,VAL_130 = ISNULL(n.VAL_130, a.VAL_130),VAL_131 = ISNULL(n.VAL_131, a.VAL_131)
                ,VAL_132 = ISNULL(n.VAL_132, a.VAL_132),VAL_133 = ISNULL(n.VAL_133, a.VAL_133)
                ,VAL_134 = ISNULL(n.VAL_134, a.VAL_134),VAL_135 = ISNULL(n.VAL_135, a.VAL_135)
                ,VAL_140 = ISNULL(n.VAL_140, a.VAL_140),VAL_141 = ISNULL(n.VAL_141, a.VAL_141)
                ,VAL_142 = ISNULL(n.VAL_142, a.VAL_142),VAL_143 = ISNULL(n.VAL_143, a.VAL_143)
                ,VAL_144 = ISNULL(n.VAL_144, a.VAL_144),VAL_145 = ISNULL(n.VAL_145, a.VAL_145)
                ,VAL_150 = ISNULL(n.VAL_150, a.VAL_150),VAL_151 = ISNULL(n.VAL_151, a.VAL_151)
                ,VAL_152 = ISNULL(n.VAL_152, a.VAL_152),VAL_153 = ISNULL(n.VAL_153, a.VAL_153)
                ,VAL_154 = ISNULL(n.VAL_154, a.VAL_154),VAL_155 = ISNULL(n.VAL_155, a.VAL_155)
                ,VAL_160 = ISNULL(n.VAL_160, a.VAL_160),VAL_161 = ISNULL(n.VAL_161, a.VAL_161)
                ,VAL_162 = ISNULL(n.VAL_162, a.VAL_162),VAL_163 = ISNULL(n.VAL_163, a.VAL_163)
                ,VAL_164 = ISNULL(n.VAL_164, a.VAL_164),VAL_165 = ISNULL(n.VAL_165, a.VAL_165)
                ,VAL_170 = ISNULL(n.VAL_170, a.VAL_170),VAL_171 = ISNULL(n.VAL_171, a.VAL_171)
                ,VAL_172 = ISNULL(n.VAL_172, a.VAL_172),VAL_173 = ISNULL(n.VAL_173, a.VAL_173)
                ,VAL_174 = ISNULL(n.VAL_174, a.VAL_174),VAL_175 = ISNULL(n.VAL_175, a.VAL_175)
                ,VAL_180 = ISNULL(n.VAL_180, a.VAL_180),VAL_181 = ISNULL(n.VAL_181, a.VAL_181)
                ,VAL_182 = ISNULL(n.VAL_182, a.VAL_182),VAL_183 = ISNULL(n.VAL_183, a.VAL_183)
                ,VAL_184 = ISNULL(n.VAL_184, a.VAL_184),VAL_185 = ISNULL(n.VAL_185, a.VAL_185)
                ,VAL_190 = ISNULL(n.VAL_190, a.VAL_190),VAL_191 = ISNULL(n.VAL_191, a.VAL_191)
                ,VAL_192 = ISNULL(n.VAL_192, a.VAL_192),VAL_193 = ISNULL(n.VAL_193, a.VAL_193)
                ,VAL_194 = ISNULL(n.VAL_194, a.VAL_194),VAL_195 = ISNULL(n.VAL_195, a.VAL_195)
                ,VAL_200 = ISNULL(n.VAL_200, a.VAL_200),VAL_201 = ISNULL(n.VAL_201, a.VAL_201)
                ,VAL_202 = ISNULL(n.VAL_202, a.VAL_202),VAL_203 = ISNULL(n.VAL_203, a.VAL_203)
                ,VAL_204 = ISNULL(n.VAL_204, a.VAL_204),VAL_205 = ISNULL(n.VAL_205, a.VAL_205)
                ,VAL_210 = ISNULL(n.VAL_210, a.VAL_210),VAL_211 = ISNULL(n.VAL_211, a.VAL_211)
                ,VAL_212 = ISNULL(n.VAL_212, a.VAL_212),VAL_213 = ISNULL(n.VAL_213, a.VAL_213)
                ,VAL_214 = ISNULL(n.VAL_214, a.VAL_214),VAL_215 = ISNULL(n.VAL_215, a.VAL_215)
                ,VAL_220 = ISNULL(n.VAL_220, a.VAL_220),VAL_225 = ISNULL(n.VAL_225, a.VAL_225)
                ,VAL_226 = ISNULL(n.VAL_226, a.VAL_226),VAL_227 = ISNULL(n.VAL_227, a.VAL_227)'
                
                set @str2 = ',VAL_230 = ISNULL(n.VAL_230, a.VAL_230),VAL_231 = ISNULL(n.VAL_231, a.VAL_231)
                ,VAL_232 = ISNULL(n.VAL_232, a.VAL_232),VAL_235 = ISNULL(n.VAL_235, a.VAL_235)
                ,VAL_236 = ISNULL(n.VAL_236, a.VAL_236),VAL_237 = ISNULL(n.VAL_237, a.VAL_237)
                ,VAL_240 = ISNULL(n.VAL_240, a.VAL_240),VAL_241 = ISNULL(n.VAL_241, a.VAL_241)
                ,VAL_242 = ISNULL(n.VAL_242, a.VAL_242)
                ,VAL_250 = ISNULL(n.VAL_250, a.VAL_250),VAL_251 = ISNULL(n.VAL_251, a.VAL_251)
                ,VAL_252 = ISNULL(n.VAL_252, a.VAL_252),VAL_253 = ISNULL(n.VAL_253, a.VAL_253)
                ,VAL_254 = ISNULL(n.VAL_254, a.VAL_254),VAL_255 = ISNULL(n.VAL_255, a.VAL_255)';
                set @str3 = ' WHEN NOT MATCHED THEN 
    INSERT ([TI_ID],[EventDateTime],[MeterEventDateTime],[DispatchDateTime],[CUS_ID],[Status]
      ,[VAL_100],[VAL_101],[VAL_102],[VAL_103],[VAL_104],[VAL_105],[VAL_110],[VAL_111],[VAL_112],[VAL_113]
      ,[VAL_114],[VAL_115],[VAL_120],[VAL_121],[VAL_122],[VAL_123],[VAL_124],[VAL_125],[VAL_130],[VAL_131]
      ,[VAL_132],[VAL_133],[VAL_134],[VAL_135],[VAL_140],[VAL_141],[VAL_142],[VAL_143],[VAL_144],[VAL_145]
      ,[VAL_150],[VAL_151],[VAL_152],[VAL_153],[VAL_154],[VAL_155],[VAL_160],[VAL_161],[VAL_162],[VAL_163]
      ,[VAL_164],[VAL_165],[VAL_170],[VAL_171],[VAL_172],[VAL_173],[VAL_174],[VAL_175],[VAL_180],[VAL_181]
      ,[VAL_182],[VAL_183],[VAL_184],[VAL_185],[VAL_190],[VAL_191],[VAL_192],[VAL_193],[VAL_194],[VAL_195]
      ,[VAL_200],[VAL_201],[VAL_202],[VAL_203],[VAL_204],[VAL_205],[VAL_210],[VAL_211],[VAL_212],[VAL_213]
      ,[VAL_214],[VAL_215],[VAL_220],[VAL_225],[VAL_226],[VAL_227],[VAL_230],[VAL_231],[VAL_232],[VAL_235]
      ,[VAL_236],[VAL_237],[VAL_240],[VAL_241],[VAL_242],[VAL_250],[VAL_251]
      ,[VAL_252],[VAL_253],[VAL_254],[VAL_255])
    VALUES ([TI_ID],[EventDateTime],[MeterEventDateTime],[DispatchDateTime],[CUS_ID],[Status]
      ,[VAL_100],[VAL_101],[VAL_102],[VAL_103],[VAL_104],[VAL_105],[VAL_110],[VAL_111],[VAL_112],[VAL_113]
      ,[VAL_114],[VAL_115],[VAL_120],[VAL_121],[VAL_122],[VAL_123],[VAL_124],[VAL_125],[VAL_130],[VAL_131]
      ,[VAL_132],[VAL_133],[VAL_134],[VAL_135],[VAL_140],[VAL_141],[VAL_142],[VAL_143],[VAL_144],[VAL_145]
      ,[VAL_150],[VAL_151],[VAL_152],[VAL_153],[VAL_154],[VAL_155],[VAL_160],[VAL_161],[VAL_162],[VAL_163]
      ,[VAL_164],[VAL_165],[VAL_170],[VAL_171],[VAL_172],[VAL_173],[VAL_174],[VAL_175],[VAL_180],[VAL_181]
      ,[VAL_182],[VAL_183],[VAL_184],[VAL_185],[VAL_190],[VAL_191],[VAL_192],[VAL_193],[VAL_194],[VAL_195]
      ,[VAL_200],[VAL_201],[VAL_202],[VAL_203],[VAL_204],[VAL_205],[VAL_210],[VAL_211],[VAL_212],[VAL_213]
      ,[VAL_214],[VAL_215],[VAL_220],[VAL_225],[VAL_226],[VAL_227],[VAL_230],[VAL_231],[VAL_232],[VAL_235]
      ,[VAL_236],[VAL_237],[VAL_240],[VAL_241],[VAL_242],[VAL_250],[VAL_251]
      ,[VAL_252],[VAL_253],[VAL_254],[VAL_255]);';

EXECUTE(N'sp_executesql N''' + @str1 + @str2 + @str3 + '''');

drop table #ArchTechQualityValuesTable;

end
go
   grant EXECUTE on usp2_WriteArchTechQualityValues to [UserMaster61968Service]
go