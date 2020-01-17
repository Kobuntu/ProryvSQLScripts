set ANSI_NULLS ON
set QUOTED_IDENTIFIER ON
go
if exists (select 1
          from sysobjects
          where  id = object_id('usf2_JournalReplace_EventParamToString')
          and type in ('IF', 'FN', 'TF'))
   drop function usf2_JournalReplace_EventParamToString
go

-- ======================================================================================
-- Автор:
--
--		Малышев Игорь
--
-- Дата создания:
--
--		Сентябрь, 2018
--
-- Описание:
--
--		Конвертируем код замещения в человеческое представление, паралельно меняем в enumEventParam
--
-- ======================================================================================
create FUNCTION [dbo].[usf2_JournalReplace_EventParamToString]
(	
	@EventParam tinyint
)
RETURNS varchar(1000)
AS
begin

if (@EventParam = 0) return 'Не определено';

if (@EventParam = 1) return 'Ручной ввод';
        
if (@EventParam = 2) return 'Замещены данными КА';

if (@EventParam = 3) return 'Возврат к измеренным значениям';

if (@EventParam = 4) return 'Заполнение 0 на период';

if (@EventParam = 5) return 'Распределение значения по получасовкам';

if (@EventParam = 6) return 'Заполнение получасовок одинаковым значением';

if (@EventParam = 7) return 'Заполнение данными другой ТИ';

if (@EventParam = 8) return 'Балансовый метод замещения';

if (@EventParam = 9) return 'Замещение по акту недоучета';

if (@EventParam = 10) return 'Заполнение данными предыдущих суток';

if (@EventParam = 11) return 'Ручной ввод показаний ПУ';

if (@EventParam = 12) return 'На основе интегральных показаний, используя часовые профили других ПУ из сечения этой точки';

if (@EventParam = 13) return 'На основе основного часового профиля контрольного ПУ';

if (@EventParam = 15) return 'На основе интегральных показаний контрольного ПУ, используя показания самой ТИ за аналогичный период прошлого года';

if (@EventParam = 16) return 'На основе интегральных показаний контрольного ПУ, данные о профиле отсутствуют 3 и более месяца';

if (@EventParam = 32) return 'На основе профиля аналогичного периода прошлого года. При отсутствии контрольного ПУ.';
        
if (@EventParam = 64) return 'При отсутствии контрольного ПУ и отсутствии данных о профиле 3 и более месяцев. На основе максимальной мощности';
        
if (@EventParam = 128) return 'Распределить 0';
        
if (@EventParam = 129) return 'Основные данные признаны недостоверными';
        
if (@EventParam = 130) return 'Основные данные профиля и интегралов удалены';
        
if (@EventParam = 131) return 'Смена источника данных';
        
if (@EventParam = 132) return 'На основе графика интегральных показаний';
        
if (@EventParam = 133) return 'Данные признаны достоверными';
        
if (@EventParam = 134) return 'Основные данные профиля удалены';
        
if (@EventParam = 135) return 'Основные данные интегральные удалены';
        
if (@EventParam = 136) return 'Удаление записи по акту недоучета';
        
if (@EventParam = 137) return 'Ввод данных по малой ТИ';
        
if (@EventParam = 138) return 'Удаление данных по малой ТИ';

return '';
end
go
grant EXECUTE on usf2_JournalReplace_EventParamToString to [UserCalcService]
go