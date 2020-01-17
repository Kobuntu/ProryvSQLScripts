if exists (select 1
          from sysobjects
          where  id = object_id('[usp2_Help_Documents_FTS_Search]')
          and type in ('P','PC'))
   drop procedure usp2_Help_Documents_FTS_Search
go


SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp2_Help_Documents_FTS_Search]
    -- Add the parameters for the stored procedure here
    @ApplicationVersion int,
    @SearchQuery nvarchar(100)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;
    
SET @SearchQuery = ltrim(rtrim(@SearchQuery));
SET @SearchQuery = replace(replace(@SearchQuery, '  ', ' '), ' ', ' and ');
SET @SearchQuery = ''+@SearchQuery + '';
 
SELECT   MAX([Uri]) as [Uri]
            , [DocumentTitle] 
     
            
 
FROM
    (SELECT  [Uri]
             ,[DocumentTitle] = [DocumentData].value('(/node()/node()/node())[1]', 'varchar(max)')
                 
     FROM
         [dbo].[Help_Documents]
 
     WHERE
         --ищем только xaml и для нашего приложения
         Uri LIKE  '%.xaml'
         --исключим index файлы
         AND Uri NOT LIKE '%index%'
         -- Сам поиск FTS
         AND (contains(DocumentData, @SearchQuery))) AS td
 
GROUP BY
    -- Исключим совпадения разделов
  DocumentTitle
END
GO

grant EXECUTE on usp2_Help_Documents_FTS_Search to [UserCalcService]
go
