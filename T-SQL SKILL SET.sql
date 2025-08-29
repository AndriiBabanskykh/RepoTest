--Create schemas Dimension, Stage ETL and Fact with tables accordingly
create schema Stage;
go
create schema Fact;
go
create schema Dimention;
go

--Dimension
create table [Dimention.Date] 
([DateKey] varchar (8) not null
,[Date] date not null
,[MonthName] varchar (10) not null default 'N/A'
,[MonthNumber] int not null default 0
,[Year] int not null default 0
 constraint PK_Date primary key clustered ([DateKey] asc));
go

--Stage
create table [Stage.Sales]  
([ETL_date] datetime
,[Segment] varchar(50)
,[Country] varchar(150)
,[Product] varchar(50) 
,[Discount Band] varchar(50)
,[Units Sold] decimal (10,2)
,[Manufacturing Price] decimal (5,2)
,[Sale Price] decimal (5,2)
,[Discounts] decimal (10,2)
,[COGS] decimal (12,2)
,[Date] date);
go
                               
--Fact
create table [Fact.Sales]
([ETL_date] date
,[SalesID] int identity (1,1) not null
,[Segment] varchar(50) not null default 'N/A' 
,[Country] varchar(150) not null default 'N/A'
,[Product] varchar(50) not null default 'N/A'
,[Discount Band] varchar(50) not null default 'None'
,[Units Sold] decimal (10,2) default 0
,[Manufacturing Price] decimal (5,2)
,[Sale Price] decimal (5,2)
,[Gross Sales] decimal (12,2)
,[Discounts] decimal (10,2)
,[Sales] decimal (12,2)
,[COGS] decimal (12,2)
,[Profit] decimal (12,2)
,[DateKey] varchar (8) not null default 0
 constraint PK_Sales primary key clustered ([SalesID] asc)
 constraint FK_Sales_DateKey foreign key ([DateKey] ) references [Dimention.Date] ([DateKey]))
 go


 --Adding column store index to Fact table because it will be large  table over time
 create columnstore index IX_CS_FactSales
 on [Fact.Sales] ([SalesID], [Segment], [Country], [Product], [Discount Band], [Units Sold], [Manufacturing Price], 
	[Sale Price], [Gross Sales], [Discounts], [Sales], [COGS], [Profit], [DateKey])
go


 --Create a reporting view to be used to connect to Power BI Dashboard
create view [dbo].[SalesByDate]
with schemabinding
as 
select  f.[Segment], f.[Country], f.[Product], f.[Discount Band], f.[Units Sold], f.[Manufacturing Price], f.[Sale Price],
		f.[Gross Sales], f.[Discounts], f.[Sales], f.[COGS], f.[Profit], f.[DateKey], d.[Date], d.[MonthName], d.[MonthNumber], d.[Year]
from [dbo].[Fact.Sales]             f 
inner join [dbo].[Dimention.Date]	d
	on f.[DateKey] = d.[DateKey]
go

-- Create error_log table
create table error_log (
 error_message varchar (max)
,error_datetime  datetime)
go

-- Create storage procedure for load data to Stage table
create procedure [Stage.SP_Sales]
    @start_date date,
    @end_date date
as
begin
    begin try
        -- truncate Stage table
        truncate table [dbo].[Stage.Sales];
        
        -- load data into Stage table
        insert into [dbo].[Stage.Sales] ([ETL_date], [Segment], [Country], [Product], [Discount Band], [Units Sold], 
										 [Manufacturing Price], [Sale Price], [Discounts], [COGS], [Date])

        select getdate() as [ETL_date], [Segment], [Country], [Product], [Discount Band], [Units Sold], 
			   [Manufacturing Price], [Sale Price], [Discounts], [COGS], [Date]
        from [dbo].[DataSource]
		where [Date] between @start_date and @end_date;
    end try
    begin catch
        insert into error_log (error_message, error_datetime)
        values (error_message(), getdate());
    end catch
end
go


-- Create storage procedure for load data to Fact table
create procedure [Fact.SP_Sales]
as
begin
    begin try
        -- delete from Fact table data for the same date
        delete from [dbo].[Fact.Sales]
		where [ETL_date] = (select distinct cast([ETL_date] as date) from [dbo].[Stage.Sales]);
        
        -- load data into Fact table
        insert into [dbo].[Fact.Sales] ([ETL_date], [Segment], [Country], [Product], [Discount Band], [Units Sold], 
										 [Manufacturing Price], [Sale Price], [Gross Sales], [Discounts], [Sales], [COGS], [Profit], [DateKey])

        select getdate() as [ETL_date], [Segment], [Country], [Product], [Discount Band], [Units Sold], 
			   [Manufacturing Price], [Sale Price], [Gross Sales] = [Units Sold] * [Sale Price], [Discounts], [Sales] = ([Units Sold] * [Sale Price]) - [Discounts],
			   [COGS], [Profit] = ([Units Sold] * [Sale Price]) - [Discounts] - [COGS], [DateKey] = CONVERT(varchar(8), date, 112)
        from [dbo].[Stage.Sales]
    end try
    begin catch
        insert into error_log (error_message, error_datetime)
        values (error_message(), getdate());
    end catch
end
go

-- Create storage procedure for load and update Dimention table
create procedure [Dimention.SP_Date]
as
begin
    begin try
		insert into [Dimention.Date]  ([DateKey], [Date])
		select distinct convert(varchar(8), [Date], 112) as [DateKey], [Date] from [dbo].[Stage.Sales]
		except
		select [DateKey], [Date] from [Dimention.Date];

		update [Dimention.Date] set
		 [MonthName]  = datename(month, [Date])
		,[MonthNumber] = month([Date])
		,[Year] = year([Date])
	end try
    begin catch
        insert into error_log (error_message, error_datetime)
        values (error_message(), getdate());
    end catch
end
go




Steps for create SQL Server Job and define parameters:
1. Open SQL Server Management Studio and connect to your SQL Server instance.
2. In the Object Explorer, expand the SQL Server Agent node and right-click on the Jobs folder. Select "New Job" from the context menu.
3. In the "New Job" dialog box, enter a name for the job and optionally provide a description. Configure any other job properties as needed.
4. Click on the "Steps" tab and click the "New" button to create a new job step.
5. In the "New Job Step" dialog box, enter a name for the step and select "Transact-SQL script (T-SQL)" as the type.
6. In the "Command" text box, enter the T-SQL code below:

declare @start_date date, @end_date date

set @start_date = convert(date, dateadd(day, -1, getdate())) --can be changed depend on
 needed period
set @end_date = convert(date, getdate())

exec [Stage.SP_Sales]  @start_date = @start_date,  @end_date = @end_date
go
exec [Dimention.SP_Date]
go
exec [Fact.SP_Sales]
go

7. Click the "OK" button to save the job step.
8. Optionally, configure any other job steps or schedules as needed.
9. Click the "OK" button to save the job.



