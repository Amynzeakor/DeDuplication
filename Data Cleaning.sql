
use TrimcoRetail
go
ALTER Procedure SpPurchaseTransDeDup
		AS
BEGIN
					--Made used of global temp table for easy collaboration
		SET NOCOUNT ON  
			BEGIN TRY  ---this is to monitor for errors and capture errors if any exist

				BEGIN TRANSACTION -- this allows for easy rollback of data if error is encountered
				-----Data Cleaning and Standadization

				-----Created a Global TempTable to work on inother not to make direct immediate changes on the DB ,
				---- Modified the customer Address table;this temptable worked as my main table in this project.
IF OBJECT_ID('tempdb..##PurchaseTrans') is not null
			Drop table ##PurchaseTrans

				SELECT  TransID,OrderID,Supplier,AccountNumber,concat_ws(' ',Address,City,',',StateProvince) Address,
						Country,OrderDate,DueDate,getdate() Loaddate
					INTO	##PurchaseTrans
						FROM	OLTP.Purchasetrans
						--- checking for duplicates data in the dataset ##purchaseTrans
							SELECT  OrderID, COUNT(OrderID) as Count,Supplier,Address,Country,AccountNumber,OrderDate,DueDate
								FROM	##PurchaseTrans
									GROUP BY OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate
;
				--Extracting the Duplicates Data
IF OBJECT_ID('tempdb..##PurchaseDuplicate ') is not null
				DROP table ##PurchaseDuplicate 
					SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate
						INTO  ##PurchaseDuplicate 
							FROM  ##PurchaseTrans
								WHERE OrderID in
									(		select orderID from ##PurchaseTrans
											group by orderID
											having count(*) >1
									) 
;
				----handling duplicate data inline with business requirement;in this case the last record of the duplicate data is the accurate record to keep.
IF OBJECT_ID('tempdb..##AccurateRecord' ) is not null
				drop table ##AccurateRecord 
					SELECT max(TransID) as TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate
						into ##AccurateRecord from ##PurchaseDuplicate
							GROUP BY OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate
;
				---Used Union all to join the table of the Non Duplicate dataset and the De-Dupliate dataset,select into the Valid TransRecord
IF OBJECT_ID('tempdb..##ValidTransRecord' ) is not null
			DROP table ##ValidTransRecord 
				SELECT A.TransID,A.OrderID,A.Supplier,A.Address,A.Country,A.AccountNumber,A.OrderDate,A.Loaddate,A.DueDate into ##ValidTransRecord 
					from 
						(
									SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate from ##AccurateRecord
						Union all
									SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate from ##PurchaseDuplicate 
									Where OrderID in (select OrderID from  ##PurchaseDuplicate 
												GROUP BY OrderID
												HAVING COUNT(*)=1
												)

							) A
				---- Created a table with same structure with the original table in the database to store the duplicates data that comes out from the main table

					SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate,GETDATE() LogDate
							INTO Audit.PurchaseTransDuplicate 
									FROM OLTP.Purchasetrans
										WHERE 1=0
;
					---- Removing Duplicate Data from the main table oltp.PurchaseTrans and storing into a table PurchaseTransDuplicate fro Audit purposes

					INSERT INTO Audit.PurchaseTransDuplicate (TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate,LogDate)
							SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate,GETDATE()
								FROM ##PurchaseDuplicate
	
					---- created a gblobal temp table to store the Data being delete from the Main Table
IF OBJECT_ID('tempdb..##DeletedRecordSet') is not null
				 DROP table ##DeletedRecordSet
						SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate
							INTO ##DeletedRecordSet
								FROM OLTP.Purchasetrans
									WHERE TransID not in (SELECT TransID from ##ValidTransRecord )

					----Deleting Duplicates from the main table
				DELETE From OLTP.Purchasetrans
						Where TransID not in (SELECT TransID FROM ##ValidTransRecord)



