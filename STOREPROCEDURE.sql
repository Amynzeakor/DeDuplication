
use TrimcoRetail
go
CREATE Procedure SpPurchaseTransDeDup
		AS
BEGIN
		SET NOCOUNT ON  
			BEGIN TRY  ---this is to monitor for errors and capture errors if any exist

				BEGIN TRANSACTION -- this allows for easy rollback of data if error is encountered
		
IF OBJECT_ID('tempdb..##PurchaseTrans') is not null
			Drop table ##PurchaseTrans

				SELECT  TransID,OrderID,Supplier,AccountNumber,concat_ws(' ',Address,City,',',StateProvince) Address,
						Country,OrderDate,DueDate,getdate() Loaddate
					INTO	##PurchaseTrans
						FROM	OLTP.Purchasetrans
							SELECT  OrderID, COUNT(OrderID) as Count,Supplier,Address,Country,AccountNumber,OrderDate,DueDate
								FROM	##PurchaseTrans
									GROUP BY OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate
;
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
IF OBJECT_ID('tempdb..##AccurateRecord' ) is not null
				drop table ##AccurateRecord 
					SELECT max(TransID) as TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate
						into ##AccurateRecord from ##PurchaseDuplicate
							GROUP BY OrderID,Supplier,Address,Country,AccountNumber,OrderDate,loadDate,DueDate
;
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

					INSERT INTO Audit.PurchaseTransDuplicate (TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate,LogDate)
							SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate,GETDATE()
								FROM ##PurchaseDuplicate
	
IF OBJECT_ID('tempdb..##DeletedRecordSet') is not null
				 DROP table ##DeletedRecordSet
						SELECT TransID,OrderID,Supplier,Address,Country,AccountNumber,OrderDate,DueDate,loadDate
							INTO ##DeletedRecordSet
								FROM OLTP.Purchasetrans
									WHERE TransID not in (SELECT TransID from ##ValidTransRecord )

				DELETE From OLTP.Purchasetrans
						Where TransID not in (SELECT TransID FROM ##ValidTransRecord)

					---commit is a command that completes the transaction to execute all the Queries in between

		COMMIT;
		--- THE PROCEDURE BELOW ENABLES ME UNDERSTAND AND CONTROL THE IMPACT OF ANY ERRORS THAT WILL BE ENCOUNTERED.
			END TRY

		BEGIN CATCH
			IF @@ERROR > 0
				ROLLBACK;

		DECLARE @REDALERT NVARCHAR(4000)=ERROR_MESSAGE()
			SELECT @REDALERT
				PRINT @REDALERT;
					THROW 50000,'THERE IS AN ERROR IN SPPURCHASETRANSDEDUP',1;
END CATCH;
END;



EXEC  SPPURCHASETRANSDEDUP
	
