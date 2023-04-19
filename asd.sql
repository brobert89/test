ALTER PROCEDURE [dbo].[spLoad_FACT_CustomerSegmentation] 
AS
BEGIN
INSERT INTO Logging.dbo.ProcedureExecutionLog (ObjectName,StartDate,UserName) VALUES (DB_NAME() + '.' + OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID), GETDATE(),SUSER_SNAME())
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET NOCOUNT ON;
SET XACT_ABORT ON;


----PENTRU DELTA , TOATE ORDERELE ,NU SE EXCLUDE NIMIC


IF OBJECT_ID('tempdb..#ORDERS_DELTA') is not null DROP TABLE  #ORDERS_DELTA
SELECT DISTINCT OrderId into #ORDERS_DELTA FROM Reporting_S.dbo.FACT_OrderHeader a (NOLOCK) WHERE CountryId = PlatformId and a.OrderDate>='2016-01-01' 
--AND cast(a.ModifiedDate as date)>=cast(getdate()-5 as date)

CREATE NONCLUSTERED INDEX NCL_IDX_ORD1 ON #ORDERS_DELTA (OrderId)

DROP TABLE IF EXISTS #ORDER_RETURNS
SELECT OrderId,b.SK_Order SK_order, b.LastQuantityOrdered, PlatformId, CountryId,b.VendorId ,b.PricePerUnit, b.CommissionNetValue,b.CommissionTargetValue,OrderDate,b.Deleted, b.ProductStatus, b.Currency  INTO #ORDER_RETURNS FROM Reporting_S.dbo.FACT_OrderReturns_RO b WHERE b.CountryId = b.PlatformId  AND OrderDate>='2016-01-01' AND b.Deleted= 0 and b.OrderId is not null
UNION ALL
SELECT OrderId,b.SK_Order SK_order, b.LastQuantityOrdered, PlatformId, CountryId,b.VendorId ,b.PricePerUnit, b.CommissionNetValue,b.CommissionTargetValue,OrderDate,b.Deleted, b.ProductStatus, b.Currency     FROM Reporting_S.dbo.FACT_OrderReturns_BG b WHERE b.CountryId = b.PlatformId  AND OrderDate>='2016-01-01' AND b.Deleted= 0 and b.OrderId is not null
UNION ALL
SELECT OrderId,b.SK_Order SK_order, b.LastQuantityOrdered, PlatformId, CountryId,b.VendorId ,b.PricePerUnit, b.CommissionNetValue,b.CommissionTargetValue,OrderDate,b.Deleted, b.ProductStatus, b.Currency    FROM Reporting_S.dbo.FACT_OrderReturns_HU b WHERE b.CountryId = b.PlatformId  AND OrderDate>='2016-01-01' AND b.Deleted= 0 and b.OrderId is not null


INSERT INTO #ORDERS_DELTA
SELECT DISTINCT b.OrderId from #ORDER_RETURNS as b With(NOLOCK) 
LEFT JOIN #ORDERS_DELTA a on b.OrderId = a.OrderId WHERE b.CountryId = b.PlatformId  AND OrderDate>='2016-01-01' --and cast(b.ModifiedDate as date)>=cast(getdate()-5 as date)
and a.OrderId is null --and b.Deleted= 0 
and b.OrderId is not null

INSERT INTO #ORDERS_DELTA
SELECT DISTINCT a.OrderId  FROM Reporting_A.dbo.FACT_Sales_W_Discounts a (NOLOCK) --DataWarehouse..Sales a (NOLOCK) 
LEFT JOIN #ORDERS_DELTA b on b.OrderId = a.OrderId WHERE a.Deleted = 0 AND CountryId = PlatformId AND b.OrderId is null
AND a.OrderId is not null --AND cast(a.ModifiedDate as date)>=cast(getdate()-5 as date)
and a.SalesPostingDate >='2016-01-01' --and Corporate='Nu'

DROP TABLE IF EXISTS #RELEVANT_ORDER 
SELECT OrderId, RelevantOrder INTO #RELEVANT_ORDER FROM Reporting_S.dbo.FACT_Orders_RO a WHERE a.OrderDate>='2016-01-01' and a.RelevantOrder=1 GROUP BY OrderId, RelevantOrder
UNION ALL
SELECT OrderId, RelevantOrder FROM Reporting_S.dbo.FACT_Orders_BG a WHERE a.OrderDate>='2016-01-01' and a.RelevantOrder=1 GROUP BY OrderId, RelevantOrder
UNION ALL
SELECT OrderId, RelevantOrder FROM Reporting_S.dbo.FACT_Orders_HU a WHERE a.OrderDate>='2016-01-01' and a.RelevantOrder=1 GROUP BY OrderId, RelevantOrder


IF OBJECT_ID('tempdb..#DATE_ORDER_HEADER') is not null DROP TABLE  #DATE_ORDER_HEADER
SELECT DISTINCT  a.OrderId , a.CountryId , a.PlatformId ,IsNull(EOS_CustomerId, 0)	 customers_id_assoc , c.RelevantOrder , a.OrderDate into #DATE_ORDER_HEADER 
FROM Reporting_S.dbo.FACT_OrderHeader a (NOLOCK)
INNER JOIN #ORDERS_DELTA b on a.OrderId  = b.OrderId
LEFT JOIN #RELEVANT_ORDER c (NOLOCK) on a.OrderId= c.OrderId 
WHERE 
 a.OrderDate>='2016-01-01' and c.RelevantOrder=1


--GP3 = Sales – COGS + BR&BSM + Consumer Credit fees GP –  Packaging Costs  - Bank Charges - Shipping Costs  + Shipping Revenue + MKTP Final Commission

DROP TABLE IF EXISTS #STORNO_ORDER 
SELECT SK_Order SK_order, OrderLine IDProdCom, OrderId, PlatformId, CountryId INTO #STORNO_ORDER FROM Reporting_S.dbo.FACT_Orders_RO a WHERE a.OrderDate>='2016-01-01' 
UNION ALL
SELECT SK_Order SK_order, OrderLine IDProdCom, OrderId, PlatformId, CountryId FROM Reporting_S.dbo.FACT_Orders_BG a WHERE a.OrderDate>='2016-01-01' 
UNION ALL
SELECT SK_Order SK_order, OrderLine IDProdCom, OrderId, PlatformId, CountryId FROM Reporting_S.dbo.FACT_Orders_HU a WHERE a.OrderDate>='2016-01-01' 


DROP TABLE IF EXISTS #EIS_INVOICES
SELECT OrderLine id_prod_com, Quantity cantitate, DispInvoiceId id_disp_fact, OrderId id_comanda
INTO #EIS_INVOICES
FROM Reporting_S.dbo.FACT_EIS_Invoices
GROUP BY OrderLine, Quantity,DispInvoiceId, OrderId



-- STORNARI
		IF OBJECT_ID('tempdb..#MG_STORNO_EMAG') is not null DROP TABLE  #MG_STORNO_EMAG
		SELECT d.SK_order, c.id_prod_com, SUM(c.cantitate) as cantitate
		INTO #MG_STORNO_EMAG
		FROM #EIS_INVOICES c with (NOLOCK)
		INNER JOIN #STORNO_ORDER as d with (NOLOCK) on c.id_prod_com = d.IDProdCom AND d.OrderId = c.id_comanda
	--	INNER JOIN StageData.emg.cmd_comenzi_fact f WITH (NOLOCK) on c.id_disp_fact = f.id_disp_fact and d.OrderId = f.id_comanda
		INNER JOIN #ORDERS_DELTA OD ON OD.OrderId = d.OrderId
		WHERE  c.cantitate < 0 and d.PlatformId = CountryId 
		GROUP BY id_prod_com , d.SK_order


		CREATE NONCLUSTERED INDEX ncl_idx_1 ON #MG_STORNO_EMAG ( SK_order )
	
		IF OBJECT_ID('tempdb..#MG_RETURNS_VENDORS') is not null DROP TABLE  #MG_RETURNS_VENDORS
		SELECT b.SK_order, SUM(IsNull(b.LastQuantityOrdered,0)) as LastQuantityOrdered , PlatformId, CountryId,
			sum(iif(b.VendorId > 1, b.LastQuantityOrdered*b.PricePerUnit,0)*isnull(ccy.ExchangeRate,1))			as Storno_OV_3P,
		sum(iif(b.VendorId > 1, b.LastQuantityOrdered,0)*isnull(ccy.ExchangeRate,1))								as Storno_Quantity_3P,
		isnull(sum(b.CommissionNetValue*isnull(ccy.ExchangeRate,1)),0) - isnull(sum(b.CommissionTargetValue*isnull(ccy.ExchangeRate,1)),0)	as Storno_CommissionMKTP
		INTO #MG_RETURNS_VENDORS FROM #ORDER_RETURNS as b With(NOLOCK) 
		INNER JOIN  #ORDERS_DELTA OD ON OD.OrderId = b.OrderId
		LEFT JOIN Reporting_A.dbo.DIM_CurrencyRates  ccy on ccy.Date = b.OrderDate and b.Currency = ccy.FromCurrency and ToCurrency ='RON' 
		WHERE b.Deleted = 0 AND b.ProductStatus = 1 and b.PlatformId = b.CountryId and b.Deleted =0 
		GROUP BY b.SK_order, PlatformId, CountryId; 

		CREATE NONCLUSTERED INDEX ncl_idx_2 ON #MG_RETURNS_VENDORS ( SK_order )

		IF OBJECT_ID('tempdb..#MG_OU_R') is not null DROP TABLE  #MG_OU_R

		SELECT
		a.SK_Order SK_order, 
		a.SK_Product SK_produs,
		a.OrderId, 
		a.VendorId,
		OH.OrderDate, 
		a.LastQuantityOrdered + IsNull(b.cantitate,0)+  IsNull(C.LastQuantityOrdered,0) as LastQuantityOrdered,  
		C.Storno_OV_3P,
		C.Storno_Quantity_3P ,
		C.Storno_CommissionMKTP ,
		a.PricePerUnit*isnull(ccy.ExchangeRate,1) PricePerUnit, 
		oed.NPS,
		a.OrderSource, 
        a.CommissionNetValue*isnull(ccy.ExchangeRate,1) Valoare_Comision,													
		a.CommissionTargetValue*isnull(ccy.ExchangeRate,1) Comision_Target,													
		a.ModifiedDate,
		a.CountryId,
		a.PlatformId
		,OH.customers_id_assoc , 
		OH.RelevantOrder
		INTO #MG_OU_R
		FROM Reporting_S.dbo.FACT_Orders_RO AS a WITH (NOLOCK)  
		INNER JOIN #DATE_ORDER_HEADER OH ON OH.OrderId = a.OrderId
		LEFT JOIN #MG_STORNO_EMAG AS b  on a.SK_Order = b.SK_order				
		LEFT JOIN #MG_RETURNS_VENDORS C ON a.SK_Order= -C.SK_order
		LEFT JOIN Reporting_A.dbo.DIM_CurrencyRates  ccy on ccy.Date = a.OrderDate and a.Currency = ccy.FromCurrency and ToCurrency ='RON' 
		LEFT JOIN Reporting_S.dbo.FACT_Orders_ExtraDetails_RO AS oed WITH (NOLOCK)  On a.SK_Order=oed.SK_Order
		WHERE a.RelevantOrder = 1 and a.PlatformId=a.CountryId and a.ProductStatus = 1 and CorporateOrder = 0 and a.Deleted=0 and 
	(	(EmagOrderStatus = 12 AND VendorId = 1) OR (VendorId <> 1 AND VendorOrderStatus in (4,5))); 


		---pana aici a rulat 
		INSERT  INTO #MG_OU_R
		SELECT
		a.SK_Order SK_order, 
		a.SK_Product SK_produs,
		a.OrderId, 
		a.VendorId,
		OH.OrderDate, 
		a.LastQuantityOrdered + IsNull(b.cantitate,0)+  IsNull(C.LastQuantityOrdered,0) as LastQuantityOrdered,  
		C.Storno_OV_3P,
		C.Storno_Quantity_3P ,
		C.Storno_CommissionMKTP ,
		a.PricePerUnit*isnull(ccy.ExchangeRate,1) PricePerUnit, 
		oed.NPS,
		a.OrderSource, 
        a.CommissionNetValue*isnull(ccy.ExchangeRate,1) Valoare_Comision,													
		a.CommissionTargetValue*isnull(ccy.ExchangeRate,1) Comision_Target,													
		a.ModifiedDate,
		a.CountryId,
		a.PlatformId
		,OH.customers_id_assoc , 
		OH.RelevantOrder
		FROM Reporting_S.dbo.FACT_Orders_BG AS a WITH (NOLOCK)  
		INNER JOIN #DATE_ORDER_HEADER OH ON OH.OrderId = a.OrderId
		LEFT JOIN #MG_STORNO_EMAG AS b  on a.SK_Order = b.SK_order				
		LEFT JOIN #MG_RETURNS_VENDORS C ON a.SK_Order= -C.SK_order
		LEFT JOIN Reporting_A.dbo.DIM_CurrencyRates  ccy on ccy.Date = a.OrderDate and a.Currency = ccy.FromCurrency and ToCurrency ='RON' 
		LEFT JOIN Reporting_S.dbo.FACT_Orders_ExtraDetails_BG AS oed WITH (NOLOCK)  On a.SK_Order=oed.SK_Order
		WHERE a.RelevantOrder = 1 and a.PlatformId=a.CountryId and a.ProductStatus = 1 and CorporateOrder = 0 and a.Deleted=0 and 
	(	(EmagOrderStatus = 12 AND VendorId = 1) OR (VendorId <> 1 AND VendorOrderStatus in (4,5))); 

			INSERT  INTO #MG_OU_R
		SELECT
		a.SK_Order SK_order, 
		a.SK_Product SK_produs,
		a.OrderId, 
		a.VendorId,
		OH.OrderDate, 
		a.LastQuantityOrdered + IsNull(b.cantitate,0)+  IsNull(C.LastQuantityOrdered,0) as LastQuantityOrdered,  
		C.Storno_OV_3P,
		C.Storno_Quantity_3P ,
		C.Storno_CommissionMKTP ,
		a.PricePerUnit*isnull(ccy.ExchangeRate,1) PricePerUnit, 
		oed.NPS,
		a.OrderSource, 
        a.CommissionNetValue*isnull(ccy.ExchangeRate,1) Valoare_Comision,													
		a.CommissionTargetValue*isnull(ccy.ExchangeRate,1) Comision_Target,													
		a.ModifiedDate,
		a.CountryId,
		a.PlatformId
		,OH.customers_id_assoc , 
		OH.RelevantOrder
		FROM Reporting_S.dbo.FACT_Orders_HU AS a WITH (NOLOCK)  
		INNER JOIN #DATE_ORDER_HEADER OH ON OH.OrderId = a.OrderId
		LEFT JOIN #MG_STORNO_EMAG AS b  on a.SK_Order = b.SK_order				
		LEFT JOIN #MG_RETURNS_VENDORS C ON a.SK_Order= -C.SK_order
		LEFT JOIN Reporting_A.dbo.DIM_CurrencyRates  ccy on ccy.Date = a.OrderDate and a.Currency = ccy.FromCurrency and ToCurrency ='RON' 
		LEFT JOIN Reporting_S.dbo.FACT_Orders_ExtraDetails_HU AS oed WITH (NOLOCK)  On a.SK_Order=oed.SK_Order
		WHERE a.RelevantOrder = 1 and a.PlatformId=a.CountryId and a.ProductStatus = 1 and CorporateOrder = 0 and a.Deleted=0 and 
	(	(EmagOrderStatus = 12 AND VendorId = 1) OR (VendorId <> 1 AND VendorOrderStatus in (4,5))); 

		IF OBJECT_ID('tempdb..#TMP_BSM_CORRECTION') IS NOT NULL DROP TABLE #TMP_BSM_CORRECTION
		SELECT DISTINCT sto.SK_Sales SK_sales 
		INTO #TMP_BSM_CORRECTION
		FROM Reporting_A.dbo.FACT_BSM_CancelledAmounts sto WITH (NOLOCK);
 

----#ELECTRONICWASTE

		IF OBJECT_ID('tempdb..#ELECTRONICWASTE') is not null DROP TABLE  #ELECTRONICWASTE
		SELECT  a.SK_sales ,SUM(ISNULL(TaxForElectronicWaste,0)) AS [Tax For Electronic Waste]
		INTO #ELECTRONICWASTE FROM [Reporting_P].[dbo].[FACT_PL_Costs_split] a WITH (NOLOCK)   -- select top 10 * from [Reporting_H].dbo.[PL_Costs_split] 
		INNER JOIN Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_sales= s.SK_Sales --DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_sales = s.SK_sales
		WHERE 1=1 AND s.SalesPostingDate>='2016-01-01' AND s.Deleted=0 AND s.PlatformId = s.CountryId  
		GROUP BY a.SK_sales		
		/* tax for electronic waste */



		
----#BANKCHARGES

		IF OBJECT_ID('tempdb..#BANKCHARGES_RO') is not null DROP TABLE  #BANKCHARGES_RO
		select   a.SK_Sales SK_sales,SUM(ISNULL(a.FinalBankCharge,0)) Bank_Charges_Value
		into #BANKCHARGES_RO
		from Reporting_E.[dbo].[FACT_BankChargesConsolidated] a WITH (NOLOCK)
		where CountryId = 1 and OnlineFlag in (0,1) and PlatformId = 1 	and SK_Sales is not null AND Deleted = 0
		GROUP BY a.SK_Sales

		IF OBJECT_ID('tempdb..#BANKCHARGES_BG') is not null DROP TABLE  #BANKCHARGES_BG
		select   a.SK_Sales SK_sales,SUM(ISNULL(a.FinalBankCharge,0)) Bank_Charges_Value
		into #BANKCHARGES_BG
		from Reporting_E.[dbo].[FACT_BankChargesConsolidated] a WITH (NOLOCK)
		where CountryId = 2 and OnlineFlag in (0,1) and PlatformId = 2 	and SK_Sales is not null AND Deleted = 0
		GROUP BY a.SK_Sales

		IF OBJECT_ID('tempdb..#BANKCHARGES_HU') is not null DROP TABLE  #BANKCHARGES_HU
		select   a.SK_Sales SK_sales,SUM(ISNULL(a.FinalBankCharge,0)) Bank_Charges_Value
		into #BANKCHARGES_HU
		from Reporting_E.[dbo].[FACT_BankChargesConsolidated] a WITH (NOLOCK)
		where CountryId = 3 and OnlineFlag in (0,1) and PlatformId = 3 	and SK_Sales is not null AND Deleted = 0
		GROUP BY a.SK_Sales

			
		IF OBJECT_ID('tempdb..#BANKCHARGES_BGHU_LEGACY') is not null DROP TABLE  #BANKCHARGES_BGHU_LEGACY		
		
		SELECT A.SK_Sales ,sum(Bank_Charges_Value) Bank_Charges_Value  INTO #BANKCHARGES_BGHU_LEGACY FROM (
		SELECT
		a.SK_Sales
		,SUM(ISNULL(BankCharge,0)) AS Bank_Charges_Value
		FROM Reporting_A.dbo.FACT_BankCharges_1P_Mixed_Legacy a WITH (NOLOCK) -- DataWarehouse.dbo.Bank_Charges_1P_Mixt a WITH (NOLOCK)
		INNER JOIN  Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_Sales= s.SK_Sales  -- DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_sales=s.SK_sales
		WHERE 1=1 AND s.Deleted=0  AND s.CountryId IN (2,3) and s.PlatformId in (2,3) AND BankCharge <>0
		GROUP BY a.SK_Sales
		UNION 
		SELECT
		a.SK_Sales
		,SUM(ISNULL(BankCharge,0)) AS Bank_Charges_Value
		FROM Reporting_A.dbo.FACT_BankCharges_1P_Legacy a WITH (NOLOCK) -- DataWarehouse.dbo.Bank_Charges_1P a WITH (NOLOCK)
		INNER JOIN  Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_Sales= s.SK_Sales --DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_sales=s.SK_sales
		WHERE 1=1 AND s.Deleted=0 AND s.CountryId IN (2,3) and s.PlatformId in (2,3) AND BankCharge <>0
		GROUP BY a.SK_Sales
		) A
		GROUP BY A.SK_Sales 

		IF OBJECT_ID('tempdb..#BANKCHARGES') is not null DROP TABLE  #BANKCHARGES
		SELECT SK_sales, Bank_Charges_Value into #BANKCHARGES FROM #BANKCHARGES_RO 
		union all
		SELECT SK_sales, Bank_Charges_Value FROM #BANKCHARGES_BG 
		union all
		SELECT SK_sales, Bank_Charges_Value FROM #BANKCHARGES_HU 
		union all
		SELECT SK_Sales,Bank_Charges_Value FROM #BANKCHARGES_BGHU_LEGACY

		
----#PACKAGINGTOTALVALUE

		IF OBJECT_ID('tempdb..#PACKAGINGTOTALVALUE') is not null DROP TABLE  #PACKAGINGTOTALVALUE
		SELECT a.SK_Sales as SK_sales , SUM(PackagingTotalValue) PackagingTotalValue into #PACKAGINGTOTALVALUE  
		FROM Reporting_P.dbo.FACT_OtherCosts a with (NOLOCK) -- DataWarehouse.dbo.OtherCosts a (NOLOCK)
		INNER JOIN  Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_Sales= s.SK_Sales--DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_sales=s.SK_sales
		GROUP BY a.SK_Sales

----#OTHERCOMMISSIONSREVENUE

	    IF OBJECT_ID('tempdb..#OTHERCOMMISSIONSREVENUE') is not null DROP TABLE  #OTHERCOMMISSIONSREVENUE	
		SELECT a.SK_Sales AS SK_sales
		,SUM(a.CommissionLineValue) AS CommisionLineValueRevenue
		INTO #OTHERCOMMISSIONSREVENUE
		FROM Reporting_A.[dbo].[FACT_OtherCommissions] a WITH (NOLOCK)
		INNER JOIN  Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_Sales= s.SK_Sales--DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_Sales=s.SK_sales
		WHERE 1=1  AND a.RevenueFlag=1
		GROUP BY a.SK_Sales


----#OTHERCOMMISSIONSCOST

		IF OBJECT_ID('tempdb..#OTHERCOMMISSIONSCOST') is not null DROP TABLE  #OTHERCOMMISSIONSCOST
		SELECT a.SK_Sales AS SK_sales
		,SUM(a.CommissionLineValue) AS CommisionLineValueCost
		INTO #OTHERCOMMISSIONSCOST
		FROM Reporting_A.[dbo].[FACT_OtherCommissions] a WITH (NOLOCK)
		INNER JOIN  Reporting_A.dbo.FACT_Sales_W_Discounts s (NOLOCK) on a.SK_Sales= s.SK_Sales --DataWarehouse.dbo.Sales s WITH (NOLOCK) ON a.SK_Sales=s.SK_sales
		WHERE 1=1   AND a.RevenueFlag=0
		GROUP BY a.SK_Sales 

				
---#CUSTOMERSHIPPINGREVENUE

		IF OBJECT_ID('tempdb..#CUSTOMERSHIPPINGREVENUE') is not null DROP TABLE  #CUSTOMERSHIPPINGREVENUE
		SELECT a.SK_Sales SK_sales , SUM(COALESCE(sd.CustomerShippingRevenue,0)) AS CustomerShippingRevenue  INTO #CUSTOMERSHIPPINGREVENUE
		FROM Reporting_A.dbo.FACT_Sales_W_Discounts a WITH (NOLOCK) 
		INNER JOIN Reporting_A.dbo.FACT_Sales_Details  sd (NOLOCK) ON a.SK_Sales = sd.SK_Sales
		WHERE a.Deleted=0 
		AND sd.Deleted=0 
		GROUP BY  a.SK_Sales 
		HAVING SUM(COALESCE(sd.CustomerShippingRevenue,0))<>0

---#CUSTOMERSHIPPINGCOST

		IF OBJECT_ID('tempdb..#CUSTOMERSHIPPINGCOST') is not null DROP TABLE  #CUSTOMERSHIPPINGCOST
		SELECT ISNULL(SUM(COALESCE (RealShippingCost,EstimatedShippingCost)),0) CustomerShippingCost, SK_sales INTO #CUSTOMERSHIPPINGCOST
		FROM Reporting_P.dbo.VW_FACT_ShippingCost A (NOLOCK) 
		WHERE Deleted =0
		GROUP BY   SK_sales
		HAVING  ISNULL(SUM(COALESCE(RealShippingCost, EstimatedShippingCost)),0)  <>0


------------------------------------------------------------------------------
------------------------------------------------------------------------------

 
--##############DATE  SALES
 
IF OBJECT_ID('tempdb..#TMP_SALES') is not null DROP TABLE  #TMP_SALES	
SELECT 

A.SK_Sales AS SK_sales
,A.SK_Product AS SK_produs
,A.SK_Product AS SK_produs_istoric
,A.SK_Customer AS SK_client
,A.InvoiceDate AS DataFactura
,A.InvoiceTime AS OraFactura
,A.DeliveryNoteDate AS DataAviz
,A.DeliveryNoteTime AS OraAviz
,A.SalesPostingDate AS DataMarcareVenit
,A.InvoiceNumber AS NrFactura
,A.InvoiceSerialNumber AS SerieFactura
,A.InvoiceIdEIS AS IdFacturaEIS
,A.DeliveryNoteNumber AS NrAviz
,A.Quantity AS Cantitate
,A.Price AS Pret
,A.Cost AS Cost
,A.Sales AS Vanzare
,0 AS FacturaInAvans
,A.BSM_DiscountAmount AS ValoareDiscountBSM
,A.BSM_DiscountAmount_Adjustment AS ValoareDiscountBSM_Reglare
,A.BR_DiscountAmount AS ValoareDiscountBR
,A.BR_DiscountAmount_Adjustment AS ValoareDiscountBR_Reglare
,A.PM_Promotion_DiscountAmount AS ValoareDiscountPromotie_PM
,A.PM_Punctual_DiscountAmount AS ValoareDiscountPunctual_PM
,A.PM_Provision_DiscountAmount AS ValoareDiscountProvizion_PM
,A.PM_Provision_DiscountAmount_Adjustment AS ValoareDiscountProvizion_PM_Reglare
,A.Others_DiscountAmount AS ValoareDiscount_Altele
,A.IssuedVoucherAmount AS ValoareVoucherEmis
,A.ReturnedVoucherAmount AS ValoareVoucherIntors
,A.CancellationReasonId AS IdMotivStornare
,A.LineType AS TipLinie
,A.Deleted AS Deleted
,A.Corporate AS Corporate
,A.Cancellation AS stornare

,A.SK_Supplier AS SK_furnizor
,A.InvoiceLineEIS AS IdLinieFacturaEIS
,A.OrderLine AS IdProdCom
,A.BatchId AS NrLot
,A.GreenTaxSales AS TaxaVerdeVanzare
,A.GreenTaxReception AS TaxaVerdeIntrare
,A.GreenTaxCost AS CostSuplimentar
,A.ModifiedDate AS ModifiedDate
,A.CorporateDiscountAmount AS ValoareDiscountCorporate
,A.VatRate AS VatRate
,A.StorageLocationSales AS StorageLocationSales
,A.OrderId AS OrderId
,A.CountryId AS CountryId
,A.CurrencyId AS CurrencyId
,A.InvoiceIdSAP AS IdFacturaSAP
,A.InvoiceLineIdSAP AS IdLinieFacturaSAP
,A.DeliveryNoteId AS IdAvizSAP
,A.DeliveryNoteLine AS IdLinieAvizSAP
,ISNULL(C.EIS_Description , A.OrderSource)  AS OrderSource
,A.PlatformId AS PlatformId
,A.LoyaltyPointsDiscountAmount AS ValoareDiscountLoyaltyPoints
,A.PaymentMethod AS PaymentMethod
,A.CompanyCode AS CompanyCode
,A.Currency AS Currency
,A.ExchangeRate AS ExchangeRate
,A.MarkupValue AS MarkupValue
,A.ProcessType AS ProcessType
,A.BSM_ProvisionAmount AS ValoareProvizionBSM
,B.StockAge
,A.Rabla_DiscountAmount
INTO #TMP_SALES 
FROM  Reporting_A.dbo.FACT_Sales_W_Discounts A (NOLOCK) 
  LEFT JOIN  Reporting_P.dbo.FACT_Sales_Inventory_Details  B (NOLOCK) ON A.SK_Sales= B.SK_Sales--DataWarehouse.dbo.Sales a
         LEFT JOIN Reporting_A.dbo.DIM_OrderSource C (NOLOCK) ON C.SalesOffice = A.OrderSource 

WHERE 
A.Deleted = 0   and A.CountryId = A.PlatformId and A.OrderId in (SELECT OrderId FROM #ORDERS_DELTA)

 
		CREATE INDEX ind_sales_e ON #TMP_SALES (SK_sales) include (SK_produs, PlatformId);

DROP TABLE IF EXISTS #TMP_VOUCHERE_SATELIT
SELECT SK_Sales SK_sales,
sum(DistributionValue) as ValoareDistributie
INTO #TMP_VOUCHERE_SATELIT
FROM Reporting_A.dbo.FACT_Vouchers  --DataWarehouse.dbo.Vouchere_Satelit 
WHERE Deleted = 0
AND DistributionDate >= '20120501'
GROUP BY SK_Sales

CREATE INDEX IDX_VS ON #TMP_VOUCHERE_SATELIT(SK_sales)

DROP TABLE IF EXISTS #TMP_ALTE_DISC
SELECT SK_Sales SK_sales,
                SUM(ISNULL(DiscountValue,0)) AS alte_discounturi
INTO #TMP_ALTE_DISC
FROM Reporting_A.dbo.FACT_OtherDiscounts
WHERE Deleted = 0 
AND DiscountType = 'dsc_neidentif'
GROUP BY SK_Sales
HAVING SUM(ISNULL(DiscountValue,0))<>0

CREATE INDEX IDX_AD ON #TMP_ALTE_DISC(SK_sales)
 

		IF OBJECT_ID('tempdb..#TMP_SALES_AGGR_ALL') IS NOT NULL DROP TABLE #TMP_SALES_AGGR_ALL
		SELECT 
		a.OrderId,
		a.DataMarcareVenit ,
		ISNULL(Cost,0) + ISNULL(CostSuplimentar,0) + ISNULL(sd.ArtisjusTax_Cost,0) + ISNULL(sd.GreenTax_ByWeight,0) + ISNULL(sd.UPFR_Cost,0) + ISNULL(ew.[Tax For Electronic Waste],0)+ ISNULL(tfp.TotalPackagesTax,0) AS COGS_new,
		ISNULL(a.Vanzare,0) AS ValMarfaVanduta,
		ISNULL(a.Vanzare,0) + (ISNULL(a.ValoareDiscount_Altele,0)+ ISNULL(a.ValoareDiscountLoyaltyPoints,0) + ISNULL(alte_discounturi,0) + isnull(a.Rabla_DiscountAmount,0)) + ISNULL(a.ValoareDiscountCorporate,0) + (ISNULL(a.ValoareDiscountPromotie_PM,0) + ISNULL(a.ValoareDiscountPunctual_PM,0) + CASE WHEN vs.SK_sales IS NULL THEN (ISNULL(a.ValoareDiscountProvizion_PM,0) + ISNULL(a.ValoareDiscountProvizion_PM_Reglare,0)) ELSE ISNULL(vs.ValoareDistributie,0) END) AS Sales,
		((Vanzare) + (ISNULL(a.ValoareDiscount_Altele,0) + ISNULL(a.ValoareDiscountLoyaltyPoints,0) + ISNULL(alte_discounturi,0) + isnull(a.Rabla_DiscountAmount,0)) + ISNULL(a.ValoareDiscountCorporate,0) + (ISNULL(a.ValoareDiscountPromotie_PM,0) + ISNULL(a.ValoareDiscountPunctual_PM,0) + CASE WHEN vs.SK_sales IS NULL THEN (ISNULL(a.ValoareDiscountProvizion_PM,0) + ISNULL(a.ValoareDiscountProvizion_PM_Reglare,0)) ELSE ISNULL(vs.ValoareDistributie,0) END) - (ISNULL(Cost,0) + ISNULL(CostSuplimentar,0) + ISNULL(sd.ArtisjusTax_Cost,0) + ISNULL(sd.GreenTax_ByWeight,0) + ISNULL(sd.UPFR_Cost,0) + ISNULL(ew.[Tax For Electronic Waste],0) + ISNULL(tfp.TotalPackagesTax,0))) AS GP1,
		(ISNULL(a.ValoareDiscountBR,0) + ISNULL(a.ValoareDiscountBR_Reglare,0)) AS BR
		,(ISNULL(a.ValoareDiscountBSM,0) + ISNULL(a.ValoareDiscountBSM_Reglare,0)) AS BSM
		,(isnull(a.ValoareDiscount_Altele,0) +coalesce (alte_discounturi,0) + isnull(a.Rabla_DiscountAmount,0))  as other_discounts,
		ISNULL(bk.Bank_Charges_Value,0) Bank_Charges_Value,
		ISNULL(pck.PackagingTotalValue,0) PackagingTotalValue,
		ISNULL(ocr.CommisionLineValueRevenue,0) CommisionLineValueRevenue,
		ISNULL(occ.CommisionLineValueCost,0) CommisionLineValueCost,
		ISNULL(csr.CustomerShippingRevenue,0) CustomerShippingRevenue,
		ISNULL(csc.CustomerShippingCost,0) CustomerShippingCost
		INTO #TMP_SALES_AGGR_ALL
		FROM #TMP_SALES a WITH (NOLOCK) 
		LEFT JOIN Reporting_A.dbo.DIM_Products_Details  p WITH (NOLOCK) on p.SK_Product = a.SK_produs and  a.PlatformId=p.PlatformId 
		LEFT JOIN Reporting_A.dbo.FACT_Sales_Details  sd WITH (NOLOCK) on a.SK_sales=sd.SK_Sales --adaugat pt aducerea green tax& artisjus care trebuie incluse in COGS
		LEFT JOIN [Reporting_A].[dbo].[FACT_Packages_Costs] tfp WITH (NOLOCK) ON a.SK_sales=tfp.SK_Sales
		LEFT JOIN #ELECTRONICWASTE ew ON ew.SK_sales=a.SK_sales
		LEFT JOIN #TMP_VOUCHERE_SATELIT vs on a.SK_sales = vs.SK_sales
		LEFT JOIN #TMP_ALTE_DISC ad on a.SK_sales = ad.SK_sales
		LEFT JOIN #TMP_BSM_CORRECTION b ON a.SK_sales=b.SK_sales
		LEFT JOIN #BANKCHARGES bk ON bk.SK_sales= a.SK_sales 
		LEFT JOIN #PACKAGINGTOTALVALUE pck on pck.SK_sales= a.SK_sales
		LEFT JOIN #OTHERCOMMISSIONSREVENUE ocr on ocr.SK_sales= a.SK_sales
		LEFT JOIN #OTHERCOMMISSIONSCOST occ on occ.SK_sales= a.SK_sales
		LEFT JOIN #CUSTOMERSHIPPINGREVENUE csr on csr.SK_sales= a.SK_sales
		LEFT JOIN #CUSTOMERSHIPPINGCOST csc on csc.SK_sales= a.SK_sales
		WHERE  b.SK_sales is null and Corporate='Nu'
  
 
 
		IF OBJECT_ID('tempdb..#TMP_GP2_ALL') IS NOT NULL DROP TABLE #TMP_GP2_ALL
		SELECT 
		OrderId,
		SUM(ISNULL(ValMarfaVanduta,0)) Vanzare,
		SUM(ISNULL(Sales,0)) as total_sales,
		SUM(ISNULL(BR,0)) as BR,
		SUM(ISNULL(BSM,0)) as BSM,
		SUM(ISNULL(GP1,0)) as GP1,
		SUM(ISNULL(COGS_new,0)) as COGS,
		SUM(ISNULL(other_discounts,0)) as other_discounts,
		SUM(ISNULL(Bank_Charges_Value,0)) Bank_Charges_Value,
		SUM(ISNULL(PackagingTotalValue,0)) PackagingTotalValue,
		SUM(ISNULL(CommisionLineValueRevenue,0)) CommisionLineValueRevenue,
		SUM(ISNULL(CommisionLineValueCost,0)) CommisionLineValueCost,
		SUM(ISNULL(CustomerShippingRevenue,0)) CustomerShippingRevenue,
		SUM(ISNULL(CustomerShippingCost,0)) CustomerShippingCost
		INTO #TMP_GP2_ALL
		FROM #TMP_SALES_AGGR_ALL a
		GROUP  BY  OrderId
 
 

	   IF OBJECT_ID('tempdb..#MG_OU_R_AGGR') is not null DROP TABLE  #MG_OU_R_AGGR
	   SELECT OrderId,VendorId ,OrderDate ,PlatformId, CountryId ,customers_id_assoc ,SUM(ISNULL((o.Valoare_Comision),0) )Valoare_Comision ,SUM(ISNULL((o.Comision_Target),0) ) Comision_Target,
	   SUM(ISNULL(Storno_CommissionMKTP ,0)) Storno_CommissionMKTP
	     INTO #MG_OU_R_AGGR FROM #MG_OU_R o 
		 GROUP BY OrderId,VendorId ,OrderDate ,PlatformId, CountryId ,customers_id_assoc 

	   IF OBJECT_ID('tempdb..#TMP_CALCULATION') is not null DROP TABLE  #TMP_CALCULATION
	   SELECT COALESCE(a.OrderId,o.OrderId) OrderId,VendorId ,OrderDate ,PlatformId, CountryId ,customers_id_assoc,
SUM(ISNULL((o.Valoare_Comision),0) - ISNULL((o.Comision_Target),0) ) as CommissionMKTP ,
SUM(ISNULL(Storno_CommissionMKTP ,0)) Storno_CommissionMKTP
,SUM(ISNULL(Vanzare ,0)) Vanzare,
SUM(ISNULL(total_sales	 ,0)) total_sales
,SUM(ISNULL(BR	 ,0))BR
,SUM(ISNULL(BSM	 ,0)) BSM
,SUM(ISNULL(GP1	 ,0)) GP1
,SUM(ISNULL(COGS	 ,0)) COGS
,SUM(ISNULL(other_discounts	 ,0)) other_discounts
,SUM(ISNULL(Bank_Charges_Value	 ,0)) Bank_Charges_Value
,SUM(ISNULL(PackagingTotalValue	 ,0)) PackagingTotalValue
,SUM(ISNULL(CommisionLineValueRevenue	 ,0)) CommisionLineValueRevenue
,SUM(ISNULL(CommisionLineValueCost	 ,0)) CommisionLineValueCost
,SUM(ISNULL(CustomerShippingRevenue	 ,0)) CustomerShippingRevenue
,SUM(ISNULL(CustomerShippingCost ,0)) CustomerShippingCost into #TMP_CALCULATION
 FROM #MG_OU_R_AGGR o LEFT JOIN #TMP_GP2_ALL a on o.OrderId = a.OrderId and o.VendorId = 1
-- WHERE o.OrderId  in (74303703,74017365)
GROUP BY COALESCE(a.OrderId,o.OrderId),VendorId ,OrderDate ,PlatformId,CountryId,customers_id_assoc


IF OBJECT_ID('tempdb..#CALCULATION_CUSTOMER_ORDER') is not null DROP TABLE  #CALCULATION_CUSTOMER_ORDER
SELECT 
cast(getdate() as date)  as RunningDate,
cast(a.OrderDate as date) OrderDate
,a.OrderDate as OrderDateTime
,OrderId	
,PlatformId	
,CountryId	
,customers_id_assoc	
,SUM(CommissionMKTP	) CommissionMKTP
,SUM(Storno_CommissionMKTP	) Storno_CommissionMKTP
,SUM(Vanzare	) Vanzare 
,SUM(total_sales	) total_sales
,SUM(BR	)BR
,SUM(BSM	) BSM
,SUM(GP1	) GP1
,SUM(COGS	) COGS
,SUM(other_discounts	)Other_discounts
,SUM(Bank_Charges_Value	)Bank_Charges_Value 
,SUM(PackagingTotalValue)	 PackagingTotalValue
,SUM(CommisionLineValueRevenue	) CommisionLineValueRevenue
,SUM(CommisionLineValueCost	) CommisionLineValueCost
,SUM(CustomerShippingRevenue	) CustomerShippingRevenue
,SUM(CustomerShippingCost) CustomerShippingCost
INTO #CALCULATION_CUSTOMER_ORDER
 FROM #TMP_CALCULATION a
 GROUP BY OrderId,PlatformId,CountryId,customers_id_assoc,a.OrderDate


 -- aduc adresele de mail

-- IF OBJECT_ID('tempdb..#MAIL_ADRESSES_TMP') is not null DROP TABLE  #MAIL_ADRESSES_TMP
--SELECT a.* , b.customers_email_address INTO #MAIL_ADRESSES_TMP FROM #CALCULATION_CUSTOMER_ORDER AS a
--LEFT JOIN DWH2.StageData_B.dwsite.remote_customers_contacte_RO AS b WITH (NOLOCK) ON a.customers_id_assoc = b.customers_id 

IF OBJECT_ID('tempdb..#MAIL_ADRESSES_TMP') is not null DROP TABLE  #MAIL_ADRESSES_TMP
SELECT a.* ,CASE WHEN a.PlatformId =1 then  b.customers_email_address
WHEN a.PlatformId =2 then  b1.customers_email_address
WHEN a.PlatformId =3 then  b2.customers_email_address
end as customers_email_address
 INTO #MAIL_ADRESSES_TMP FROM #CALCULATION_CUSTOMER_ORDER AS a
LEFT JOIN BELLATRIX.StageData_B.dwsite.remote_customers_contacte_RO AS b WITH (NOLOCK) ON a.customers_id_assoc = b.customers_id and a.PlatformId = 1
LEFT JOIN BELLATRIX.StageData_B.dwsite.remote_customers_contacte_BG AS b1 WITH (NOLOCK) ON a.customers_id_assoc = b1.customers_id and a.PlatformId = 2
LEFT JOIN BELLATRIX.StageData_B.dwsite.remote_customers_contacte_HU AS b2 WITH (NOLOCK) ON a.customers_id_assoc = b2.customers_id and a.PlatformId = 3


IF OBJECT_ID('tempdb..#TMP_CATEGORIZARE') is not null DROP TABLE   #TMP_CATEGORIZARE 
select a.* , CASE WHEN (ISNULL([customers_id_assoc], 0) = 0 
				OR customers_email_address like '%client.nedefinit%' 
				OR customers_email_address like 'client%@emag%' 
				OR customers_email_address like '%test@emag.ro%'
				OR customers_email_address like '%tester@emag.ro%'
				OR customers_email_address like '%@emag.com') and  CountryId =1
				 THEN 0 
				 WHEN customers_id_assoc in ( 21110	-- valoare comanda in lei vechi
									 ,2413855	-- client care cumpara doar rovignette
									-- ,222079	-- comanda dubioasa de vloare foarte mare modificata in aug 2016 
									 ,77142		-- cont corporate dintotdeauna, marcat incepand cu 2012 cotnari Iasi
									 ,1761980	-- showroom Crangasi 
									 ,1110050	-- comenzi DANTE INTERNATIONAL
									 	-- aici trebuie trecuta ca nerelevanta cmd # 21421
									
									 ,2479679) and CountryId =1 THEN 2 ---3
									 WHEN 
									 customers_id_assoc in (312982  -- clustere GP -< marja negativa foarte mare: cumpara multe resigilate sau la discount foarte mare. 
									,352715
									,548796
									,631220
									,851379
									,866764
									,953609
									, 32) and CountryId =1 THEN 3 --5

									ELSE 1 END AS CustomerIsRelevant INTO #TMP_CATEGORIZARE FROM #MAIL_ADRESSES_TMP a --WHERE a.PlatformId =1


			Update c
				Set c.CustomerIsRelevant = 4-- 6
					, c.customers_email_address = 'deleted.account@gdpr.reg'
			FROM #TMP_CATEGORIZARE c 
			inner join Reporting_A.dbo.FACT_GDPR_Customers g 
			on c.customers_email_address collate SQL_Latin1_General_CP1_CI_AS = g.Email and c.PlatformId =g.PlatformId
			

			--risk clients 

			IF OBJECT_ID('tempdb..#SUSPECTS') is not null DROP TABLE  #SUSPECTS
		
			select distinct o.EOS_CustomerId customers_id_assoc into #SUSPECTS--, o.IdClient, client_id 
																		from [SIRIUS].DataMining.[rsk].[Output_InitialSuspects] r --select top 10 * from DataScience_DEV.dbo.RiskEngine_Clients
																		inner join [SIRIUS].[Reporting_S].dbo.FACT_OrderHeader o with(nolock) on r.client_id = o.CustomerId 
																		where o.PlatformId = 1

			Update c set CustomerIsRelevant = 5 -- 4
		from #TMP_CATEGORIZARE c where c.customers_id_assoc in (
																		select  customers_id_assoc --, o.IdClient, client_id 
																		from #SUSPECTS
																		)  and c.PlatformId =1


TRUNCATE TABLE Reporting_E.dbo.TMP_FACT_Customer_Segmentation_Table

-- 13 min
INSERT INTO Reporting_E.dbo.TMP_FACT_Customer_Segmentation_Table(
 OrderDate
,OrderDateTime
,OrderId
,PlatformId
,CountryId
,CustomersIdAssoc
,CommissionMKTP
,StornoCommissionMKTP
,TotalSales
,Sales
,BR
,BSM
,GP1
,COGS
,OtherDiscounts
,BankChargesValue
,PackagingTotalValue
,CommisionLineValueRevenue
,CommisionLineValueCost
,CustomerShippingRevenue
,CustomerShippingCost
,CustomersEmailAddress
,CustomerIsRelevant
,ModifiedDate
)
SELECT 
OrderDate
,OrderDateTime
,OrderId
,PlatformId
,CountryId
,customers_id_assoc
,CommissionMKTP
,Storno_CommissionMKTP
,Vanzare
,total_sales
,BR
,BSM
,GP1
,COGS
,Other_discounts
,Bank_Charges_Value
,PackagingTotalValue
,CommisionLineValueRevenue
,CommisionLineValueCost
,CustomerShippingRevenue
,CustomerShippingCost
,customers_email_address as CustomersEmailAddress
,CustomerIsRelevant
,getdate() ModifiedDate
 FROM #TMP_CATEGORIZARE


EXEC DB_Config.dbo.spMerge 'Reporting_E.dbo.TMP_FACT_Customer_Segmentation_Table',0 -- 11 min

-- to acomodate hydra import mechanism which has delete_from_target = 1 for this table in merge table
update a
set a.ModifiedDate = GETDATE()
from Reporting_E.dbo.FACT_Customer_Segmentation_Table a -- 10 min


 
SET NOCOUNT OFF;

END