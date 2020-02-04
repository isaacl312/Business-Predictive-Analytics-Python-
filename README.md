# Business-Predictive-Analytics-Python-

e-commerce dataset 


(1) Metrics
(Data set: transaction log, Product, Customer) 
Computed Monthly Metrics: [

(Monthly Rev            by Segmentation, Category), 
(Monthly Rev Gr         by Segmentation, Category), 
(Monthly Avg Rev        by Segmentation, Category),
(Monthly Sales Quantity by Segmentation, Category),
(Monthly Avg Rev/Order  by Segmentation),
(MAU,Gr                 by Segmentation),
(Monthly New Customer, Gr)
(Monthly Customer Ratio by Segmentation),
(Monthly Retention      by Segmentation)
(Cohort based retention rate)



What metrics do we track?
•	MRR — Monthly Recurring Revenue
•	# of Paying Accounts
•	New MRR: Additional MRR from new customers
•	Expansion MRR: Additional MRR from existing customers upgrades
•	Churned MRR: MRR lost from cancellations
•	Contraction MRR: MRR lost from existing customers downgrades
•	Net New MRR: (New MRR + Expansion MRR) — (Churned MRR + Contraction MRR)
•	MRR Churn/Accounts Churn: % of users who left this month, but were active last month
•	ARPU: Average Revenue Per User (Account)


Fast Rolling 1M growth metrics in SQL
(1) DAU : unique active user 1M
(2) first month activation rate (new/DAU)
(3) Retention rate mom    (MAU1m/ MAU2m-1m)
(4) Reactivation rate mom (MAU1m/ not active 2m-1m)
