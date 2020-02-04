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

•	Step 1: normalize the incoming data
•	Step 2: calculate MRR, accounts per month and ARPU
•	Step 3: calculate MRR changes
•	Step 4: Calculate Churn
•	Step 5: Calculate Total per Month
•	Step 6: Join Everything and Calculate Net New MRR

WITH v_charges AS (
    SELECT org_id,
	       date_trunc('month', start_date) AS month,
	       coalesce((extra::json->>'amount')::float, (extra::json->>'charged_amount')::integer/100) as total
	FROM charges
	WHERE extra::json->>'months' = '1'
), 
v_mrr as (
	SELECT month, 
	       sum(total) as mrr, 
	       count(distinct org_id) as accounts,
	       sum(total) / count(distinct org_id) as arpu
	FROM v_charges
	GROUP BY 1
),
v_mrr_changes AS (
	SELECT this_month.org_id, 
	       this_month.month, 
	       case 
	         when previous_month.month is null then this_month.total
	         else 0 
	       end as new_mrr,
	       case 
	         when previous_month.total is null then 0
	         when previous_month.total > this_month.total then previous_month.total - this_month.total 
	       end as contraction_mrr,
	       case 
	         when previous_month.total is null then 0
	         when previous_month.total < this_month.total then this_month.total  - previous_month.total
	       end as expansion_mrr
	FROM v_charges as this_month
	LEFT JOIN v_charges previous_month ON this_month.org_id = previous_month.org_id AND this_month.month = previous_month.month + interval '1 month'
),
v_mrr_churn AS (
	SELECT this_month.month + interval '1 month' as month,
	       sum(
	         case 
	           when next_month.month is null then this_month.total
	           else 0 
	         end) as churned_mrr,
	       100.0 * sum(
	        case 
	          when next_month.month is null then this_month.total
	          else 0 
	        end) / v_mrr.mrr as mrr_churn,
	      100.0 * (sum(
	        case 
	          when next_month.month is null then 1
	          else 0 
	        end) * 1.0) / v_mrr.accounts as accounts_churn
	FROM v_charges as this_month
	LEFT JOIN v_charges next_month ON this_month.org_id = next_month.org_id AND this_month.month = next_month.month - interval '1 month'
	JOIN v_mrr on v_mrr.month = this_month.month
	group by 1, v_mrr.mrr, v_mrr.accounts
),
v_totals as (
	SELECT v_mrr_changes.month, 
	       sum(new_mrr) as new_mrr, 
	       sum(contraction_mrr) as contraction_mrr, 
	       sum(expansion_mrr) as expansion_mrr
	FROM v_mrr_changes 
	GROUP BY 1
)

SELECT v_totals.month, 
	       v_mrr.mrr, 
	       v_mrr.accounts, 
	       v_totals.new_mrr, 
	       v_totals.expansion_mrr, 
	       v_mrr_churn.churned_mrr*-1 as churned_mrr, 
	       v_totals.contraction_mrr*-1 as contraction_mrr, 
	       v_totals.net_new_mrr, 
	       new_mrr + expansion_mrr - churned_mrr - contraction_mrr as net_new_mrr,
	       mrr_churn, 
	       accounts_churn, 
	       v_mrr.arpu
	FROM v_totals
	LEFT JOIN v_mrr_churn on v_totals.month = v_mrr_churn.month
	JOIN v_mrr on v_mrr.month = v_totals.month
	WHERE v_totals.month < date_trunc('month', now())
	ORDER BY month desc
