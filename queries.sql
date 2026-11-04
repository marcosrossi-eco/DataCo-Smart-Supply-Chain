-- Pré-requisito: Crawlers (bronze/silver/gold) -> database dataco_db. Ajuste nomes de tabela criados pelo crawler.
-- Q1 Atraso por modal
SELECT shipping_mode, late_pct, avg_delay_days, items FROM dataco_db.agg_shipping_mode ORDER BY late_pct DESC;
-- Q2 Receita e lucro por mercado
SELECT market, ROUND(SUM(sales),0) sales, ROUND(SUM(profit),0) profit, ROUND(AVG(late_pct),1) late_pct
FROM dataco_db.agg_market_region GROUP BY market ORDER BY sales DESC;
-- Q3 Top 10 categorias por perda
SELECT category_name, loss_value, late_pct FROM dataco_db.agg_category ORDER BY loss_value DESC LIMIT 10;
-- Q4 Evolução mensal
SELECT order_ym, sales, profit, late_pct FROM dataco_db.agg_monthly ORDER BY order_ym;
-- Q5 Fraude por região (Silver)
SELECT order_region, COUNT_IF(is_fraud) fraud, COUNT(*) items, ROUND(100.0*COUNT_IF(is_fraud)/COUNT(*),2) fraud_pct
FROM dataco_db.silver GROUP BY order_region ORDER BY fraud DESC LIMIT 10;
-- Q6 Ranking de clientes com janela
SELECT customer_id, SUM(sales) sales, RANK() OVER (ORDER BY SUM(sales) DESC) rk FROM dataco_db.silver GROUP BY customer_id LIMIT 20;
