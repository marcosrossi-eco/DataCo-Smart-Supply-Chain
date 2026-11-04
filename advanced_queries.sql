-- =====================================================
-- ETAPA 4: CONSULTAS ANALITICAS AVANCADAS
-- DataCo Smart Supply Chain & Logistica
-- Foco: Impacto financeiro (perda de receita) por entregas atrasadas
-- =====================================================

-- NOTA: Estas queries foram pensadas para Spark SQL
-- Utilize sobre a camada SILVER (dataco_silver) ou GOLD conforme indicado

-- -----------------------------------------------------
-- Q1. VISAO GERAL DE IMPACTO FINANCEIRO POR ATRASO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_geral AS
SELECT 
  COUNT(DISTINCT Order Id) AS total_pedidos,
  COUNT(Order Item Id) AS total_itens,
  SUM(Sales) AS receita_total,
  SUM(Order Profit Per Order) AS lucro_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = false THEN Sales ELSE 0 END) AS receita_no_prazo,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido_aprox,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada,
  ROUND((SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) / NULLIF(SUM(Order Profit Per Order),0)) * 100, 2) AS pct_lucro_comprometido
FROM dataco_silver;

SELECT * FROM vw_impacto_geral;

-- -----------------------------------------------------
-- Q2. IMPACTO FINANCEIRO POR MODO DE ENVIO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_por_shipping AS
SELECT 
  Shipping Mode,
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada,
  ROUND(AVG(CASE WHEN is_late = true THEN shipping_delay_days END), 2) AS avg_atraso_dias,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS qtd_itens_atrasados
FROM dataco_silver
GROUP BY Shipping Mode
ORDER BY receita_atrasada DESC;

SELECT * FROM vw_impacto_por_shipping;

-- -----------------------------------------------------
-- Q3. IMPACTO FINANCEIRO POR CATEGORIA DE PRODUTO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_por_categoria AS
SELECT 
  Category Name,
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS itens_atrasados,
  ROUND((COUNT(CASE WHEN is_late = true THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS taxa_atraso_pct,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada
FROM dataco_silver
GROUP BY Category Name
ORDER BY receita_atrasada DESC;

SELECT * FROM vw_impacto_por_categoria;

-- -----------------------------------------------------
-- Q4. IMPACTO FINANCEIRO POR MERCADO E REGIAO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_por_regiao AS
SELECT 
  Market,
  Order Region,
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS itens_atrasados,
  ROUND((COUNT(CASE WHEN is_late = true THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS taxa_atraso_pct,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada
FROM dataco_silver
GROUP BY Market, Order Region
ORDER BY receita_atrasada DESC;

SELECT * FROM vw_impacto_por_regiao;

-- -----------------------------------------------------
-- Q5. IMPACTO FINANCEIRO POR ANO/MES
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_temporal AS
SELECT 
  order_year,
  order_month,
  COUNT(Order Item Id) AS total_itens,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS itens_atrasados,
  ROUND((COUNT(CASE WHEN is_late = true THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS taxa_atraso_pct,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada
FROM dataco_silver
WHERE order_year IS NOT NULL
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

SELECT * FROM vw_impacto_temporal;

-- -----------------------------------------------------
-- Q6. TOP 20 PEDIDOS COM MAIOR IMPACTO FINANCEIRO POR ATRASO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_top_pedidos_atraso AS
SELECT 
  Order Id,
  Order Item Id,
  Customer Id,
  Product Name,
  Category Name,
  Shipping Mode,
  Market,
  Order Region,
  Order Status,
  Delivery Status,
  delivery_performance,
  is_late,
  risk_delay_flag,
  is_financially_suspicious,
  risk_score,
  shipping_delay_days,
  Sales,
  Order Item Total,
  Order Profit Per Order,
  Benefit per order,
  order date (DateOrders) AS order_date
FROM dataco_silver
WHERE is_late = true
ORDER BY Sales DESC, shipping_delay_days DESC
LIMIT 20;

SELECT * FROM vw_top_pedidos_atraso;

-- -----------------------------------------------------
-- Q7. PEDIDOS CRITICOS COM ATRASO > 3 DIAS E RISCO FINANCEIRO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_pedidos_criticos AS
SELECT 
  Order Id,
  Order Item Id,
  Customer Id,
  Product Name,
  Category Name,
  Shipping Mode,
  Market,
  Order Region,
  Order Status,
  is_late,
  shipping_delay_days,
  risk_delay_flag,
  is_financially_suspicious,
  risk_score,
  Sales,
  Order Profit Per Order
FROM dataco_silver
WHERE is_late = true AND shipping_delay_days > 3 AND (risk_score >= 1 OR is_financially_suspicious = true)
ORDER BY risk_score DESC, shipping_delay_days DESC, Sales DESC;

SELECT * FROM vw_ping_pedidos_criticos;

-- -----------------------------------------------------
-- Q8. OTIF-LIKE (On Time In Full) APROXIMADO
-- Nota: Dataset nao possui quantidade entregue x solicitada
-- Considerando itens 'no prazo' como proxy
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_otif_approx AS
SELECT 
  Shipping Mode,
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  COUNT(CASE WHEN is_late = false THEN 1 END) AS itens_no_prazo,
  COUNT(CASE WHEN delivery_performance = 'On Time' THEN 1 END) AS itens_on_time_declarado,
  ROUND((COUNT(CASE WHEN is_late = false THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS otif_proxy_pct,
  ROUND((COUNT(CASE WHEN delivery_performance = 'On Time' THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS on_time_pct_declarado
FROM dataco_silver
GROUP BY Shipping Mode
ORDER BY otif_proxy_pct DESC;

SELECT * FROM vw_otif_approx;

-- -----------------------------------------------------
-- Q9. IMPACTO FINANCEIRO POR STATUS DE PEDIDO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_impacto_por_status_pedido AS
SELECT 
  Order Status,
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS itens_atrasados,
  COUNT(CASE WHEN is_financially_suspicious = true THEN 1 END) AS itens_fin_suspeitos,
  ROUND((COUNT(CASE WHEN is_late = true THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS taxa_atraso_pct
FROM dataco_silver
GROUP BY Order Status
ORDER BY receita_atrasada DESC;

SELECT * FROM vw_impacto_por_status_pedido;

-- -----------------------------------------------------
-- Q10. RESUMO EXECUTIVO DE IMPACTO FINANCEIRO
-- -----------------------------------------------------
CREATE OR REPLACE TEMP VIEW vw_resumo_executivo_atraso AS
SELECT 
  COUNT(Order Item Id) AS total_itens,
  COUNT(DISTINCT Order Id) AS total_pedidos,
  SUM(Sales) AS receita_total,
  SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) AS receita_atrasada,
  SUM(CASE WHEN is_late = false THEN Sales ELSE 0 END) AS receita_no_prazo,
  SUM(Order Profit Per Order) AS lucro_total,
  SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) AS lucro_comprometido_aprox,
  COUNT(CASE WHEN is_late = true THEN 1 END) AS itens_atrasados,
  COUNT(CASE WHEN is_late = false THEN 1 END) AS itens_no_prazo,
  ROUND((COUNT(CASE WHEN is_late = true THEN 1 END) / COUNT(Order Item Id)) * 100, 2) AS pct_itens_atrasados,
  ROUND((SUM(CASE WHEN is_late = true THEN Sales ELSE 0 END) / NULLIF(SUM(Sales),0)) * 100, 2) AS pct_receita_atrasada,
  ROUND((SUM(CASE WHEN is_late = true THEN Order Profit Per Order ELSE 0 END) / NULLIF(SUM(Order Profit Per Order),0)) * 100, 2) AS pct_lucro_comprometido
FROM dataco_silver;

SELECT * FROM vw_resumo_executivo_atraso;
