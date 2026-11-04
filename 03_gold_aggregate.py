#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataCo Smart Supply Chain - Camada GOLD
Modelagem Analitica: Fato, Dimensoes e Agregados para Torre de Controle

ETAPA 3
"""

import os, sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, sum, avg, count, countDistinct,
    round as spark_round, coalesce
)

sys.path.append(str(Path(__file__).parent))
from utils import get_project_root, ensure_dir, setup_logger, save_json


def get_spark(app_name='DataCo_Gold_Aggregate'):
    builder = (SparkSession.builder.appName(app_name)
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.shuffle.partitions','4'))
    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        return spark
    except Exception as e:
        print('[AVISO]', e)
        spark = SparkSession.builder.appName(app_name).config('spark.sql.shuffle.partitions','4').getOrCreate()
        spark.sparkContext.setLogLevel('WARN')
        return spark


def write_table(spark, df, path, name):
    try:
        df.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(str(path))
        print(f'[OK] {name} gravado em DELTA: {path}')
        return 'delta'
    except Exception as e:
        print(f'[AVISO] Falha Delta para {name}: {e}')
        df.write.mode('overwrite').parquet(str(path))
        print(f'[OK] {name} gravado em PARQUET: {path}')
        return 'parquet'


def main():
    root = get_project_root()
    log_dir = root / 'logs'
    reports_dir = root / 'reports' / 'gold'
    data_silver = root / 'data' / 'silver' / 'dataco_silver'
    data_gold = root / 'data' / 'gold'
    
    ensure_dir(log_dir)
    ensure_dir(reports_dir)
    ensure_dir(data_gold)
    
    logger = setup_logger('03_gold_aggregate', log_dir)
    logger.info('=' * 60)
    logger.info('INICIANDO AGREGACAO CAMADA GOLD - DATACO SUPPLY CHAIN')
    logger.info('=' * 60)
    
    if not data_silver.exists():
        logger.error(f'CAMADA SILVER NAO ENCONTRADA: {data_silver}')
        logger.error('Execute primeiro: python src/02_silver_transform.py')
        sys.exit(1)
    
    spark = get_spark()
    
    try:
        # 1) LEITURA SILVER
        logger.info('[ETAPA 1] Lendo camada SILVER...')
        try:
            df_s = spark.read.format('delta').load(str(data_silver))
            logger.info('[OK] Lido em formato DELTA')
        except:
            df_s = spark.read.parquet(str(data_silver))
            logger.info('[OK] Lido em formato PARQUET')
        
        logger.info(f'Linhas Silver: {df_s.count()} | Colunas: {len(df_s.columns)}')
        
        # Garantir tipos numericos
        df_s = df_s.withColumn('Sales', coalesce(col('Sales'), lit(0))) \
                   .withColumn('Order Profit Per Order', coalesce(col('Order Profit Per Order'), lit(0))) \
                   .withColumn('Benefit per order', coalesce(col('Benefit per order'), lit(0))) \
                   .withColumn('shipping_delay_days', coalesce(col('shipping_delay_days'), lit(0)))
        
        # 2) DIMENSAO CLIENTES
        logger.info('[ETAPA 2] Criando dim_customers...')
        dim_customers = df_s.groupBy('Customer Id').agg(
            countDistinct('Order Id').alias('total_orders'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit'),
            count(when(col('is_late')==True, 1)).alias('late_orders'),
            count(when(col('risk_delay_flag')==True, 1)).alias('risk_orders')
        ).withColumn(
            'pct_late_customer',
            spark_round((col('late_orders')/col('total_orders'))*100, 2)
        )
        if all(c in df_s.columns for c in ['Customer Fname','Customer Lname','Customer Segment','Customer City','Customer Country','Market']):
            cust_extra = df_s.select('Customer Id','Customer Fname','Customer Lname','Customer Segment','Customer City','Customer Country','Market').dropDuplicates(['Customer Id'])
            dim_customers = dim_customers.join(cust_extra, on='Customer Id', how='left')
        write_table(spark, dim_customers, data_gold / 'dim_customers', 'dim_customers')
        
        # 3) DIMENSAO PRODUTOS
        logger.info('[ETAPA 3] Criando dim_products...')
        dim_products = df_s.groupBy('Product Card Id').agg(
            countDistinct('Order Id').alias('total_orders'),
            sum('Order Item Quantity').alias('total_quantity'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            count(when(col('risk_delay_flag')==True, 1)).alias('risk_items')
        ).withColumn(
            'pct_late_product',
            spark_round((col('late_items')/col('total_orders'))*100, 2)
        )
        prod_cols = [c for c in ['Product Card Id','Product Name','Category Id','Category Name','Department Id','Department Name','Product Price'] if c in df_s.columns]
        if prod_cols:
            prod_extra = df_s.select(prod_cols).dropDuplicates(['Product Card Id'])
            dim_products = dim_products.join(prod_extra, on='Product Card Id', how='left')
        write_table(spark, dim_products, data_gold / 'dim_products', 'dim_products')
        
        # 4) FATO LOGISTICA
        logger.info('[ETAPA 4] Criando fact_logistics_performance...')
        fact_cols = [c for c in [
            'Order Id','Order Item Id','Customer Id','Product Card Id','Order Customer Id',
            'order date (DateOrders)','order_date_parsed','order_year','order_month','order_quarter',
            'Shipping Mode','Delivery Status','delivery_performance','Order Status',
            'Market','Order Region','Order Country','Order City',
            'Category Name','Department Name','Product Name',
            'Days for shipping (real)','Days for shipment (scheduled)','shipping_delay_days',
            'is_late','risk_delay_flag','shipping_efficiency','is_financially_suspicious','risk_score',
            'Order Item Quantity','Sales','Order Item Total','Order Profit Per Order','Benefit per order',
            'Order Item Discount','Order Item Discount Rate',
            'ingestion_date','silver_processing_date'
        ] if c in df_s.columns]
        fact_logistics = df_s.select(fact_cols).withColumn('gold_processing_date', lit(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')))
        write_table(spark, fact_logistics, data_gold / 'fact_logistics_performance', 'fact_logistics_performance')
        
        # 5) AGREGADO POR SHIPPING MODE
        logger.info('[ETAPA 5] Criando agg_performance_by_shipping_mode...')
        agg_ship = df_s.groupBy('Shipping Mode').agg(
            count('Order Item Id').alias('total_items'),
            countDistinct('Order Id').alias('total_orders'),
            count(when(col('is_late')==False, 1)).alias('on_time_items'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            spark_round(avg(when(col('is_late')==True, col('shipping_delay_days'))), 2).alias('avg_delay_late_days'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit')
        ).withColumn('pct_on_time', spark_round((col('on_time_items')/col('total_items'))*100, 2)) \
         .withColumn('pct_late', spark_round((col('late_items')/col('total_items'))*100, 2))
        write_table(spark, agg_ship, data_gold / 'agg_performance_by_shipping_mode', 'agg_performance_by_shipping_mode')
        
        # 6) AGREGADO POR MARKET/REGION
        logger.info('[ETAPA 6] Criando agg_performance_by_market_region...')
        agg_mr = df_s.groupBy('Market','Order Region').agg(
            count('Order Item Id').alias('total_items'),
            countDistinct('Order Id').alias('total_orders'),
            count(when(col('is_late')==False, 1)).alias('on_time_items'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            spark_round(avg(when(col('is_late')==True, col('shipping_delay_days'))), 2).alias('avg_delay_late_days'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit')
        ).withColumn('pct_on_time', spark_round((col('on_time_items')/col('total_items'))*100, 2)) \
         .withColumn('pct_late', spark_round((col('late_items')/col('total_items'))*100, 2))
        write_table(spark, agg_mr, data_gold / 'agg_performance_by_market_region', 'agg_performance_by_market_region')
        
        # 7) TAXA DE ATRASO POR CATEGORIA DE PRODUTO
        logger.info('[ETAPA 7] Criando agg_delay_rate_by_category...')
        agg_cat = df_s.groupBy('Category Name').agg(
            count('Order Item Id').alias('total_items'),
            countDistinct('Order Id').alias('total_orders'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            count(when(col('is_late')==False, 1)).alias('on_time_items'),
            spark_round(avg(col('shipping_delay_days')), 2).alias('avg_delay_all_days'),
            spark_round(avg(when(col('is_late')==True, col('shipping_delay_days'))), 2).alias('avg_delay_late_days'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit'),
            sum(when(col('is_late')==True, col('Sales'))).alias('sales_late')
        ).withColumn('taxa_atraso_pct', spark_round((col('late_items')/col('total_items'))*100, 2)) \
         .withColumn('taxa_no_prazo_pct', spark_round((col('on_time_items')/col('total_items'))*100, 2))
        write_table(spark, agg_cat, data_gold / 'agg_delay_rate_by_category', 'agg_delay_rate_by_category')
        
        # 8) REGIOES COM MAIOR INDICE DE PERDAS/FRAUDES
        logger.info('[ETAPA 8] Criando agg_losses_fraud_by_region...')
        agg_loss = df_s.groupBy('Market','Order Region').agg(
            count('Order Item Id').alias('total_items'),
            count(when(col('risk_delay_flag')==True, 1)).alias('items_risk_delay'),
            count(when(col('is_financially_suspicious')==True, 1)).alias('items_fin_suspect'),
            count(when(col('risk_score')>=2, 1)).alias('items_high_risk'),
            sum('Sales').alias('total_sales'),
            sum(when(col('is_late')==True, col('Sales'))).alias('sales_atrasados'),
            sum(when(col('is_financially_suspicious')==True, col('Sales'))).alias('sales_fin_suspect'),
            sum(when(col('is_late')==True, col('Order Profit Per Order'))).alias('profit_comprometido_aprox'),
            sum('Order Profit Per Order').alias('total_profit')
        ).withColumn('pct_risco_delay', spark_round((col('items_risk_delay')/col('total_items'))*100, 2)) \
         .withColumn('pct_fin_suspeita', spark_round((col('items_fin_suspect')/col('total_items'))*100, 2)) \
         .withColumn('indice_perdas_fraude', spark_round(((col('items_risk_delay')+col('items_fin_suspect'))/col('total_items'))*100, 2))
        write_table(spark, agg_loss, data_gold / 'agg_losses_fraud_by_region', 'agg_losses_fraud_by_region')
        
        # 9) EFICIENCIA DE METODOS DE ENVIO
        logger.info('[ETAPA 9] Criando agg_shipping_efficiency_method...')
        agg_eff = df_s.groupBy('Shipping Mode','shipping_efficiency').agg(
            count('Order Item Id').alias('total_items'),
            countDistinct('Order Id').alias('total_orders'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            avg(col('shipping_delay_days')).alias('avg_delay_days'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit')
        ).withColumn('avg_delay_days', spark_round(col('avg_delay_days'), 2))
        agg_eff_pivot = df_s.groupBy('Shipping Mode').agg(
            count('Order Item Id').alias('total_items'),
            count(when(col('shipping_efficiency')=='Efficient', 1)).alias('efficient_items'),
            count(when(col('shipping_efficiency')=='Within Tolerance', 1)).alias('within_tol_items'),
            count(when(col('shipping_efficiency')=='Inefficient', 1)).alias('inefficient_items'),
            count(when(col('is_late')==False, 1)).alias('on_time_items'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            spark_round(avg(col('shipping_delay_days')), 2).alias('avg_delay_global'),
            sum('Sales').alias('total_sales')
        ).withColumn('pct_efficient', spark_round((col('efficient_items')/col('total_items'))*100, 2)) \
         .withColumn('pct_on_time', spark_round((col('on_time_items')/col('total_items'))*100, 2)) \
         .withColumn('pct_late', spark_round((col('late_items')/col('total_items'))*100, 2))
        write_table(spark, agg_eff_pivot, data_gold / 'agg_shipping_efficiency_method', 'agg_shipping_efficiency_method')
        write_table(spark, agg_eff, data_gold / 'agg_shipping_efficiency_detail', 'agg_shipping_efficiency_detail')
        
        # 10) RISCO/FRAUDE GERAL
        logger.info('[ETAPA 10] Criando agg_risk_fraud_analysis...')
        agg_risk = df_s.groupBy('Order Status','Delivery Status').agg(
            count('Order Item Id').alias('total_items'),
            countDistinct('Order Id').alias('total_orders'),
            count(when(col('is_late')==True, 1)).alias('late_items'),
            count(when(col('risk_delay_flag')==True, 1)).alias('risk_delay_items'),
            count(when(col('is_financially_suspicious')==True, 1)).alias('fin_suspect_items'),
            count(when(col('risk_score')>=2, 1)).alias('high_risk_items'),
            sum('Sales').alias('total_sales'),
            sum('Order Profit Per Order').alias('total_profit'),
            spark_round(avg(col('shipping_delay_days')), 2).alias('avg_delay_days')
        ).withColumn('pct_late', spark_round((col('late_items')/col('total_items'))*100, 2)) \
         .withColumn('pct_fin_suspect', spark_round((col('fin_suspect_items')/col('total_items'))*100, 2))
        write_table(spark, agg_risk, data_gold / 'agg_risk_fraud_analysis', 'agg_risk_fraud_analysis')
        
        # 11) VALIDACAO
        logger.info('[ETAPA 11] Validacoes rapidas...')
        try:
            logger.info('Top 5 Categorias com maior taxa de atraso:')
            spark.read.format('delta').load(str(data_gold / 'agg_delay_rate_by_category')) \
                .orderBy(col('taxa_atraso_pct').desc()).show(5, truncate=False)
            logger.info('Top 5 Regioes com maior indice de perdas/fraudes:')
            spark.read.format('delta').load(str(data_gold / 'agg_losses_fraud_by_region')) \
                .orderBy(col('indice_perdas_fraude').desc()).show(5, truncate=False)
            logger.info('Eficiência por Metodo de Envio:')
            spark.read.format('delta').load(str(data_gold / 'agg_shipping_efficiency_method')) \
                .show(truncate=False)
        except Exception as e:
            logger.warning(f'Erro na validacao: {e}')
        
        logger.info('=' * 60)
        logger.info('CAMADA GOLD CONCLUIDA COM SUCESSO!')
        logger.info(f'Destino: {data_gold}')
        logger.info('Tabelas criadas: dim_customers, dim_products, fact_logistics_performance, agg_performance_by_shipping_mode, agg_performance_by_market_region, agg_delay_rate_by_category, agg_losses_fraud_by_region, agg_shipping_efficiency_method, agg_shipping_efficiency_detail, agg_risk_fraud_analysis')
        logger.info('=' * 60)
        
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
