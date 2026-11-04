#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataCo Smart Supply Chain - Camada SILVER
Transformacao Logistica + Risco de Atraso + Tratamento Financeiro Suspeito

ETAPA 2
"""

import os, sys
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import (
    col, when, lit, to_date, year, month, quarter,
    upper, trim, coalesce
)

sys.path.append(str(Path(__file__).parent))
from utils import get_project_root, ensure_dir, setup_logger, save_json


def get_spark(app_name='DataCo_Silver_Transform'):
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


def main():
    root = get_project_root()
    log_dir = root / 'logs'
    reports_dir = root / 'reports' / 'silver'
    data_bronze = root / 'data' / 'bronze' / 'dataco_bronze'
    data_silver = root / 'data' / 'silver'
    
    ensure_dir(log_dir)
    ensure_dir(reports_dir)
    ensure_dir(data_silver)
    
    logger = setup_logger('02_silver_transform', log_dir)
    logger.info('=' * 60)
    logger.info('INICIANDO TRANSFORMACAO CAMADA SILVER - DATACO SUPPLY CHAIN')
    logger.info('=' * 60)
    
    if not data_bronze.exists():
        logger.error(f'CAMADA BRONZE NAO ENCONTRADA: {data_bronze}')
        logger.error('Execute primeiro: python src/01_bronze_ingest.py')
        sys.exit(1)
    
    spark = get_spark()
    
    try:
        # 1) LEITURA BRONZE
        logger.info('[ETAPA 1] Lendo camada BRONZE...')
        try:
            df_bronze = spark.read.format('delta').load(str(data_bronze))
            logger.info('[OK] Lido em formato DELTA')
        except:
            df_bronze = spark.read.parquet(str(data_bronze))
            logger.info('[OK] Lido em formato PARQUET')
        
        logger.info(f'Linhas Bronze: {df_bronze.count()} | Colunas: {len(df_bronze.columns)}')
        
        # 2) NORMALIZACAO BASICA
        logger.info('[ETAPA 2] Normalizando strings e tratando campos-chave...')
        df_norm = df_bronze
        for c in ['Delivery Status', 'Order Status', 'Shipping Mode', 'Customer Segment', 
                  'Market', 'Order Region', 'Category Name', 'Department Name']:
            if c in df_norm.columns:
                df_norm = df_norm.withColumn(c, trim(upper(col(c))))
        
        # 3) CALCULO: VARIACAO REAL X AGENDADO
        logger.info('[ETAPA 3] Calculando shipping_delay_days (Real x Agendado)...')
        df_calc = df_norm.withColumn(
            'shipping_delay_days',
            col('Days for shipping (real)') - col('Days for shipment (scheduled)')
        )
        
        # 4) FLAG DE RISCO DE ATRASO
        logger.info('[ETAPA 4] Criando flags de risco de atraso...')
        df_calc = df_calc.withColumn(
            'is_late',
            when(col('shipping_delay_days') > 0, True).otherwise(False)
        ).withColumn(
            'risk_delay_flag',
            when((col('shipping_delay_days') > 0) | (col('Delivery Status').contains('LATE')), True).otherwise(False)
        )
        
        # 5) CLASSIFICACAO DE PERFORMANCE DE ENTREGA
        logger.info('[ETAPA 5] Classificando delivery_performance...')
        df_calc = df_calc.withColumn(
            'delivery_performance',
            when(col('Delivery Status').contains('CANCEL'), 'Shipping canceled')
            .when((col('shipping_delay_days') < 0), 'Advance shipping')
            .when((col('shipping_delay_days') == 0), 'On Time')
            .when((col('shipping_delay_days') > 0), 'Late delivery')
            .otherwise(col('Delivery Status'))
        )
        
        # 6) ENRIQUECIMENTO TEMPORAL
        logger.info('[ETAPA 6] Extraindo order_year/month/quarter a partir de order date (DateOrders)...')
        date_col = None
        for cand in ['order date (DateOrders)', 'Order Date']:
            if cand in df_calc.columns:
                date_col = cand
                break
        
        if date_col:
            df_calc = df_calc.withColumn('order_date_parsed', to_date(to_timestamp(col(date_col), 'M/d/yyyy H:mm')))
            df_calc = df_calc.withColumn('order_year', year(col('order_date_parsed'))) \
                             .withColumn('order_month', month(col('order_date_parsed'))) \
                             .withColumn('order_quarter', quarter(col('order_date_parsed')))
            logger.info(f'[OK] Datas extraidas de: {date_col}')
        else:
            logger.warning('[AVISO] Coluna de data nao encontrada. Criando campos temporais vazios.')
            df_calc = df_calc.withColumn('order_date_parsed', lit(None).cast('date')) \
                             .withColumn('order_year', lit(None).cast('int')) \
                             .withColumn('order_month', lit(None).cast('int')) \
                             .withColumn('order_quarter', lit(None).cast('int'))
        
        # 7) EFICIENCIA DE ENVIO
        logger.info('[ETAPA 7] Classificando shipping_efficiency...')
        df_calc = df_calc.withColumn(
            'shipping_efficiency',
            when(col('shipping_delay_days') <= 0, 'Efficient')
            .when((col('shipping_delay_days') > 0) & (col('shipping_delay_days') <= 1), 'Within Tolerance')
            .when(col('shipping_delay_days') > 1, 'Inefficient')
            .otherwise('Unknown')
        )
        
        # 8) TRATAMENTO DE TRANSACOES FINANCEIRAS SUSPEITAS
        logger.info('[ETAPA 8] Criando flag de transacao financeira suspeita...')
        df_calc = df_calc.withColumn(
            'is_financially_suspicious',
            when(
                (col('Order Status').contains('SUSPECTED')) |
                ((col('Order Profit Per Order') < 0) & (col('Sales') > 0)) |
                ((col('Benefit per order') < 0) & (col('Sales') > 1000)),
                True
            ).otherwise(False)
        ).withColumn(
            'risk_score',
            (when(col('risk_delay_flag'), 1).otherwise(0) + 
             when(col('is_financially_suspicious'), 1).otherwise(0) +
             when(col('shipping_delay_days') > 3, 1).otherwise(0))
        )
        
        # 9) METADADOS SILVER
        logger.info('[ETAPA 9] Adicionando metadados da camada SILVER...')
        df_silver = df_calc.withColumn('silver_processing_date', lit(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')))
        
        # 10) ESCRITA
        logger.info('[ETAPA 10] Gravando na camada SILVER (Delta Lake)...')
        silver_path = data_silver / 'dataco_silver'
        try:
            df_silver.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(str(silver_path))
            logger.info(f'[OK] GRAVADO EM DELTA: {silver_path}')
        except Exception as e:
            logger.warning(f'[AVISO] Falha Delta: {e}')
            df_silver.write.mode('overwrite').parquet(str(silver_path))
            logger.info(f'[OK] GRAVADO EM PARQUET: {silver_path}')
        
        # 11) VALIDACAO
        logger.info('[ETAPA 11] Validacoes...')
        try:
            try:
                df_chk = spark.read.format('delta').load(str(silver_path))
            except:
                df_chk = spark.read.parquet(str(silver_path))
            logger.info(f'Linhas Silver: {df_chk.count()}')
            logger.info(f'Colunas Silver: {len(df_chk.columns)}')
            logger.info(f'Colunas novas: shipping_delay_days, is_late, risk_delay_flag, delivery_performance, order_year, order_month, shipping_efficiency, is_financially_suspicious, risk_score')
            df_chk.groupBy('delivery_performance').count().orderBy('count', ascending=False).show()
            df_chk.groupBy('is_late').count().show()
            df_chk.filter(col('risk_delay_flag')).select('Order Id','Order Item Id','shipping_delay_days','Delivery Status','Order Status').show(5, truncate=False)
        except Exception as e:
            logger.warning(f'Erro na validacao: {e}')
        
        logger.info('=' * 60)
        logger.info('TRANSFORMACAO SILVER CONCLUIDA COM SUCESSO!')
        logger.info(f'Destino: {silver_path}')
        logger.info('=' * 60)
        
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
