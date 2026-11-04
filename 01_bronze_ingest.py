#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# DataCo Smart Supply Chain - Camada BRONZE
# Ingestao fiel do dataset bruto para Delta Lake

import os, sys
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
sys.path.append(str(Path(__file__).parent))
from utils import get_project_root, ensure_dir, setup_logger, save_json

def get_spark(app_name='DataCo_Bronze_Ingest'):
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
    reports_dir = root / 'reports' / 'bronze'
    data_raw = root / 'data' / 'raw'
    data_bronze = root / 'data' / 'bronze'
    ensure_dir(log_dir); ensure_dir(reports_dir); ensure_dir(data_raw); ensure_dir(data_bronze)
    logger = setup_logger('01_bronze_ingest', log_dir)
    logger.info('INICIANDO INGESTAO CAMADA BRONZE')
    csv_file = data_raw / 'DataCoSupplyChainDataset.csv'
    if not csv_file.exists():
        logger.error(f'ARQUIVO NAO ENCONTRADO: {csv_file}')
        logger.error('Baixe em: https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis')
        sys.exit(1)
    spark = get_spark()
    ingestion_ts = datetime.utcnow()
    df_raw = spark.read.option('header','true').option('quote','\"').option('escape','\"').option('encoding','ISO-8859-1').csv(str(csv_file))
    logger.info(f'Linhas: {df_raw.count()} | Colunas: {len(df_raw.columns)}')
    df_bronze = df_raw.withColumn('ingestion_date', lit(ingestion_ts.strftime('%Y-%m-%dT%H:%M:%SZ'))).withColumn('source_file', lit(csv_file.name))
    schema_dict = [{'name':f.name,'type':str(f.dataType),'nullable':f.nullable} for f in df_bronze.schema.fields]
    save_json(schema_dict, reports_dir / 'bronze_schema.json')
    bronze_path = data_bronze / 'dataco_bronze'
    try:
        df_bronze.write.format('delta').mode('overwrite').option('overwriteSchema','true').save(str(bronze_path))
        logger.info(f'GRAVADO EM DELTA: {bronze_path}')
    except Exception as e:
        logger.warning(e)
        df_bronze.write.mode('overwrite').parquet(str(bronze_path))
        logger.info(f'GRAVADO EM PARQUET: {bronze_path}')
    try:
        try:
            df_check = spark.read.format('delta').load(str(bronze_path))
        except:
            df_check = spark.read.parquet(str(bronze_path))
        logger.info(f'VALIDACAO: {df_check.count()} linhas | {len(df_check.columns)} colunas')
    finally:
        spark.stop()
    logger.info('CONCLUIDO')

if __name__=='__main__':
    main()
