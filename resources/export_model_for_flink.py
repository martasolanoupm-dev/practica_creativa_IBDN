#!/usr/bin/env python3
"""
export_model_for_flink.py

Exporta el modelo Random Forest (y sus artefactos) a un JSON parseando
el toDebugString del modelo. Enfoque robusto e independiente de la version.
"""

import sys
import json
import os
import re

from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer, StringIndexerModel
from pyspark.ml.classification import RandomForestClassificationModel


def build_spark():
    return (
        SparkSession.builder
        .appName("export_model_for_flink")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def parse_tree_debug_string(debug_str):
    """
    Parsea el toDebugString de un DecisionTreeModel de Spark y devuelve
    un dict recursivo con la estructura del arbol.

    Formato esperado (cada linea tiene una indentacion con espacios):
      If (feature N <= X.X)
       If (feature M in {a,b,c})
        Predict: 0.0
       Else (feature M not in {a,b,c})
        Predict: 1.0
      Else (feature N > X.X)
       Predict: 2.0
    """
    lines = debug_str.strip().split("\n")
    # Ignorar la primera linea del tipo "DecisionTreeClassificationModel..."
    # o "Tree 0 (weight 1.0):" - buscar la primera linea con "If" o "Predict"
    tree_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("If ") or stripped.startswith("Else ") or stripped.startswith("Predict:"):
            tree_lines.append(line)

    if not tree_lines:
        raise ValueError("No se encontraron nodos en el debug string")

    # Calcular la indentacion base (la del primer nodo)
    base_indent = len(tree_lines[0]) - len(tree_lines[0].lstrip())

    def get_indent(line):
        return len(line) - len(line.lstrip()) - base_indent

    def parse(idx):
        """Parsea recursivamente desde la linea idx. Devuelve (nodo, idx_siguiente)."""
        line = tree_lines[idx]
        stripped = line.strip()

        # HOJA: "Predict: X.X"
        m = re.match(r"Predict:\s*([-\d.]+)", stripped)
        if m:
            return {"is_leaf": True, "prediction": float(m.group(1))}, idx + 1

        # NODO INTERNO: "If (feature N <= X.X)" o "If (feature N in {...})"
        m_cont = re.match(r"If\s*\(feature\s+(\d+)\s*<=\s*([-\d.E]+)\)", stripped)
        m_cat = re.match(r"If\s*\(feature\s+(\d+)\s*in\s*\{([^}]*)\}\)", stripped)

        if m_cont:
            feature = int(m_cont.group(1))
            threshold = float(m_cont.group(2))
            left, next_idx = parse(idx + 1)
            # La siguiente linea debe ser el "Else"; saltarla y parsear el hijo derecho
            # El "Else" esta al mismo nivel de indentacion que el "If"
            right, next_idx2 = parse(next_idx + 1)
            return {
                "is_leaf": False,
                "feature": feature,
                "split_type": "continuous",
                "threshold": threshold,
                "left": left,
                "right": right,
            }, next_idx2

        if m_cat:
            feature = int(m_cat.group(1))
            cats_str = m_cat.group(2)
            categories_left = [float(c.strip()) for c in cats_str.split(",") if c.strip()]
            left, next_idx = parse(idx + 1)
            right, next_idx2 = parse(next_idx + 1)
            return {
                "is_leaf": False,
                "feature": feature,
                "split_type": "categorical",
                "categories_left": categories_left,
                "left": left,
                "right": right,
            }, next_idx2

        raise ValueError(f"Linea no reconocida: {line}")

    root, _ = parse(0)
    return root


def main(base_path):
    spark = build_spark()

    # 1. Bucketizer
    bucketizer_path = "s3a://practica/models/arrival_bucketizer_2.0.bin"
    print(f"Cargando Bucketizer de {bucketizer_path} ...")
    bucketizer = Bucketizer.load(bucketizer_path)
    bucketizer_splits = list(bucketizer.getSplits())
    bucketizer_splits_json = []
    for s in bucketizer_splits:
        if s == float("inf"):
            bucketizer_splits_json.append("Infinity")
        elif s == -float("inf"):
            bucketizer_splits_json.append("-Infinity")
        else:
            bucketizer_splits_json.append(float(s))

    # 2. StringIndexers
    string_indexers = {}
    for column in ["Carrier", "Origin", "Dest", "Route"]:
        indexer_path = f"s3a://practica/models/string_indexer_model_{column}.bin"
        print(f"Cargando StringIndexer de {indexer_path} ...")
        indexer = StringIndexerModel.load(indexer_path)
        labels = list(indexer.labels)
        string_indexers[column] = {label: float(i) for i, label in enumerate(labels)}

    # 3. Feature columns
    feature_columns = [
        "DepDelay", "Distance",
        "DayOfMonth", "DayOfWeek", "DayOfYear",
        "Carrier_index", "Origin_index", "Dest_index", "Route_index"
    ]

    # 4. Random Forest via toDebugString
    model_path = "s3a://practica/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    print(f"Cargando RandomForest de {model_path} ...")
    rf_model = RandomForestClassificationModel.load(model_path)

    print(f"Numero de arboles: {rf_model.getNumTrees}")
    print(f"Numero total de nodos: {rf_model.totalNumNodes}")

    trees_json = []
    for i, tree_model in enumerate(rf_model.trees):
        print(f"  Exportando arbol {i+1}/{len(rf_model.trees)} ...")
        debug = tree_model.toDebugString
        trees_json.append(parse_tree_debug_string(debug))

    # 5. Empaquetar y guardar
    export = {
        "bucketizer_splits": bucketizer_splits_json,
        "string_indexers": string_indexers,
        "feature_columns": feature_columns,
        "num_classes": len(bucketizer_splits) - 1,
        "num_trees": len(trees_json),
        "trees": trees_json,
    }

    output_dir = os.path.join(base_path, "data")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "flink_model.json")
    print(f"Escribiendo modelo exportado a {output_path} ...")
    with open(output_path, "w") as f:
        json.dump(export, f, indent=2)

    print("Exportacion completa.")
    print(f"Tamano del JSON: {os.path.getsize(output_path) / 1024:.1f} KB")

    spark.stop()


if __name__ == "__main__":
    base_path = sys.argv[1] if len(sys.argv) > 1 else "/opt/train"
    main(base_path)