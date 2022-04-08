from loguru import logger
from pathlib import Path
from tqdm import tqdm
from privatekube.experiments.utils import save_yaml, load_yaml, yaml_dir_to_df
import os
import itertools
import subprocess
import pandas as pd
import datetime
import time
import numpy as np

from ray.util.multiprocessing import Pool

from utils import plot_workload_run, load_block_claims, plot_schedulers_run
from utils import DEFAULT_LOG_PATH, DEFAULT_GO_EXEC


def analyze(config):
    config_output = Path("/home/kelly/PrivateKube/evaluation/macrobenchmark/logs/schedulers/online-0407-074256/0")
    output_dir = Path("/home/kelly/PrivateKube/evaluation/macrobenchmark/logs/schedulers/online-0407-074256/0")
    logger.info("Analyzing the config.")
    (blocks_df, claims_df) = load_block_claims(
        config_output.joinpath("claims.json"),
        config_output.joinpath("blocks.json"),
    )
    plot_workload_run(blocks_df, claims_df, config_output)
    config["n_pipelines"] = len(claims_df)
    config["n_mice"] = len(claims_df.query("mice == True"))
    config["n_elephants"] = len(claims_df.query("mice == False"))

    config["n_allocated_pipelines"] = sum(claims_df["success"])
    config["n_allocated_mice"] = sum(claims_df.query("mice == True")["success"])
    config["n_allocated_elephants"] = sum(
        claims_df.query("mice == False")["success"]
    )
    config["fraction_allocated_pipelines"] = config["n_allocated_pipelines"] / len(
        claims_df
    )
    total_profit = 0
    for _, claim in claims_df.iterrows():

        if claim["success"] == True:
            total_profit += claim["profit"]
    config["realized_profit"] = total_profit
    path = config["workload"] if "workload" in config else config["mice"]
    if "/user-time/" in path:
        semantic = "user-time"
    elif "/user/" in path:
        semantic = "user"
    elif "/event/" in path:
        semantic = "event"
    config["semantic"] = semantic

    # metrics.append(config)
    save_yaml(config_output.joinpath("metrics.yaml"), config)

    metrics = [config]
    try:
        data = {}
        for key in metrics[0].keys():
            data[key] = []
        for metric in metrics:
            for k, v in metric.items():
                data[k].append(v)
        metrics_df = pd.DataFrame(data=data)
        metrics_df.to_csv(output_dir.joinpath("metrics.csv"))
        plot_schedulers_run(metrics_df, output_dir)
    except Exception as e:
        logger.error(e)
        save_yaml(output_dir.joinpath("metrics.yaml"), metrics)


if __name__ == "__main__":
    config = load_yaml("/home/kelly/PrivateKube/evaluation/macrobenchmark/online.yaml")
    analyze(config)
