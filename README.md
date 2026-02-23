![LEAN Data Source SDK](http://cdn.quantconnect.com.s3.us-east-1.amazonaws.com/datasources/Github_LeanDataSourceSDK.png)

# Lean DataSource SDK

[![Build Status](https://github.com/QuantConnect/LeanDataSdk/workflows/Build%20%26%20Test/badge.svg)](https://github.com/QuantConnect/LeanDataSdk/actions?query=workflow%3A%22Build%20%26%20Test%22)

Welcome to the DataBento Library repository! This library, built on .NET 6, provides seamless integration with the QuantConnect LEAN Algorithmic Trading Engine. It empowers users to interact with DataBento's services to create powerful trading algorithms 

### Introduction

The Lean Data SDK is a cross-platform template repository for developing custom data types for Lean.
These data types will be consumed by [QuantConnect](https://www.quantconnect.com/) trading algorithms and research environment, locally or in the cloud.

It is composed of an example .NET solution for the data type and converter scripts.

DataBento Library is an open-source project written in C#, designed to simplify the process of accessing real-time and historical financial market data. With support for Futures Data across the CME exchange and low latency, it offers a comprehensive solution for algorithmic trading.

### Prerequisites

The solution targets dotnet 6, for installation instructions please follow [dotnet download](https://dotnet.microsoft.com/download).

The data downloader and converter script can be developed in different ways: C# executable, Python script, Python Jupyter notebook or even a bash script.
- The python script should be compatible with python 3.6.8
- Bash script will run on Ubuntu Bionic

Specifically, the environment where these scripts will be run is [quantconnect/research](https://hub.docker.com/repository/docker/quantconnect/research) based on [quantconnect/lean:foundation](https://hub.docker.com/repository/docker/quantconnect/lean).

### DataBento Overview
DataBento provides real time and historical market data through many powerful and developer-friendly APIs. Currently this implementation uses the Globex dataset to access CME
exchanges and provide data on CME products. DataBento provides a wide array of datasets, and exchanges for stocks, futures, and options.

### Tutorial

You can use the following command line arguments to launch the [LEAN CLI](https://github.com/quantConnect/Lean-cli) pip project with DataBento.

#### Downloading data
```
lean data download --data-provider-historical DataBento --data-type Trade --resolution Daily --security-type Future --ticker ES --start 20240303 --end 20240404 --databento-api-key <your-key>
```

#### Backtesting
```
lean backtest "My Project" --data-provider-historical DataBento --databento-api-key <your-key>
```

#### Jupyter Research Notebooks
```
lean research "My Project" --data-provider-historical DataBento --databento-api-key <your-key>
```

#### Live Trading
```
lean live deploy "My Project" --data-provider-live DataBento --brokerage "Paper Trading" --databento-api-key <your-key>
```

### Installation

To contribute to the DataBento API Connector Library for .NET 6 within QuantConnect LEAN, follow these steps:
 - Obtain API Key: Visit [DataBento](https://databento.com/) and sign up for an API key.
 - Fork the Project: Fork the repository by clicking the "Fork" button at the top right of the GitHub page.
 - Clone Your Forked Repository:

Configure your project by
 - Set the databento-api-key in your QuantConnect configuration (config.json or environment variables).

### Documentation
Refer to the [documentation](https://databento.com/docs/) for detailed information on the library's functions, parameters, and usage examples.

### Price Plan

For detailed information on DataBento's pricing plans, please refer to the [DataBento Pricing](https://databento.com/pricing) page.

### License

This project is licensed under the Apache License 2.0 — see the [LICENSE](https://github.com/QuantConnect/Lean.DataSource.DataBento/blob/master/LICENSE) file for details.

Happy coding and algorithmic trading!