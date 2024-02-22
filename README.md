# Lean DateBento Data Source Plugin

[![Build Status](https://github.com/QuantConnect/LeanDataSdk/workflows/Build%20%26%20Test/badge.svg)](https://github.com/QuantConnect/LeanDataSdk/actions?query=workflow%3A%22Build%20%26%20Test%22)

### Introduction
This repository contains a Lean data source plugin for IQFeed, allowing users to seamlessly integrate IQFeed data with  QuantConnect LEAN Algorithmic Trading Engine.

### About IQFeed
The Lean IQFeed Data Source Plugin offers unparalleled market data precision by leveraging IQFeed's true tick-by-tick data feed. Unlike other providers that may miss over 50% of actual trades, IQFeed delivers unfiltered, real-time data for every trade from exchanges directly. Its reliability is attributed to owning and managing its datacenter, eliminating dependence on third parties, and ensuring fully redundant systems to proactively handle outages. IQFeed stands out by providing more comprehensive and faster data, thanks to its advanced Quad Redundant ticker plant designed to handle today's high-volume markets.

### Tutorials

#### Setup IQFeed
1. To use this plugin, you first need to sign up for an IQFeed account. 
Visit [IQFeed](https://www.iqfeed.net/Trent/index.cfm?web=iqfeed&displayaction=start) to create an account.

2. Install IQFeed Client
Download and install the IQFeed client from [IQFeed Download](http://www.iqfeed.net/index.cfm?displayaction=support&section=download).

3. Configure
To configure the IQFeed plugin.