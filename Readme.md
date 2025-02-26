Stock Trading Analysis with Apache Flink on Ververica
-----------------------------------------------------
<p align="center">
    <img src="assets/logo.png">
</p>

**Note:** The easiest way to get started and deploy the application is by signing up for a free trial of [Ververica Cloud](https://www.ververica.com/deployment/managed-service?gad_source=1&gclid=Cj0KCQiA8fW9BhC8ARIsACwHqYqmdHylwUw1MfRyanPmLd5ZdKIMvPHCwlnmfG9KTb-8QwktbFKHy38aAtG-EALw_wcB).

You can also find more details on this video [here]().
# Project Overview
You can find the application [here](src/main/java/com/ververica/apps). This project provides multiple Apache Flink streaming jobs to handle various stock market scenarios:
1. **MarketEventImpactAnalysis**  
   Evaluates how specific market events impact stock prices in real time.

2. **PriceSpikeDetection**  
   Detects significant stock price increases within a defined time window using Flink\'s CEP.

3. **RapidTradeDetection**  
   Identifies potential rapid or manipulative trading activities by detecting multiple trades from the same trader in quick succession, while also enriching trade data.

4. **SmartTradeExecution**  
   Dynamically decides whether to execute, delay, or cancel trade orders based on real-time price volatility thresholds.

### Sample Data Generation


- **`models package`**  
  Defines the data model for stock trading, including `Market Events`, `Stocks`, `Trade Orders` and `Trader` details.

- **`StockMarketDataGenerator.java`**  
  Uses [**javafaker**](https://github.com/DiUS/java-faker) to create realistic synthetic transaction data with random amounts, currency codes, merchants, and geolocation.

- **`FxProducer.java`**
  Generates transactions and sends them to a Kafka topic. Make sure to fill in the required properties for `bootstrap.servers` and `sasl.jaas.config` into the `AppConfig.java` file.


## Build and Package

Use Maven to build and package the application:
```shell
mvn clean package
```

## Deploy and Run

Run any of the jobs by specifying its main class. For example:
```shell
java -cp target/your-jar-file.jar com.ververica.apps.SmartTradeExecution
```

Replace `SmartTradeExecution` with the desired job name.

## Requirements

- Java 11
- Maven
- Apache Flink 1.17
