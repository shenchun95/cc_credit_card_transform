# Credit Card Transaction Data Analysis Pipeline

This repository contains a series of Jupyter Notebooks designed to process and analyze credit card transaction data. The pipeline is structured into three distinct stages, each represented by a separate script: data extraction and transformation, data serving for further transformation, and finally, data visualization and analysis.

## Pipeline Stages

1.  **`cc_credit_card.ipynb` (Extract and Transform - Raw to Curated)**

    * **Purpose:** This is the initial script to be executed. It is responsible for extracting raw credit card transaction data from the designated raw zone (`raw_cc_credit_card`).
    * **Actions:** The script performs the necessary transformations on the raw data, such as cleaning, data type conversions, and initial structuring, to prepare it for further processing in the curated zone (`curated_cc_credit_card`).
    * **Output:** The transformed and cleaned data is stored in the curated zone (`curated_cc_credit_card`).

2.  **`cc_credit_card_data_serving.ipynb` (Serve for Transformation - Curated to Transformed)**

    * **Purpose:** This is the second script in the pipeline. It takes the processed data from the curated zone (`curated_cc_credit_card`) and serves it for further transformation.
    * **Actions:** This script likely involves selecting specific features or preparing the data in a format suitable for more advanced transformations or modeling. The exact transformations performed here will depend on the analytical goals.
    * **Output:** The data, ready for analysis, is moved to the transformed zone (`transformed_cc_credit_card`).

3.  **`cc_credit_card_data_visualization.ipynb` (Visualization and Analysis - Transformed Zone)**

    * **Purpose:** This is the final script in the defined pipeline. It accesses the data located in the transformed zone (`transformed_cc_credit_card`) to generate visualizations and perform in-depth analysis.
    * **Actions:** This script will contain the code for creating various charts, graphs, and statistical summaries to extract meaningful insights from the processed credit card transaction data.
    * **Output:** Visualizations and analytical findings are the primary outputs of this script.

## Data Zones

The data pipeline utilizes the following conceptual zones:

* **Raw Zone (`raw_cc_credit_card`):** This zone contains the original, unprocessed credit card transaction data. **Users should not directly access or modify data in this zone.**
* **Curated Zone (`curated_cc_credit_card`):** This zone holds data that has undergone initial cleaning and transformation from the raw zone. It is a more structured and reliable version of the raw data, prepared for further processing.
* **Transformed Zone (`transformed_cc_credit_card`):** This zone contains the final, analysis-ready data. It is the zone from which data should be accessed for visualization and analytical purposes. **Data users should primarily interact with data in this zone.**

## Instructions

To run the data analysis pipeline, execute the Jupyter Notebooks in the following sequential order:

1.  `cc_credit_card.ipynb`
2.  `cc_credit_card_data_serving.ipynb`
3.  `cc_credit_card_data_visualization.ipynb`

Ensure that the necessary data files are present in the `raw_cc_credit_card` location before running the first script and that each script is executed successfully before proceeding to the next.

This structured approach ensures a clear separation of concerns and facilitates a systematic process for analyzing the credit card transaction data.