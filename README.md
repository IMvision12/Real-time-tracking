# Real-time-tracking

![map](https://github.com/user-attachments/assets/70ef7430-621a-4e88-9de2-3ab990e2c7f5)

# Project Overview

This project is a comprehensive real-time bus tracking application that leverages modern web technologies and big data processing to provide live bus location tracking and analytics.

# Technologies Used

1. Frontend: Leaflet.js for interactive mapping
2. Backend: Python Flask
3. Big Data Processing: Apache Hadoop / HDFS

# Installation

1. Clone the repository

    ```python
    git clone https://github.com/IMvision12/Real-time-tracking
    cd Real-time-tracking
    ```

2. Set up Python virtual environment

    ```python
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3. Navigate to your Hadoop installation directory
   
5. Locate and edit the configuration files:
   * `etc/hadoop/core-site.xml`
   * `etc/hadoop/hdfs-site.xml`
   * Ensure network and storage paths are correctly specified
      
6. Start Hadoop Services
   * Open a terminal with administrator privileges
   * Run the Hadoop cluster startup command: `start-all.cmd`
   * Verify Hadoop services are running:
      + Check NameNode and DataNode status
      + Confirm no startup errors in the console
         
7. Start Data Collection Service
   * Update the API key in `config.py` and `leaf.js` files
   * Open a new terminal
   * Navigate to the project's `src` directory: `cd Real-time-tracking/src`
   * Launch the MTA Bus API data fetching script: `python main.py`
   * Verify data ingestion is working correctly
      + Check console logs for successful API connections
      + Monitor initial data retrieval process
         
9. Launch Flask Web Application
   * pen another terminal
   * Ensure you're in the `Real-time-tracking` project directory
   * Start the Flask web application: `python app.py`
