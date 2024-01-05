# Heart Disease Prediction

The heart disease prediction app was developed in Python, utilizing a well-trained trained machine learning model (Logistic Regression). Through cross-validation and assessment with Decision Tree and Artificial Neural Network (ANN), the Logistic Regression model demonstrated the best performance with an accuracy rate of 80%. 

The application was deployed in Render to make it publicly available to the users with no extra charge :wink:.

App link: https://cardio-insight.onrender.com

![image](https://github.com/abccastro/Heart-Disease-Prediction-using-ML-and-DL/assets/113273491/632c88fa-333b-4ab0-a6f4-3563a2ec3706)

The application's output includes a prediction and the corresponding probability of whether an individual has heart disease or not.

Relevant parameters/input of the application:
- Sex (Male or Female)
- Age Category (18 and above)
- Body Mass Index (BMI)
- General health condition (Poor, Fair, Good, Very Good or Excellent)
- Hours of sleep (on average)
- Smoker 
    - Never smoked
    - Former smoker
    - Current smoker
- Alcohol drinker
  - For adult men: having more than 14 drinks per week
  - For adult women: having more than 7 drinks per week
- Have/had a stroke
- Have/had a diabetes
- Have/had an asthma
- Have/had a skin cancer
- Have/had a kidney disease (excluding kidney stones, bladder infection or incontinence)
- Doing physical activity or exercise during the past 30 days other than the regular job
- Difficulty walking or climbing stairs

## General Info

According to [World Health Organization (WHO)](https://www.who.int/health-topics/cardiovascular-diseases#tab=tab_1), cardiovascular diseases (CVDs) are the leading cause of death globally, taking an estimated 17.9 million lives each year. CVDs are a group of disorders of the heart and blood vessels and include coronary heart disease, cerebrovascular disease, rheumatic heart disease and other conditions. Hence, team became fascinated by this subject and aims to explore further, understanding the main causes of the disease. This involves analyzing indicators of heart disease and ultimately developing a model to predict whether an individual has heart disease or not.

The dataset selected for this project is obtained from the Behavioral Risk Factor Surveillance System (BRFSS) by the CDC. This health survey, operational since 1984, annually surveys more than 400,000 adults across all U.S. states and territories. The dataset includes information for 2022 and comprises vital factors associated with heart disease, such as diabetes, obesity, lack of physical activity, and excessive alcohol consumption. 

Link of the dataset: https://www.kaggle.com/datasets/kamilpytlak/personal-key-indicators-of-heart-disease

Because we had data for both 2020 and 2022, our team decided to stick with the features from the 2020 dataset to make the analysis simpler. This choice also allows us to explore both datasets when refining the model in the future. So, out of the initial 40 features, we used 18 for the model analysis and development. We included two extra features for weight and height since they're needed to fill in missing BMI values. Specifically, we focused on the 2022 dataset with missing values to dig deeper into the data and provide our own insights when filling in those missing values.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the following Python packages.

For Jupyter file:
```bash
pip install numpy
pip install pandas
pip install matplotlib
pip install seaborn
pip install scikit-learn
pip install imblearn
pip install tensorflow
```

For web application:
```bash
pip install numpy
pip install pandas
pip install flask
pip install scikit-learn
pip install gunicorn (for Render)
```

## Program Execution

Below is the script to launch the heart disease application in the local machine. Make sure to install all necessary Python libraries before executing the program.
```bash
python app.py
```

## Members
- Auradee Castro
- Bhumika Rajendra Babu  
- Miraj Sinya
- Olivia Deguit
- Roger Mais
