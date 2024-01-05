# Heart Disease Prediction

This program has used logistic regression, decision tree classifier and artificial intelligence algorithms with cross-validation to develop and train the models that could estimate the likelihood of heart disease based on important indicators associated with the condition. The model was then evaluated for its accuracy, precision, recall and auc scores. The model with highest scores in evaluation metrics is the one incorporated in the application to predict heart disease.

- Sex (Male or Female)
- Age Category
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


The dataset were obtained from the website https://www.kaggle.com/datasets/kamilpytlak/personal-key-indicators-of-heart-disease. We created synthetic samples as well to balance the data distribution among the predictor class.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the following Python packages.

For Jupyter file:
```bash
pip install numpy
pip install pandas
pip install matplotlib
pip install seaborn
pip install sklearn
pip install imblearn
```

For web application:
```bash
pip install numpy
pip install pandas
pip install flask
```

## Program Execution
Launch the heart disease application
```bash
python app.py
```

## Members
- Auradee Castro
- Bhumika Rajendra Babu  
- Miraj Sinya
- Olivia Deguit
- Roger Mais