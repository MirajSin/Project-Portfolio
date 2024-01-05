
"""
##########################################################################################################################

Data Preprocessing Methods

This Python file contains a collection of data preprocessing methods to clean and prepare data prior to building a model. 
These methods include functions for tasks such as data imputations.

Usage:
- Import this file in your Python script.
- Call the desired preprocessing functions with your text data to apply the respective transformation.

##########################################################################################################################
"""

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector, VectorUDT


def impute_biometrics(main_df, result_df, col_to_imputate, col_impute_val):
    """
    Function to fill in missing values in height and weight features
    """
    try:
        result = result_df.collect()
        main_df = main_df.withColumn(col_to_imputate, 
            # for female biometrics
            when((col(col_to_imputate).isNull()) & (col("Sex") == result[0]["Sex"]) & (col("AgeCategory").isNull()), result[0][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[1]["Sex"]) & (col("AgeCategory") == result[1]["AgeCategory"]), result[1][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[2]["Sex"]) & (col("AgeCategory") == result[2]["AgeCategory"]), result[2][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[3]["Sex"]) & (col("AgeCategory") == result[3]["AgeCategory"]), result[3][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[4]["Sex"]) & (col("AgeCategory") == result[4]["AgeCategory"]), result[4][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[5]["Sex"]) & (col("AgeCategory") == result[5]["AgeCategory"]), result[5][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[6]["Sex"]) & (col("AgeCategory") == result[6]["AgeCategory"]), result[6][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[7]["Sex"]) & (col("AgeCategory") == result[7]["AgeCategory"]), result[7][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[8]["Sex"]) & (col("AgeCategory") == result[8]["AgeCategory"]), result[8][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[9]["Sex"]) & (col("AgeCategory") == result[9]["AgeCategory"]), result[9][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[10]["Sex"]) & (col("AgeCategory") == result[10]["AgeCategory"]), result[10][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[11]["Sex"]) & (col("AgeCategory") == result[11]["AgeCategory"]), result[11][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[12]["Sex"]) & (col("AgeCategory") == result[12]["AgeCategory"]), result[12][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[13]["Sex"]) & (col("AgeCategory") == result[13]["AgeCategory"]), result[13][col_impute_val])
            # for male biometrics
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[14]["Sex"]) & (col("AgeCategory").isNull()), result[14][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[15]["Sex"]) & (col("AgeCategory") == result[15]["AgeCategory"]), result[15][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[16]["Sex"]) & (col("AgeCategory") == result[16]["AgeCategory"]), result[16][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[17]["Sex"]) & (col("AgeCategory") == result[17]["AgeCategory"]), result[17][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[18]["Sex"]) & (col("AgeCategory") == result[18]["AgeCategory"]), result[18][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[19]["Sex"]) & (col("AgeCategory") == result[19]["AgeCategory"]), result[19][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[20]["Sex"]) & (col("AgeCategory") == result[20]["AgeCategory"]), result[20][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[21]["Sex"]) & (col("AgeCategory") == result[21]["AgeCategory"]), result[21][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[22]["Sex"]) & (col("AgeCategory") == result[22]["AgeCategory"]), result[22][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[23]["Sex"]) & (col("AgeCategory") == result[23]["AgeCategory"]), result[23][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[24]["Sex"]) & (col("AgeCategory") == result[24]["AgeCategory"]), result[24][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[25]["Sex"]) & (col("AgeCategory") == result[25]["AgeCategory"]), result[25][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[26]["Sex"]) & (col("AgeCategory") == result[26]["AgeCategory"]), result[26][col_impute_val])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[27]["Sex"]) & (col("AgeCategory") == result[27]["AgeCategory"]), result[27][col_impute_val])
            .otherwise(main_df[col_to_imputate])
        ))))))))))))))))))))))))))))
    except Exception as err:
        print(f"ERROR: {err}")

    return main_df


def impute_bmi(df):
    """
    Function to fill in missing values in BMI column
    """
    try:
        df = df.withColumn("BMI", 
            when((col("BMI").isNull()) & (col("WeightInKilograms").isNotNull()) & (col("HeightInMeters").isNotNull()),
                (col("WeightInKilograms") / (col("HeightInMeters")**2)))
            .otherwise(col("BMI")))
    except Exception as err:
        print(f"ERROR: {err}")

    return df


def impute_based_on_sws(main_df, result_df, col_to_imputate):
    """
    Function to fill in missing values using window patitioning for sex and weight status
    """
    try:
        result = result_df.collect()
        main_df = main_df.withColumn(col_to_imputate, 
            # female age category 
            when((col(col_to_imputate).isNull()) & (col("Sex") == result[0]["Sex"]) & (col("WeightStatus") == result[0]["WeightStatus"]), result[0][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[1]["Sex"]) & (col("WeightStatus") == result[1]["WeightStatus"]), result[1][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[2]["Sex"]) & (col("WeightStatus") == result[2]["WeightStatus"]), result[2][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[3]["Sex"]) & (col("WeightStatus") == result[3]["WeightStatus"]), result[3][col_to_imputate])
            # male age category
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[4]["Sex"]) & (col("WeightStatus") == result[4]["WeightStatus"]), result[4][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[5]["Sex"]) & (col("WeightStatus") == result[5]["WeightStatus"]), result[5][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[6]["Sex"]) & (col("WeightStatus") == result[6]["WeightStatus"]), result[6][col_to_imputate])
            .otherwise(when((col(col_to_imputate).isNull()) & (col("Sex") == result[7]["Sex"]) & (col("WeightStatus") == result[7]["WeightStatus"]), result[7][col_to_imputate])
            .otherwise(main_df[col_to_imputate])
        ))))))))
    except Exception as err:
        print(f"ERROR: {err}")

    return main_df


def impute_smoking_status(main_df):
    """
    Function to fill in missing values in smoker status based on the grouping with sex and age category
    """
    try:
        main_df = main_df.withColumn("SmokerStatus", 
            when(
                (col("Sex") == "Female") & (col("SmokerStatus").isNull()), 
                "Never smoked")
            .otherwise(when(
                (col("Sex") == "Male") & (col("SmokerStatus").isNull()) & (col("AgeCategory").isin("Age 75 to 79", "Age 80 or older")), 
                "Former smoker")
            .otherwise(when(
                (col("Sex") == "Male") & (col("SmokerStatus").isNull()) & ~(col("AgeCategory").isin("Age 75 to 79", "Age 80 or older")), 
                "Never smoker")
            .otherwise(main_df["SmokerStatus"])
        )))
    except Exception as err:
        print(f"ERROR: {err}")

    return main_df


def impute_alcohol_drinker(main_df):
    """
    Function to fill in missing values in alcohol drinker column based on the grouping with sex and age category
    """
    try:
        main_df = main_df.withColumn("AlcoholDrinkers", 
            when((col("Sex") == "Female") & (col("AlcoholDrinkers").isNull()) & (col("AgeCategory").isin("Age 65 to 69", "Age 70 to 74", "Age 75 to 79", "Age 80 or older")), "No")
            .otherwise(when((col("Sex") == "Female") & (col("AlcoholDrinkers").isNull()) & ~(col("AgeCategory").isin("Age 65 to 69", "Age 70 to 74", "Age 75 to 79", "Age 80 or older")), "Yes")
            .otherwise(when((col("Sex") == "Male") & (col("AlcoholDrinkers").isNull()) & (col("AgeCategory").isin("Age 80 or older")), "No")
            .otherwise(when((col("Sex") == "Male") & (col("AlcoholDrinkers").isNull()) & ~(col("AgeCategory").isin("Age 80 or older")), "Yes")
            .otherwise(main_df["AlcoholDrinkers"])
        ))))
    except Exception as err:
        print(f"ERROR: {err}")

    return main_df


def impute_sleep_hours(main_df):
    """
    Function to fill in missing values in sleep hours column based on the grouping with sex and age category
    """
    try:
        main_df = main_df.withColumn("SleepHours", 
            when((col("SleepHours").isNull()) & (col("AgeCategory").isin("Age 65 to 69", "Age 70 to 74", "Age 75 to 79", "Age 80 or older")), 8)
            .otherwise(when((col("SleepHours").isNull()) & ~(col("AgeCategory").isin("Age 65 to 69", "Age 70 to 74", "Age 75 to 79", "Age 80 or older")), 7)
            .otherwise(main_df["SleepHours"])
        ))
    except Exception as err:
        print(f"ERROR: {err}")

    return main_df


def get_statistical_analysis(df, col1_, col2_, col3_):
    """
    Function to display each group having the maximum record count
    """
    try:
        window_spec = Window.partitionBy(col1_, col2_).orderBy(desc("count"))
        result = df.groupBy(col1_, col2_, col3_).count()\
            .withColumn("max_count", max("count").over(window_spec))\
            .filter(col("count") == col("max_count")).drop("max_count")
    except Exception as err:
        print(f"ERROR: {err}")

    return result


def convert_binary_one_hot_encoding(df, columns):
    """
    Function to convert categorical (binary) into numerical values (1 or 0)
    """
    for col_ in columns:
        app_name = df.select(col_).distinct().orderBy(col_, ascending=False).first()[col_]
        app_name = app_name.title().replace(" ", "")
        indexer = StringIndexer(inputCol=col_, outputCol=(col_+'_'+app_name), stringOrderType="alphabetAsc")
        df = indexer.fit(df).transform(df)
    return df


def convert_one_hot_encoding(df, columns):
    """
    Function to convert categorical with more than 2 values into numerical values 
    """
    for col_ in columns:
        indexer = StringIndexer(inputCol=col_, outputCol=(col_+'_index'), stringOrderType="alphabetAsc")

        # Convert categorical features to one-hot encoding
        onehot_encoder = OneHotEncoder(inputCols=[(col_+'_index')], outputCols=[(col_+'_vec')])

        # Create pipeline and pass it to stages
        pipeline = Pipeline(stages=[indexer, onehot_encoder])

        df = pipeline.fit(df).transform(df)
        df = df.drop(*[(col_+'_index')])
    
    return df


def apply_standard_scaling(df, columns):

    # Create vector assembler
    vector_assembler = VectorAssembler(inputCols=columns, outputCol='vec_features')

    # Standardize the data
    scaler = StandardScaler(inputCol='vec_features', outputCol='scaled_features', withMean=True, withStd=True)

    # Create pipeline and pass it to stages
    pipeline = Pipeline(stages=[vector_assembler, scaler])
    
    df = pipeline.fit(df).transform(df)

    # Define a UDF to convert the vector column to DenseVector
    to_dense_vector_udf = udf(lambda v: DenseVector(v.toArray()), VectorUDT())

    # Apply the UDF to the "features" column
    df = df.withColumn("vec_features", to_dense_vector_udf("vec_features"))

    return df