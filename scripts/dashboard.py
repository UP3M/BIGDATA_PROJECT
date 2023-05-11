import streamlit as st
import altair as alt
import pandas as pd
import re
def first_reg(string1):
    # regular expression
    pattern = r"doc='regularization parameter \(>= 0\).'\):\s(.*?),\sParam\(parent=u"
    matches = re.search(pattern, string1)
    pattern1 = "name='maxIter'.*?(\d+)}"
    matches1 = re.search(pattern1, string1)
    return "regularization param:{}, maxIter:{}".format(matches.group(1), matches1.group(1))
def second_reg(string1):
    # regular expression
    pattern = r"name='maxDepth'.+?\): (\d+)"
    matches = re.search(pattern, string1)
    pattern1 = r"name='numTrees', doc='Number of trees to train \(>= 1\).'\): (\d+)"
    matches1 = re.search(pattern1, string1)
    return "Maximum depth of the tree: {} , Number of trees to train {}".format(matches.group(1), matches1.group(1))

customers = pd.read_csv("data/customers.csv")
pings = pd.read_csv("data/pings.csv")
test = pd.read_csv("data/test.csv")
q1 = pd.read_csv("output/q1.csv")
q2 = pd.read_csv("output/q2.csv")
q3 = pd.read_csv("output/q3.csv")
q4 = pd.read_csv("output/q4.csv")
q5 = pd.read_csv("output/q5.csv")
q6 = pd.read_csv("output/q6.csv/part-00000-1713104f-df86-4b25-a572-0f5471e636e0-c000.csv")
q6.iloc[:, 1] = q6.iloc[:, 1].apply(second_reg)
q7 = pd.read_csv("output/q7.csv/part-00000-924d61ed-d7db-44bc-85c8-06f537bfd2c9-c000.csv")
q7.iloc[:, 1] = q7.iloc[:, 1].apply(first_reg)

st.markdown('---')
st.markdown('<center>Big Data Project 2023</center>', unsafe_allow_html = True)
st.markdown("<h1 style='text-align: center;'>COP (Customer Online_hours Predictions)</h1>", unsafe_allow_html = True)
st.markdown(
    """
    <style>
    body {
        background-image: url('https://cutewallpaper.org/21/professional-powerpoint-background/Cute-But-Professional-Powerpoint-Templates-Exotic-Collection-.jpg');
        background-size: cover;
        background-repeat: no-repeat;
        #background-position: center;
    }
    </style>
    """,
    unsafe_allow_html=True
)


import streamlit as st

st.markdown(
    """
    <style>
    .centered-image {
        display: flex;
        justify-content: center;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class="centered-image">
        <img src="https://elnathsoft.pl/wp-content/uploads/2021/11/timeChange-1600x1067.png" alt="Customers and their way of spending time" width="400">
    </div>
    """,
    unsafe_allow_html=True
)

#st.markdown("<p style='text-align: center; color: grey;'>Employees and Departments</p>", unsafe_allow_html=True)

st.markdown('---')
st.header('Descriptive Data Analysis')
st.subheader('Data Characteristics')
customers_dda = pd.DataFrame(data = [["Customers", customers.shape[0]-1, customers.shape[1]], ["Pings", pings.shape[0], pings.shape[1]], ["Test", test.shape[0], test.shape[1]]], columns = ["Tables", "Instances", "Features"])
st.write(customers_dda)
st.markdown('`customers` table')
st.write(customers.describe())
st.markdown('`pings` table')
st.write(pings.describe())
st.markdown('`test` table')
st.write(test.describe())

st.subheader('Some samples from the data')
st.markdown('`customers` table')
st.write(customers.head(5))
st.markdown("`pings` table")
st.write(pings.head(5))
st.markdown("`test` table")
st.write(test.head(5))

st.markdown('---')
q1.iloc[:, 1] = q1.iloc[:, 1].astype(int)
q1_chart = alt.Chart(q1).mark_bar().encode(
    y=alt.Y(q1.columns[1], axis=alt.Axis(title='Number of Customers')),
    x=alt.X('gender:N', axis=alt.Axis(title='Gender')),
    color=alt.Color('gender:N', legend=None)
) 
st.header("Exploratory Data Analysis")
st.subheader('Q1')
st.text('The distribution of customers gender')
st.altair_chart(q1_chart, use_container_width=True)

st.markdown('---')
q2_chart = alt.Chart(q2).mark_bar().encode(
    y=alt.Y(q2.columns[1], axis=alt.Axis(title='Number of Customers')),
    x=alt.X(q2.columns[0], axis=alt.Axis(title='Age of Customer')),
)
st.header("Exploratory Data Analysis")
st.subheader('Q2')
st.text('The distribution of customers age')
st.altair_chart(q2_chart, use_container_width=True)

st.markdown('---')
q3_chart = alt.Chart(q3).mark_bar().encode(
    y=alt.Y(q3.columns[1], axis=alt.Axis(title='Number of Customers')),
    x=alt.X('number_of_customer_kids:N', axis=alt.Axis(title='Number of Kids of Customer')),
)
st.header("Exploratory Data Analysis")
st.subheader('Q3')
st.text('The distribution of total number of customers kids')
st.altair_chart(q3_chart, use_container_width=True)

st.markdown('---')
q4_chart = alt.Chart(q4).mark_bar().encode(
    y=alt.Y(q4.columns[1], axis=alt.Axis(title='Number of Customers')),
    x=alt.X(q4.columns[0], axis=alt.Axis(title='Online Hours of Customer')),
)
st.header("Exploratory Data Analysis")
st.subheader('Q4')
st.text('The distribution of online hours of customers')
st.altair_chart(q4_chart, use_container_width=True)

st.markdown('---')
q5_chart = alt.Chart(q5).mark_bar().encode(
    y=alt.Y(q5.columns[1], axis=alt.Axis(title='Total Online Hours')),
    x=alt.X(q5.columns[0], axis=alt.Axis(title='Date')),
)
st.header("Exploratory Data Analysis")
st.subheader('Q5')
st.text('The distribution of total online hours of customers everyday')
st.altair_chart(q5_chart, use_container_width=True)


st.markdown('---')
st.header('Predictive Data Analytics')
st.subheader('ML Model')
st.markdown('1. Linear Regression Model')
st.markdown('Settings of the Linear Regression model and its result')
st.markdown("`q7` table")
st.table(q7)
st.markdown('`q7` table description')
st.write(q7.describe())

st.markdown('2. RandomForest Regressor')
st.markdown('Settings of the RandomForest model and its result')
st.markdown("`q6` table")
st.table(q6)
st.markdown('`q6` table description')
st.write(q6.describe())

##Optional (not implemented)
#st.subheader('Training vs. Error chart')
#st.write("matplotlib or altair chart")
#st.subheader('Prediction')
#st.text('Given a sample, predict its value and display results in a table.')
#st.text('Here you can use input elements but it is not mandatory')
