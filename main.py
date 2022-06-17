import pandas as pd
import re


# create data cleaner function ---------------------------------------------------------------
def clean_data(data):
    # clean data1_not['Gender']
    data['Gender'][data['Gender'] == '0'] = 'Male'
    data['Gender'][data['Gender'] == 'M'] = 'Male'
    data['Gender'][data['Gender'] == '1'] = 'Female'
    data['Gender'][data['Gender'] == 'F'] = 'Female'

    # clean data1_not['FirstName']
    x = pd.Series([" ".join(re.findall("[a-zA-Z]+", str(i).title())) for i in data['FirstName'].tolist()])
    data['FirstName'] = x

    # clean data1_not['LastName']
    y = pd.Series([" ".join(re.findall("[a-zA-Z]+", str(i).title())) for i in data['LastName'].tolist()])
    data['LastName'] = y

    # clean data1_not['UserName']
    z = pd.Series([" ".join(re.findall("[a-zA-Z]+", str(i).lower())) for i in data['UserName'].tolist()])
    data['UserName'] = z

    # clean data1_not['Email']
    data['Email'] = pd.Series(["None" if str(i) == 'nan' else i.lower() for i in data['Email']])
    data['Email'] = pd.Series([i + ".in" if i.endswith('@woodinc') else str(i) for i in data['Email']])

    # clean data1_not['City']
    data['City'] = pd.Series([" ".join(re.findall("[a-zA-Z]+", i.title())) for i in data['City'].tolist()])

    # clean data1_not['Age']
    data['Age'] = pd.Series([int(" ".join(re.findall("\d+", str(i)))) for i in data['Age'].tolist()])

    # clean data1_not['Country']
    data['Country'] = pd.Series(["None" if str(i) == 'nan' else "USA" for i in data['Country']])

    return data


# cleaned table1 ---------------------------------------------------------------
table1 = pd.read_csv('only_wood_customer_us_1.csv')
data1 = clean_data(table1)
# data1 ---------------------------------------------------------------


# cleaned table2 ---------------------------------------------------------------
table2 = pd.read_csv('only_wood_customer_us_2.csv', delimiter=';', header=None)
table2.rename(columns={0: 'Age', 1: 'City', 2: 'Gender', 4: 'Email'}, inplace=True)
table2['FirstName'] = pd.Series([i.split()[0] for i in table2[3]])
table2['LastName'] = pd.Series([i.split()[1] for i in table2[3]])
table2['UserName'] = pd.Series([i.lower() for i in table2['FirstName']])
table2['Country'] = pd.Series(["USA" for i in range(10000)])
del table2[3]
table2 = table2[['Gender', 'FirstName', 'LastName', 'UserName', 'Email', 'Age', 'City', 'Country']]
data2 = clean_data(table2)
# data2 ---------------------------------------------------------------


# cleaned table3 ---------------------------------------------------------------
table3 = pd.read_csv('only_wood_customer_us_3.csv', header=1, delimiter='\t')
table3['string_Male'][10000] = (table3.columns[0])
table3['string_kendall DACH'][10000] = (table3.columns[1])
table3['string_DACH.KENDALL@HOTMAIL.COM'][10000] = (table3.columns[2])
table3['string_Dallas'][10000] = (table3.columns[3])
table3['string_United_State_Of_America'][10000] = (table3.columns[4])

table3.columns = ['Gender', '2', 'Email', 'Age', 'City', 'Country']

table3['FirstName'] = [i.split()[0] for i in table3['2']]
table3['LastName'] = [i.split()[1] for i in table3['2']]
table3['UserName'] = pd.Series([i.lower() for i in table3['FirstName']])

del table3['2']
table3 = table3[['Gender', 'FirstName', 'LastName', 'UserName', 'Email', 'Age', 'City', 'Country']]


def replace_x_words(data):
    return pd.Series(
        [str(i).replace((" ".join(re.findall("string_|character_|integer_|boolean_", str(i)))), '') for i in data])


table3['Gender'], table3['FirstName'], table3['UserName'], table3['Email'], table3['Age'], table3['City'], table3['Country'] = replace_x_words(table3['Gender']), replace_x_words(table3['FirstName']), replace_x_words(table3['UserName']), replace_x_words(table3['Email']), replace_x_words(table3['Age']), replace_x_words(table3['City']), replace_x_words(table3['Country'])
data3 = clean_data(table3)
# data3 ---------------------------------------------------------------


# full data ---------------------------------------------------------------
full_data = data1.append(data2).append(data3)
# full_data ---------------------------------------------------------------


# data to sql ---------------------------------------------------------------
from sqlalchemy import create_engine

engine = create_engine('sqlite://', echo=False)

full_data.to_sql('plastic_free_boutique', con=engine)

x = engine.execute("SELECT * FROM plastic_free_boutique").fetchall()

for i, j in enumerate(x):
    print(j)

    if i == 100:
        break

# end code ---------------------------------------------------------------
