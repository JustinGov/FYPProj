import streamlit as st
import numpy as np
import pandas as pd
import pycountry
import glob
import plotly.express as px
from PIL import Image, ImageDraw
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype
)

# Define custom color scale with a distinct midpoint color
color_scale = [
    [0.0, 'rgb(255, 0, 0)'],
    [0.4, 'rgb(255, 255, 0)'],
    [0.5, 'rgb(255, 255, 255)'],
    [0.6, 'rgb(0, 255, 255)'],
    [1.0, 'rgb(0, 0, 255)']
]

reversed_color_scale = list(reversed(color_scale))

st.set_page_config(page_title='Weather data Visualization')
st.header('Weather data visualization')

# read csv file
csv_files = glob.glob('/usr/local/hadoop_namenode/*.csv')

dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file, usecols=lambda x: x != 'Unnamed: 0')
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)

# Function to Filter columns in CSV


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    modify = st.checkbox("Add filters to dataframe")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            try:
                left, right = st.columns((1, 20))
                # Treat columns with < 10 unique values as categorical
                if is_categorical_dtype(df[column]) or df[column].nunique() < 15:
                    user_cat_input = right.multiselect(
                        f"Values for {column} to exclude",
                        df[column].unique(),
                    )
                    df = df[~df[column].isin(user_cat_input)]
                elif is_numeric_dtype(df[column]):
                    _min = float(df[column].min())
                    _max = float(df[column].max())
                    step = (_max - _min) / 100
                    user_num_input = right.slider(
                        f"Values for {column}",
                        min_value=_min,
                        max_value=_max,
                        value=(_min, _max),
                        step=step,
                    )
                    df = df[df[column].between(*user_num_input)]
                elif is_datetime64_any_dtype(df[column]):
                    user_date_input = right.date_input(
                        f"Values for {column}",
                        value=(
                            df[column].min(),
                            df[column].max(),
                        ),
                    )
                    if len(user_date_input) == 2:
                        user_date_input = tuple(
                            map(pd.to_datetime, user_date_input))
                        start_date, end_date = user_date_input
                        df = df.loc[df[column].between(start_date, end_date)]
                else:
                    user_text_input = right.text_input(
                        f"Substring or regex in {column}",
                    )
                    if user_text_input:
                        df = df[df[column].astype(
                            str).str.contains(user_text_input)]
            except:
                right.write(f'Column {column} cannot be filtered')
        drop_cols = st.checkbox("Drop columns")
        if drop_cols:
            to_drop_columns = st.multiselect("Drop columns", df.columns)
            df = df.drop(to_drop_columns, axis=1)

        filtered_df = df


    return filtered_df if 'filtered_df' in locals() else df

# Start of functions for Visualization Plots


new_col = []
for col in df.columns:
    new_col.append(col)
all_cols = tuple(new_col)
numeric_cols = tuple(df.select_dtypes(include=['float64', 'int64']))
datetime_cols = tuple(df.select_dtypes(include=['datetime64']))
string_cols = tuple(df.select_dtypes(include=['object']))
string_cols_limited = tuple(df.select_dtypes(
    include=['object']).loc[:, df.select_dtypes(include=['object']).nunique() < 20])
categorical_cols = tuple(df.select_dtypes(include=['object', 'category']))
heatmap_cols = []
for col in numeric_cols:
    if len(df[col].unique()) > 5:
        heatmap_cols.append(col)
heatmap_cols = tuple(heatmap_cols)


def piechart_cols(df):
    new_cols = []
    for col in df.columns:
        if is_categorical_dtype(df[col]) or df[col].nunique() < 10:
            new_cols.append(col)
    return tuple(new_cols)

# End of functions for visualization plots

# Function to Let user choose which plot to display


def visualization(plot_selected, df):
    # BOXPLOT
    if plot_selected == 'BoxPlot':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful to visualize the distribution of a particular weather parameter, such as temperature, over a given time period or location.')
            st.write('- This can help identify outliers, skewness, and other patterns in the data that may not be immediately apparent from summary statistics.')
            st.write('- Can show the median, quartiles, and outliers of data collected over a period')
            st.write('- Hover over different parts of the boxplot to see specific values')
        try:
            option = st.selectbox(
                'Which column boxplot would you like to observe', numeric_cols)
            add_cat = st.checkbox("Add categories")
            if add_cat:
                option2 = st.selectbox(
                    'Which column boxplot would you like to use as the categories', string_cols_limited)
                fig = px.box(df, y=option, color=option2,
                            color_discrete_sequence=["#636EFA"])
                fig.update_layout(
                    title="Boxplot",
                    yaxis_title=option,
                    font=dict(
                        family="Courier New, monospace",
                        size=18,
                        color="#7f7f7f"
                    ),
                    xaxis=dict(
                        tickangle=-45,
                        title="",
                        tickfont=dict(size=14)
                    ),
                    yaxis=dict(
                        tickfont=dict(size=14)
                    )
                )
                st.plotly_chart(fig)
            else:
                fig = px.box(df, y=option, color_discrete_sequence=["#636EFA"])
                fig.update_layout(
                    title="Boxplot",
                    yaxis_title=option,
                    font=dict(
                        family="Courier New, monospace",
                        size=18,
                        color="#7f7f7f"
                    ),
                    xaxis=dict(
                        tickangle=-45,
                        title="",
                        tickfont=dict(size=14)
                    ),
                    yaxis=dict(
                        tickfont=dict(size=14)
                    )
                )
                st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # PIECHART
    elif plot_selected == 'PieChart':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful in visualizing the proportion, percentage or distribution of different weather phenomena such as rain, snow, thunderstorms, etc. or other forms of data')
            st.write('- Example, pie chart can be used to show the percentage of rainy days vs. sunny days')
        try:
            option = st.selectbox(
                'Which column piechart would you like to observe', piechart_cols(df))
            new_df = df.groupby([option])[
                option].count().reset_index(name='count')
            st.dataframe(new_df)
            fig = px.pie(new_df, values='count', names=option, title=option)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(title='Distribution of Weather Conditions')
            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # SCATTERPLOT
    elif plot_selected == 'ScatterPlot':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Scatter plot displays the relationship between two variables')
            st.write('- Example, the x-axis can represent temperature, while the y-axis represents precipitation. The scatter plot will then plot each data point in the dataset as a point on the plot')
            st.write('- Help to identify any patterns or trends in the data, example, if there is a positive correlation between 2 variables, then the data points on the scatter plot will form a roughly upward-sloping line')
        try:
            x_col = st.selectbox('Select x-axis column:', numeric_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            add_cat = st.checkbox("Add category to seperate points by")
            if add_cat:
                option1 = st.selectbox(
                    'Which column would you like to use as the category seperator', string_cols_limited)
                fig = px.scatter(df, x=x_col, y=y_col,
                                hover_name=option1, color=option1)
            else:
                fig = px.scatter(df, x=x_col, y=y_col)
            fig.update_traces(opacity=0.7)
            title = 'Scatter Plot of ' + x_col + ' and ' + y_col
            fig.update_layout(
                title=title,
                xaxis_title=x_col,
                yaxis_title=y_col
            )
            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # LINEPLOT
    elif plot_selected == 'LinePlot':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful for showing trends over time.')

            st.write('Steps to set timeseries if available:')
            st.write('1) Set x-axis column as the datetime column')
            st.write('2) Set y-axis column as column to be observed')
            st.write('3) Use dataframe filter above as required')
        try:
            x_col = st.selectbox('Select x-axis column (preferably a date-time column):', all_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            add_cat = st.checkbox("Add location to seperate lines by")
            if add_cat:
                option1 = st.selectbox(
                    'Which column would you like to use as the location seperator', string_cols_limited)
                fig = px.line(df, x=x_col, y=y_col, color=option1)
            else:
                fig = px.line(df, x=x_col, y=y_col)
            fig.update_traces(opacity=0.7)
            fig.update_layout(
                title='Line Plot',
                xaxis_title=x_col,
                yaxis_title=y_col
            )
            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # BARCHART
    elif plot_selected == 'BarChart':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful for visualizing categorical data and comparing the relative size or frequency of each category.')
            st.write('- Best used with categorical or nominal data')
        try:
            x_col = st.selectbox('Select x-axis column:', all_cols)
            y_col = f"Count of {x_col}"
            fig = px.histogram(df, x=x_col)
            fig.update_layout(bargap=0.1)
            fig.update_traces(marker_color='#008080', opacity=0.7, marker_line_color='rgb(8,48,107)',
                            marker_line_width=1.5)
            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # VIOLINCHART
    elif plot_selected == 'ViolinChart':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful plot for visualizing the distribution of a continuous variable across different categories. ')
            st.write('- The shape of the violin plot shows the distribution of the data, with the width of the plot showing the density of the data at that point. The height of the plot shows the range of the data.')
            st.write('- For weather data, a violin plot could be useful in visualizing the distribution of a continuous variable, such as temperature or precipitation, across different categories, such as months or seasons.')
        try:
            x_col = st.selectbox('Select x-axis column:', categorical_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            fig = px.violin(df, x=x_col, y=y_col)
            fig.update_traces(opacity=0.7)
            fig.update_layout(
                title="Violin Chart",
                font=dict(size=14)
            )
            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # HEATMAP
    elif plot_selected == 'HeatMap':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful in visualizing the relationship between two variables in a dataset. ')
            st.write('- Uses a color-coded matrix to represent the values of the variables, with each cell in the matrix representing the intersection between a specific value of one variable and a specific value of the other variable.')
        correl_check = st.checkbox("HeatMap of correlation")
        if correl_check:
            try:
                fig = px.imshow(df.corr(),
                                color_continuous_scale='viridis',
                                labels=dict(x="Columns", y="Columns",
                                            color="Correlation"),
                                x=df.corr().columns,
                                y=df.corr().columns,
                                title="Correlation Heatmap")
                fig.update_layout(font=dict(family="Arial", size=12))
                st.plotly_chart(fig)
            except:
                st.write('Dataset error')
        else:
            try:
                x_col = st.selectbox('Select x-axis column:', heatmap_cols)
                y_col = st.selectbox('Select y-axis column:', heatmap_cols)
                matrix = pd.pivot_table(
                    df, values=y_col, index=x_col, aggfunc=np.mean)
                fig = px.imshow(matrix, color_continuous_scale='viridis',
                                title="Heatmap")
                fig.update_layout(font=dict(family="Arial", size=12))
                st.plotly_chart(fig)
            except:
                st.write('Error: X and Y axis must be different')
    # Scatter3D
    elif plot_selected == 'Scatter3d':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Three-dimensional version of a scatter plot, where data points are represented by markers in three-dimensional space.')
            st.write('- This plot type is useful for visualizing the relationship between three variables.')
            st.write('- Help in identifying outliers or clusters of data points that may be of interest')
        try:
            x_col = st.selectbox('Select x-axis column:', numeric_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            z_col = st.selectbox('Select z-axis column:', numeric_cols)
            add_cat = st.checkbox("Add category feature to plot")
            if add_cat:
                option1 = st.selectbox(
                    'Which column would you like to use as the category seperator', string_cols_limited)
                fig = px.scatter_3d(df, x=x_col, y=y_col,
                                    z=z_col, color=option1)
            else:
                fig = px.scatter_3d(df, x=x_col, y=y_col, z=z_col)
            fig.update_traces(opacity=0.7, marker=dict(size=2))
            fig.update_layout(legend=dict(title='Scatter3D Plot',
                            orientation='h'), scene_camera=dict(eye=dict(x=2, y=2, z=2)))
            st.plotly_chart(fig, use_container_width=True,
                            height=2000, width=2000)
        except:
            st.write('Invalid column set')
    # BUBBLECHART
    elif plot_selected == 'BubbleChart':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- A type of scatter plot where the size of the marker is used to represent a third variable in addition to the x- and y-variables.')
            st.write('- Usefulness of a bubble chart in the context of weather forecasting or analysis could be to represent three variables such as temperature, humidity, and wind speed in a single chart')
        try:
            x_col = st.selectbox('Select x-axis column:', numeric_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            size_col = st.selectbox('Select size column:', all_cols)
            fig = px.scatter(df, x=x_col, y=y_col,
                             size=size_col, size_max=30, opacity=0.7, color_continuous_scale=px.colors.sequential.Viridis, width=800, height=500)

            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')


    # CHLORPLETH MAP
    elif plot_selected == 'Choropleth':
        usage_info = st.checkbox("Plot usage info")
        if usage_info:
            st.write(
                '- Useful for visualizing spatial data on a map.')
            st.write('- Choropleth maps can be used to display weather data such as temperature, rainfall, humidity, or wind speed, across regions, states, or countries')
            st.write('- Map can also be animated over time to show changes in temperature patterns, allowing for better understanding of weather patterns over time, by adding datetime feature.')
        geo_col = st.selectbox(
            'Select geo-location column:', string_cols)
        value_col = st.selectbox('Select value column:', numeric_cols)
        add_cat = st.checkbox("Add datetime feature to animate")
        
        def convert_country_to_iso3(country_name):
            """
            Converts country name to ISO-3 code
            """
            try:
                return pycountry.countries.get(name=country_name).alpha_3
            except:
                return None
            
        if add_cat:
            option1 = st.selectbox(
                'Select datetime column', all_cols)
            df['iso3'] = df[geo_col].apply(convert_country_to_iso3)
            fig = px.choropleth(df, locations='iso3', locationmode='ISO-3', color=value_col, animation_frame=option1,
                                hover_name=geo_col, projection='natural earth', color_continuous_scale=reversed_color_scale, title="Chloropleth Map")
        else:
            df['iso3'] = df[geo_col].apply(convert_country_to_iso3)
            fig = px.choropleth(df, locations='iso3', locationmode='ISO-3', color=value_col,
                                hover_name=geo_col, projection='natural earth', color_continuous_scale=reversed_color_scale, title="Chloropleth Map")
        fig.update_layout(coloraxis=dict(colorscale="Viridis"))
        fig.update_layout(
            title=dict(x=0.5),
            font=dict(size=12),
            margin=dict(l=50, r=50, t=50, b=50),
            plot_bgcolor="white"
        )
        st.plotly_chart(fig)

st.markdown("<hr style='border-top: 3px dotted'>", unsafe_allow_html=True)
st.subheader('Data Filtering')
filtered_df = filter_dataframe(df)
st.write('**Weather dataframe**')
st.dataframe(filtered_df)
st.markdown("<hr style='border-top: 3px dotted'>", unsafe_allow_html=True)
st.subheader('Data Summary')
summ = st.checkbox("Expand summary of filtered dataset")
if summ:
    summary_obj = filtered_df.describe(include='object')
    summary_num = filtered_df.describe(include='number')
    st.write('**Summary of Object columns**')
    st.write(summary_obj)
    st.write('**Summary of Numerical columns**')
    st.write(summary_num)
    val_counts_checkbox = st.checkbox('Observe unique value counts of a selected column')
    if val_counts_checkbox:
        st.write('**Value Counts of Column**')
        selected = st.selectbox(
            'Select column to observe unique value counts',
            filtered_df.columns
        )
        val_counts = filtered_df[selected].value_counts()
        st.write(val_counts)
st.markdown("<hr style='border-top: 3px dotted'>", unsafe_allow_html=True)
st.subheader('Data Visualization')
# let user select column to analyze in boxplot
plots = ['BoxPlot', 'PieChart', 'ScatterPlot',
         'LinePlot', 'BarChart', 'ViolinChart', 'HeatMap', 'Scatter3d', 'BubbleChart', 'Choropleth']

plot_selected = st.selectbox(
    'Which plot would you like to visualize?', plots)

visualization(plot_selected, filtered_df)
st.markdown("<hr style='border-top: 3px dotted'>", unsafe_allow_html=True)