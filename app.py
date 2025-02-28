#%%
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#%%
# Title of the app
st.title("Simple Streamlit Dashboard")

# Sidebar input
st.sidebar.header("User Input")
n = st.sidebar.slider("Number of data points", 10, 100, 50)

# Generate random data
data = np.random.randn(n)
df = pd.DataFrame(data, columns=["Values"])

# Display data
st.write("## Generated Data")
st.dataframe(df)

# Show basic statistics
st.write("## Statistics")
st.write(df.describe())

# Plot histogram
st.write("## Histogram")
fig, ax = plt.subplots()
ax.hist(df["Values"], bins=15, color='skyblue', edgecolor='black')
st.pyplot(fig)
